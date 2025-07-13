# metrics_pipeline/pipeline.py
"""Main pipeline class for processing metrics."""

import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp
from google.cloud import bigquery

from .exceptions import MetricsPipelineError, RollbackError
from .data_processing import (
    parse_tables_from_sql,
    replace_run_dates_in_sql,
    normalize_numeric_value,
    safe_decimal_conversion,
    align_schema_with_bq
)
from .bq_operations import get_partition_dt
from .validation import validate_json, check_dependencies_exist
from .gcs_operations import read_json_from_gcs

logger = logging.getLogger(__name__)

class MetricsPipeline:
    """Main pipeline class for processing metrics."""

    def __init__(self, spark: SparkSession, bq_client: bigquery.Client):
        """Initialize the pipeline.

        Args:
            spark: SparkSession instance
            bq_client: BigQuery client
        """
        self.spark = spark
        self.bq_client = bq_client
        self.execution_id = str(uuid.uuid4())
        self.processed_metrics = []  # Track processed metrics for rollback

    def get_replacement_dates(
        self,
        sql: str,
        run_date: str,
        partition_mode: str,
        partition_info_table: str
    ) -> List[str]:
        """Get replacement dates for all tables based on partition modes.

        Args:
            sql: SQL query string
            run_date: CLI provided run date
            partition_mode: Pipe-separated partition modes
            partition_info_table: Metadata table name

        Returns:
            List of replacement dates for each table

        Raises:
            MetricsPipelineError: If date determination fails
        """
        # Parse all tables from SQL
        tables = parse_tables_from_sql(sql)

        # Parse partition modes
        modes = [mode.strip() for mode in partition_mode.split('|')]

        # Validate that number of tables matches number of modes
        if len(tables) != len(modes):
            raise MetricsPipelineError(
                f"Number of tables ({len(tables)}) doesn't match number of partition modes ({len(modes)}). "
                f"Tables: {tables}, Modes: {modes}"
            )

        replacement_dates = []

        for i, (project_dataset, table_name) in enumerate(tables):
            mode = modes[i]

            if mode == 'currently':
                replacement_date = run_date
            elif mode == 'partition_info':
                replacement_date = get_partition_dt(
                    self.bq_client, project_dataset, table_name, partition_info_table
                )
                if not replacement_date:
                    raise MetricsPipelineError(
                        f"Could not determine partition_dt for table {project_dataset}.{table_name}"
                    )
            else:
                # This should not happen due to validation, but adding safety check
                raise MetricsPipelineError(
                    f"Invalid partition mode '{mode}' for table {project_dataset}.{table_name}"
                )

            replacement_dates.append(replacement_date)
            logger.info(
                "Table %s.%s will use date: %s (mode: %s)",
                project_dataset, table_name, replacement_date, mode
            )

        return replacement_dates

    def execute_sql(
        self,
        sql: str,
        run_date: str,
        partition_mode: str,
        partition_info_table: str
    ) -> Dict[str, Any]:
        """Execute SQL query with dynamic date replacement for multiple tables.

        Args:
            sql: SQL query string
            run_date: CLI provided run date
            partition_mode: Pipe-separated partition modes
            partition_info_table: Metadata table name

        Returns:
            Dictionary with query results

        Raises:
            MetricsPipelineError: If SQL execution fails
        """
        try:
            # Get replacement dates for all tables
            replacement_dates = self.get_replacement_dates(
                sql, run_date, partition_mode, partition_info_table
            )

            # Replace all {run_date} placeholders
            final_sql = replace_run_dates_in_sql(sql, replacement_dates)

            logger.info("Executing SQL with multiple date replacements: %s", replacement_dates)

            # Execute query
            query_job = self.bq_client.query(final_sql)
            results = query_job.result()

            # Process results
            result_dict = {
                'metric_output': None,
                'numerator_value': None,
                'denominator_value': None,
                'business_data_date': None
            }

            for row in results:
                # Convert row to dictionary
                row_dict = dict(row)

                # Map columns to result dictionary with precision preservation
                for key in result_dict.keys():
                    if key in row_dict:
                        value = row_dict[key]
                        # Normalize numeric values to preserve precision
                        if key in ['metric_output', 'numerator_value', 'denominator_value']:
                            result_dict[key] = normalize_numeric_value(value)
                        else:
                            result_dict[key] = value

                break  # Take first row only

            # Calculate business_data_date (one day before the reference date)
            # Use the first replacement date as reference
            ref_date = datetime.strptime(replacement_dates[0], '%Y-%m-%d')
            business_date = ref_date - timedelta(days=1)
            result_dict['business_data_date'] = business_date.strftime('%Y-%m-%d')

            return result_dict

        except Exception as e:
            logger.error("Failed to execute SQL: %s", str(e))
            raise MetricsPipelineError(f"Failed to execute SQL: {str(e)}") from e

    def rollback_metric(
        self,
        metric_id: str,
        target_table: str,
        partition_dt: str
    ) -> None:
        """Rollback a specific metric from the target table.

        Args:
            metric_id: Metric ID to rollback
            target_table: Target BigQuery table
            partition_dt: Partition date for the metric

        Raises:
            RollbackError: If rollback fails
        """
        try:
            delete_query = f"""
            DELETE FROM `{target_table}`
            WHERE metric_id = '{metric_id}'
            AND partition_dt = '{partition_dt}'
            AND pipeline_execution_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            """

            logger.info("Rolling back metric %s from %s", metric_id, target_table)

            query_job = self.bq_client.query(delete_query)
            query_job.result()

            logger.info("Successfully rolled back metric %s", metric_id)

        except Exception as e:
            logger.error("Failed to rollback metric %s: %s", metric_id, str(e))
            raise RollbackError(f"Failed to rollback metric {metric_id}: {str(e)}") from e

    def rollback_processed_metrics(
        self,
        target_table: str,
        partition_dt: str
    ) -> None:
        """Rollback all processed metrics in case of failure.

        Args:
            target_table: Target BigQuery table
            partition_dt: Partition date for rollback
        """
        logger.info("Starting rollback of processed metrics")

        for metric_id in self.processed_metrics:
            try:
                self.rollback_metric(metric_id, target_table, partition_dt)
            except Exception as e:
                logger.error("Failed to rollback metric %s: %s", metric_id, str(e))

        logger.info("Rollback process completed")

    def process_metrics(
        self,
        json_data: List[Dict[str, Any]],
        run_date: str,
        dependencies: List[str],
        partition_info_table: str
    ) -> DataFrame:
        """Process metrics and create Spark DataFrame.

        Args:
            json_data: List of metric definitions
            run_date: CLI provided run date
            dependencies: List of dependencies to process
            partition_info_table: Metadata table name

        Returns:
            Spark DataFrame with processed metrics

        Raises:
            MetricsPipelineError: If processing fails
        """
        logger.info("Processing metrics for dependencies: %s", dependencies)

        # Check if all dependencies exist
        check_dependencies_exist(json_data, dependencies)
        partition_dt = datetime.now().strftime('%Y-%m-%d')
        logger.info("Using pipeline run date as partition_dt: %s", partition_dt)

        # Filter records by dependency
        filtered_data = [
            record for record in json_data
            if record['dependency'] in dependencies
        ]

        if not filtered_data:
            raise MetricsPipelineError(
                f"No records found for dependencies: {dependencies}"
            )

        logger.info("Found %d records to process", len(filtered_data))

        # Process each record
        processed_records = []

        for record in filtered_data:
            try:
                # Execute SQL and get results
                sql_results = self.execute_sql(
                    record['sql'],
                    run_date,
                    record['partition_mode'],
                    partition_info_table
                )

                # Build final record with precision preservation
                final_record = {
                    'metric_id': record['metric_id'],
                    'metric_name': record['metric_name'],
                    'metric_type': record['metric_type'],
                    'numerator_value': safe_decimal_conversion(sql_results['numerator_value']),
                    'denominator_value': safe_decimal_conversion(sql_results['denominator_value']),
                    'metric_output': safe_decimal_conversion(sql_results['metric_output']),
                    'business_data_date': sql_results['business_data_date'],
                    'partition_dt': partition_dt,
                    'pipeline_execution_ts': datetime.utcnow()
                }

                processed_records.append(final_record)
                logger.info("Successfully processed metric_id: %s", record['metric_id'])

            except Exception as e:
                logger.error("Failed to process metric_id %s: %s", record['metric_id'], str(e))
                raise MetricsPipelineError(
                    f"Failed to process metric_id {record['metric_id']}: {str(e)}"
                )

        # Create Spark DataFrame with explicit schema to avoid type conflicts
        if not processed_records:
            raise MetricsPipelineError("No records were successfully processed")

        # Define explicit schema with high precision for numeric fields
        schema = StructType([
            StructField("metric_id", StringType(), False),
            StructField("metric_name", StringType(), False),
            StructField("metric_type", StringType(), False),
            StructField("numerator_value", DecimalType(38, 9), True),
            StructField("denominator_value", DecimalType(38, 9), True),
            StructField("metric_output", DecimalType(38, 9), True),
            StructField("business_data_date", StringType(), False),
            StructField("partition_dt", StringType(), False),
            StructField("pipeline_execution_ts", TimestampType(), False)
        ])

        # Create DataFrame with explicit schema
        df = self.spark.createDataFrame(processed_records, schema)
        logger.info("Created DataFrame with %d records", df.count())

        return df

    def write_to_bq(
        self,
        df: DataFrame,
        target_table: str
    ) -> None:
        """Write DataFrame to BigQuery table with transaction safety.

        Args:
            df: Spark DataFrame to write
            target_table: Target BigQuery table

        Raises:
            MetricsPipelineError: If writing to BigQuery fails
        """
        try:
            logger.info("Writing DataFrame to BigQuery table: %s", target_table)

            # Collect metric IDs for rollback tracking
            metric_ids = [row['metric_id'] for row in df.select('metric_id').collect()]
            self.processed_metrics.extend(metric_ids)

            # First align the schema
            aligned_df = align_schema_with_bq(df, self.bq_client, target_table)

            # Write to BigQuery using Spark BigQuery connector
            aligned_df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()

            logger.info("Successfully wrote %d records to %s", aligned_df.count(), target_table)

        except Exception as e:
            logger.error("Failed to write to BigQuery: %s", str(e))
            raise MetricsPipelineError(f"Failed to write to BigQuery: {str(e)}") from e
