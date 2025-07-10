import argparse
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
import sys
from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, 
    when, isnan, isnull, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    TimestampType, DecimalType, IntegerType, DoubleType
)

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass


class MetricsPipeline:
    """Main pipeline class for processing metrics"""
    
    def __init__(self, spark: SparkSession, bq_client: bigquery.Client):
        self.spark = spark
        self.bq_client = bq_client
        
    def read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """
        Read JSON file from GCS and return as list of dictionaries
        
        Args:
            gcs_path: GCS path to JSON file
            
        Returns:
            List of metric definitions
            
        Raises:
            MetricsPipelineError: If file cannot be read or parsed
        """
        try:
            logger.info(f"Reading JSON from GCS: {gcs_path}")
            
            # Read JSON file using Spark
            df = self.spark.read.option("multiline", "true").json(gcs_path)
            
            if df.count() == 0:
                raise MetricsPipelineError(f"No data found in JSON file: {gcs_path}")
            
            # Convert to list of dictionaries
            json_data = [row.asDict() for row in df.collect()]
            
            logger.info(f"Successfully read {len(json_data)} records from JSON")
            return json_data
            
        except Exception as e:
            logger.error(f"Failed to read JSON from GCS: {str(e)}")
            raise MetricsPipelineError(f"Failed to read JSON from GCS: {str(e)}")
    
    def validate_json(self, json_data: List[Dict]) -> List[Dict]:
        """
        Validate JSON data for required fields
        
        Args:
            json_data: List of metric definitions
            
        Returns:
            List of validated metric definitions
            
        Raises:
            MetricsPipelineError: If validation fails
        """
        required_fields = [
            'metric_id', 'metric_name', 'metric_type', 
            'sql', 'dependency', 'partition_mode'
        ]
        
        logger.info("Validating JSON data")
        
        for i, record in enumerate(json_data):
            for field in required_fields:
                if field not in record:
                    raise MetricsPipelineError(
                        f"Record {i}: Missing required field '{field}'"
                    )
                
                value = record[field]
                if value is None or (isinstance(value, str) and value.strip() == ""):
                    raise MetricsPipelineError(
                        f"Record {i}: Field '{field}' is null or empty"
                    )
            
            # Validate partition_mode values
            if record['partition_mode'] not in ['currently', 'partition_info']:
                raise MetricsPipelineError(
                    f"Record {i}: Invalid partition_mode '{record['partition_mode']}'. "
                    f"Must be 'currently' or 'partition_info'"
                )
        
        logger.info(f"Successfully validated {len(json_data)} records")
        return json_data
    
    def parse_table_from_sql(self, sql: str) -> Optional[Tuple[str, str]]:
        """
        Parse project_dataset and table_name from SQL query
        
        Args:
            sql: SQL query string
            
        Returns:
            Tuple of (project_dataset, table_name) or None if not found
        """
        # Pattern to match BigQuery table references like `project.dataset.table`
        pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
        matches = re.findall(pattern, sql)
        
        if matches:
            project, dataset, table = matches[0]  # Take first match
            return dataset, table
        
        return None, None
    
    def get_partition_dt(self, sql: str, partition_info_table: str) -> Optional[str]:
        """
        Get latest partition_dt from metadata table
        
        Args:
            sql: SQL query to parse table from
            partition_info_table: Metadata table name
            
        Returns:
            Latest partition date as string or None
        """
        project_dataset, table_name = self.parse_table_from_sql(sql)
        
        if not project_dataset or not table_name:
            logger.warning(f"Could not parse table from SQL: {sql}")
            return None
        
        try:
            query = f"""
            SELECT partition_dt 
            FROM `{partition_info_table}` 
            WHERE project_dataset = '{project_dataset}' 
            AND table_name = '{table_name}'
            """
            
            logger.info(f"Querying partition info for {project_dataset}.{table_name}")
            
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            for row in results:
                partition_dt = row.partition_dt
                if isinstance(partition_dt, datetime):
                    return partition_dt.strftime('%Y-%m-%d')
                return str(partition_dt)
            
            logger.warning(f"No partition info found for {project_dataset}.{table_name}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get partition_dt: {str(e)}")
            return None
    
    def normalize_numeric_value(self, value: Union[int, float, Decimal, None]) -> Optional[float]:
        """
        Normalize numeric values to consistent float type
        
        Args:
            value: Numeric value of any type
            
        Returns:
            Float value or None
        """
        if value is None:
            return None
        
        try:
            # Convert to float to ensure consistent type
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert value to float: {value}")
            return None
    
    def execute_sql(self, sql: str, run_date: str, partition_mode: str, 
                   partition_info_table: str) -> Dict:
        """
        Execute SQL query with dynamic date replacement
        
        Args:
            sql: SQL query string
            run_date: CLI provided run date
            partition_mode: 'currently' or 'partition_info'
            partition_info_table: Metadata table name
            
        Returns:
            Dictionary with query results
        """
        try:
            # Determine the date to use for {run_date} replacement
            if partition_mode == 'currently':
                replacement_date = run_date
            else:  # partition_info
                replacement_date = self.get_partition_dt(sql, partition_info_table)
                if not replacement_date:
                    raise MetricsPipelineError(
                        f"Could not determine partition_dt for SQL: {sql}"
                    )
            
            # Replace {run_date} in SQL
            final_sql = sql.replace('{run_date}', f"'{replacement_date}'")
            
            logger.info(f"Executing SQL with date {replacement_date}")
            logger.debug(f"Final SQL: {final_sql}")
            
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
                
                # Map columns to result dictionary with type normalization
                for key in result_dict.keys():
                    if key in row_dict:
                        value = row_dict[key]
                        # Normalize numeric values to consistent float type
                        if key in ['metric_output', 'numerator_value', 'denominator_value']:
                            result_dict[key] = self.normalize_numeric_value(value)
                        else:
                            result_dict[key] = value
                
                break  # Take first row only
            
            # Calculate business_data_date (one day before the reference date)
            ref_date = datetime.strptime(replacement_date, '%Y-%m-%d')
            business_date = ref_date - timedelta(days=1)
            result_dict['business_data_date'] = business_date.strftime('%Y-%m-%d')
            
            return result_dict
            
        except Exception as e:
            logger.error(f"Failed to execute SQL: {str(e)}")
            raise MetricsPipelineError(f"Failed to execute SQL: {str(e)}")
    
    def process_metrics(self, json_data: List[Dict], run_date: str, 
                       dependencies: List[str], partition_info_table: str) -> DataFrame:
        """
        Process metrics and create Spark DataFrame
        
        Args:
            json_data: List of metric definitions
            run_date: CLI provided run date
            dependencies: List of dependencies to process
            partition_info_table: Metadata table name
            
        Returns:
            Spark DataFrame with processed metrics
        """
        logger.info(f"Processing metrics for dependencies: {dependencies}")
        
        # Filter records by dependency
        filtered_data = [
            record for record in json_data 
            if record['dependency'] in dependencies
        ]
        
        if not filtered_data:
            raise MetricsPipelineError(
                f"No records found for dependencies: {dependencies}"
            )
        
        logger.info(f"Found {len(filtered_data)} records to process")
        
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
                
                # Build final record with consistent types
                final_record = {
                    'metric_id': record['metric_id'],
                    'metric_name': record['metric_name'],
                    'metric_type': record['metric_type'],
                    'numerator_value': sql_results['numerator_value'],
                    'denominator_value': sql_results['denominator_value'],
                    'metric_output': sql_results['metric_output'],
                    'business_data_date': sql_results['business_data_date'],
                    'partition_dt': run_date,
                    'pipeline_execution_ts': datetime.utcnow()
                }
                
                processed_records.append(final_record)
                logger.info(f"Successfully processed metric_id: {record['metric_id']}")
                
            except Exception as e:
                logger.error(f"Failed to process metric_id {record['metric_id']}: {str(e)}")
                raise MetricsPipelineError(
                    f"Failed to process metric_id {record['metric_id']}: {str(e)}"
                )
        
        # Create Spark DataFrame with explicit schema to avoid type conflicts
        if not processed_records:
            raise MetricsPipelineError("No records were successfully processed")
        
        # Define explicit schema to prevent type inference issues
        schema = StructType([
            StructField("metric_id", StringType(), False),
            StructField("metric_name", StringType(), False),
            StructField("metric_type", StringType(), False),
            StructField("numerator_value", DoubleType(), True),
            StructField("denominator_value", DoubleType(), True),
            StructField("metric_output", DoubleType(), True),
            StructField("business_data_date", StringType(), False),
            StructField("partition_dt", StringType(), False),
            StructField("pipeline_execution_ts", TimestampType(), False)
        ])
        
        # Create DataFrame with explicit schema
        df = self.spark.createDataFrame(processed_records, schema)
        logger.info(f"Created DataFrame with {df.count()} records")
        
        return df
    
    def get_bq_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """
        Get BigQuery table schema
        
        Args:
            table_name: Full table name (project.dataset.table)
            
        Returns:
            List of schema fields
        """
        try:
            logger.info(f"Getting schema for table: {table_name}")
            table = self.bq_client.get_table(table_name)
            return table.schema
            
        except NotFound:
            raise MetricsPipelineError(f"Table not found: {table_name}")
        except Exception as e:
            logger.error(f"Failed to get table schema: {str(e)}")
            raise MetricsPipelineError(f"Failed to get table schema: {str(e)}")
    
    def align_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame:
        """
        Align Spark DataFrame with BigQuery table schema
        
        Args:
            df: Spark DataFrame
            target_table: BigQuery table name
            
        Returns:
            Schema-aligned DataFrame
        """
        logger.info(f"Aligning DataFrame schema with BigQuery table: {target_table}")
        
        # Get BigQuery schema
        bq_schema = self.get_bq_table_schema(target_table)
        
        # Get current DataFrame columns
        current_columns = df.columns
        
        # Build list of columns in BigQuery schema order
        bq_columns = [field.name for field in bq_schema]
        
        # Drop extra columns not in BigQuery schema
        columns_to_keep = [col for col in current_columns if col in bq_columns]
        columns_to_drop = [col for col in current_columns if col not in bq_columns]
        
        if columns_to_drop:
            logger.info(f"Dropping extra columns: {columns_to_drop}")
            df = df.drop(*columns_to_drop)
        
        # Reorder columns to match BigQuery schema
        df = df.select(*[col(c) for c in bq_columns if c in columns_to_keep])
        
        # Handle type conversions for BigQuery compatibility
        for field in bq_schema:
            if field.name in df.columns:
                if field.field_type == 'DATE':
                    df = df.withColumn(field.name, to_date(col(field.name)))
                elif field.field_type == 'TIMESTAMP':
                    df = df.withColumn(field.name, col(field.name).cast(TimestampType()))
                elif field.field_type == 'NUMERIC':
                    df = df.withColumn(field.name, col(field.name).cast(DecimalType(38, 9)))
                elif field.field_type == 'FLOAT':
                    df = df.withColumn(field.name, col(field.name).cast(DoubleType()))
        
        logger.info(f"Schema alignment complete. Final columns: {df.columns}")
        return df
    
    def write_to_bq(self, df: DataFrame, target_table: str) -> None:
        """
        Write DataFrame to BigQuery table
        
        Args:
            df: Spark DataFrame to write
            target_table: Target BigQuery table
        """
        try:
            logger.info(f"Writing DataFrame to BigQuery table: {target_table}")
            
            # Write to BigQuery using Spark BigQuery connector
            df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} records to {target_table}")
            
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {str(e)}")
            raise MetricsPipelineError(f"Failed to write to BigQuery: {str(e)}")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='PySpark BigQuery Metrics Pipeline'
    )
    
    parser.add_argument(
        '--gcs_path', 
        required=True, 
        help='GCS path to JSON input file'
    )
    parser.add_argument(
        '--run_date', 
        required=True, 
        help='Run date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--dependencies', 
        required=True, 
        help='Comma-separated list of dependencies to process'
    )
    parser.add_argument(
        '--partition_info_table', 
        required=True, 
        help='BigQuery table for partition info (project.dataset.table)'
    )
    parser.add_argument(
        '--target_table', 
        required=True, 
        help='Target BigQuery table (project.dataset.table)'
    )
    
    return parser.parse_args()


def validate_date_format(date_str: str) -> None:
    """Validate date format"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise MetricsPipelineError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")


def main():
    """Main function"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Validate date format
        validate_date_format(args.run_date)
        
        # Parse dependencies
        dependencies = [dep.strip() for dep in args.dependencies.split(',')]
        
        logger.info("Starting Metrics Pipeline")
        logger.info(f"GCS Path: {args.gcs_path}")
        logger.info(f"Run Date: {args.run_date}")
        logger.info(f"Dependencies: {dependencies}")
        logger.info(f"Partition Info Table: {args.partition_info_table}")
        logger.info(f"Target Table: {args.target_table}")
        
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("MetricsPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Initialize BigQuery client
        bq_client = bigquery.Client()
        
        # Initialize pipeline
        pipeline = MetricsPipeline(spark, bq_client)
        
        # Execute pipeline steps
        logger.info("Step 1: Reading JSON from GCS")
        json_data = pipeline.read_json_from_gcs(args.gcs_path)
        
        logger.info("Step 2: Validating JSON data")
        validated_data = pipeline.validate_json(json_data)
        
        logger.info("Step 3: Processing metrics")
        metrics_df = pipeline.process_metrics(
            validated_data, 
            args.run_date, 
            dependencies, 
            args.partition_info_table
        )
        
        logger.info("Step 4: Aligning schema with BigQuery")
        aligned_df = pipeline.align_schema_with_bq(metrics_df, args.target_table)

        aligned_df.printSchema()
        aligned_df.show(truncate=False)
        
        logger.info("Step 5: Writing to BigQuery")
        pipeline.write_to_bq(aligned_df, args.target_table)
        
        logger.info("Pipeline completed successfully!")
        
    except MetricsPipelineError as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
    finally:
        # Clean up Spark session
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
