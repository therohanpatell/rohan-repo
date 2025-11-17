"""
BigQuery operations module for the Metrics Pipeline
Contains all BigQuery client operations including read, write, schema, and query operations
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    TimestampType, DecimalType, DoubleType, DateType, 
    StructType, StructField, StringType, IntegerType, BooleanType
)

from config import PipelineConfig, setup_logging
from exceptions import BigQueryError, SQLExecutionError, MetricsPipelineError, SchemaValidationError
from utils import StringUtils, NumericUtils

logger = setup_logging()


class BigQueryOperations:
    """Handles all BigQuery operations for the Metrics Pipeline"""
    
    def __init__(self, spark: SparkSession, bq_client: Optional[bigquery.Client] = None):
        """Initialize BigQuery operations"""
        self.spark = spark
        self.bq_client = bq_client or bigquery.Client(location="europe-west2")
    
    # Schema Operations
    def get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get BigQuery table schema"""
        try:
            table = self.bq_client.get_table(table_name)
            return table.schema
            
        except NotFound:
            raise BigQueryError(f"Table not found: {table_name}")
        except Exception as e:
            logger.error(f"Failed to get table schema: {str(e)}")
            raise BigQueryError(f"Failed to get table schema: {str(e)}")
    
    def get_spark_schema_from_bq_table(self, table_name: str) -> StructType:
        """
        Get Spark StructType schema from BigQuery table schema
        
        This method enables dynamic schema fetching by converting BigQuery table schemas
        to Spark StructType at runtime, eliminating the need for hardcoded schemas.
        
        Args:
            table_name: Full table name (project.dataset.table)
            
        Returns:
            Spark StructType schema matching the BigQuery table with proper type mappings
            and nullable/required field information preserved
            
        Raises:
            BigQueryError: If table not found or schema conversion fails
        """
        try:
            bq_schema = self.get_table_schema(table_name)
            
            spark_fields = []
            for field in bq_schema:
                field_name = field.name
                field_type = field.field_type.upper()
                nullable = field.mode != 'REQUIRED'
                
                if field_type in ['STRING', 'BYTES']:
                    spark_type = StringType()
                elif field_type in ['INTEGER', 'INT64']:
                    spark_type = IntegerType()
                elif field_type in ['FLOAT', 'FLOAT64']:
                    spark_type = DoubleType()
                elif field_type in ['NUMERIC', 'DECIMAL', 'BIGNUMERIC']:
                    spark_type = DecimalType(38, 9)
                elif field_type in ['BOOLEAN', 'BOOL']:
                    spark_type = BooleanType()
                elif field_type == 'DATE':
                    spark_type = DateType()
                elif field_type in ['TIMESTAMP', 'DATETIME']:
                    spark_type = TimestampType()
                elif field_type in ['GEOGRAPHY', 'JSON', 'ARRAY', 'STRUCT', 'RECORD']:
                    spark_type = StringType()
                else:
                    error_msg = f"Unsupported BigQuery data type '{field_type}' for field '{field_name}' in table {table_name}"
                    logger.error(error_msg)
                    raise BigQueryError(error_msg)
                
                spark_fields.append(StructField(field_name, spark_type, nullable))
            
            return StructType(spark_fields)
            
        except BigQueryError:
            raise
        except Exception as e:
            error_msg = f"Failed to convert BigQuery schema to Spark schema for {table_name}: {str(e)}"
            logger.error(error_msg)
            raise BigQueryError(error_msg)
    
    def align_dataframe_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame:
        """
        Align Spark DataFrame with BigQuery table schema with comprehensive validation
        
        This method ensures the DataFrame schema matches the target BigQuery table by:
        - Validating all required (non-nullable) columns are present
        - Adding null values for missing nullable columns
        - Dropping extra columns not in the target schema
        - Converting data types to match BigQuery expectations
        
        Enhanced for dynamic schema support: This method now validates that SQL query results
        contain all required columns from the target table, enabling flexible metric definitions
        without hardcoded schema constraints.
        
        Args:
            df: Spark DataFrame to align (typically created from SQL query results)
            target_table: Target BigQuery table name (project.dataset.table)
            
        Returns:
            DataFrame with aligned schema, column order, and data types matching target table
            
        Raises:
            SchemaValidationError: If required columns are missing from the DataFrame
        """
        bq_schema = self.get_table_schema(target_table)
        current_columns = df.columns
        bq_columns = [field.name for field in bq_schema]
        
        required_columns = [field.name for field in bq_schema if field.mode == 'REQUIRED']
        missing_required = [col_name for col_name in required_columns if col_name not in current_columns]
        
        if missing_required:
            error_msg = f"Missing required columns in DataFrame for table {target_table}: {missing_required}"
            logger.error(error_msg)
            raise SchemaValidationError(error_msg)
        
        nullable_columns = [field.name for field in bq_schema if field.mode != 'REQUIRED']
        missing_nullable = [col_name for col_name in nullable_columns if col_name not in current_columns]
        
        if missing_nullable:
            from pyspark.sql.functions import lit
            for col_name in missing_nullable:
                df = df.withColumn(col_name, lit(None))
        
        columns_to_drop = [col_name for col_name in current_columns if col_name not in bq_columns]
        if columns_to_drop:
            df = df.drop(*columns_to_drop)
        
        df = df.select(*[col(c) for c in bq_columns])
        
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
        
        return df
    
    # Query Operations
    def execute_query(self, query: str, timeout: int = PipelineConfig.QUERY_TIMEOUT) -> bigquery.table.RowIterator:
        """Execute a BigQuery SQL query"""
        try:
            query_job = self.bq_client.query(query)
            return query_job.result(timeout=timeout)
            
        except (DeadlineExceeded, TimeoutError):
            error_msg = f"Query timed out after {timeout} seconds"
            logger.error(error_msg)
            raise BigQueryError(error_msg)
        except Exception as e:
            error_msg = f"Failed to execute query: {str(e)}"
            logger.error(error_msg)
            raise BigQueryError(error_msg)
    
    def execute_sql_with_results(self, sql: str, metric_id: Optional[str] = None) -> List[Dict]:
        """
        Execute SQL query and return all results as list of dictionaries
        
        Modified to support multi-record SQL queries: Previously returned only the first row,
        now returns all rows to enable metrics that produce multiple output records (e.g.,
        grouped aggregations, time series data).
        
        Args:
            sql: SQL query to execute
            metric_id: Optional metric ID for error tracking
            
        Returns:
            List of dictionaries, one per result row, with all columns from the SQL result.
            Returns empty list if query produces no results.
            
        Raises:
            SQLExecutionError: If SQL execution fails or times out
        """
        try:
            query_job = self.bq_client.query(sql)
            results = query_job.result(timeout=PipelineConfig.QUERY_TIMEOUT)
            
            all_results = []
            for row in results:
                row_dict = dict(row)
                processed_row = {}
                for key, value in row_dict.items():
                    if hasattr(value, 'strftime'):
                        processed_row[key] = value.strftime('%Y-%m-%d')
                    else:
                        processed_row[key] = value
                all_results.append(processed_row)
            
            return all_results
            
        except (DeadlineExceeded, TimeoutError) as timeout_error:
            error_msg = f"Query for metric '{metric_id}' timed out after {PipelineConfig.QUERY_TIMEOUT} seconds. Error: {str(timeout_error)}"
            logger.error(error_msg)
            raise SQLExecutionError(error_msg, metric_id)
        
        except NotFound as nf:
            error_msg = f"Table or resource not found for metric '{metric_id}': {str(nf)}"
            logger.error(error_msg)
            raise SQLExecutionError(error_msg, metric_id)
        
        except GoogleCloudError as gce:
            error_msg = f"Google Cloud error executing SQL for metric '{metric_id}': {str(gce)}"
            logger.error(error_msg)
            raise SQLExecutionError(error_msg, metric_id)
        
        except Exception as e:
            error_type = type(e).__name__
            error_msg = f"Failed to execute SQL for metric '{metric_id}' with {error_type}: {str(e)}"
            logger.error(error_msg)
            raise SQLExecutionError(error_msg, metric_id)
    
    def get_partition_date(self, project_dataset: str, table_name: str, partition_info_table: str) -> Optional[str]:
        """
        Get latest partition_dt from metadata table
        
        Args:
            project_dataset: Project and dataset (project.dataset)
            table_name: Table name
            partition_info_table: Partition info table name
            
        Returns:
            Latest partition date as string or None if not found
        """
        try:
            query = f"""
            SELECT partition_dt 
            FROM `{partition_info_table}` 
            WHERE project_dataset = '{project_dataset}' 
            AND table_name = '{table_name}'
            """
            
            results = self.execute_query(query)
            
            for row in results:
                partition_dt = row.partition_dt
                if isinstance(partition_dt, datetime):
                    return partition_dt.strftime('%Y-%m-%d')
                return str(partition_dt)
            
            logger.warning(f"No partition info found for {project_dataset}.{table_name}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get partition_dt for {project_dataset}.{table_name}: {str(e)}")
            return None
    
    # Read Operations
    def check_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]:
        """
        Check which metric IDs already exist in BigQuery table for the given partition date
        
        Args:
            metric_ids: List of metric IDs to check
            partition_dt: Partition date
            target_table: Target table name
            
        Returns:
            List of existing metric IDs
            
        Raises:
            BigQueryError: If check operation fails
        """
        try:
            if not metric_ids:
                return []
            
            escaped_metric_ids = [StringUtils.escape_sql_string(mid) for mid in metric_ids]
            metric_ids_str = "', '".join(escaped_metric_ids)
            
            query = f"""
            SELECT DISTINCT metric_id 
            FROM `{target_table}` 
            WHERE metric_id IN ('{metric_ids_str}') 
            AND partition_dt = '{partition_dt}'
            """
            
            results = self.execute_query(query)
            existing_metrics = [row.metric_id for row in results]
            return existing_metrics
            
        except Exception as e:
            logger.error(f"Failed to check existing metrics: {str(e)}")
            raise BigQueryError(f"Failed to check existing metrics: {str(e)}")
    
    # Write Operations
    def write_dataframe_to_table(self, df: DataFrame, target_table: str, write_mode: str = "append") -> None:
        """
        Write Spark DataFrame to BigQuery table
        
        Args:
            df: Spark DataFrame to write
            target_table: Target BigQuery table
            write_mode: Write mode ('append', 'overwrite', etc.)
            
        Raises:
            BigQueryError: If write operation fails
        """
        try:
            df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode(write_mode) \
                .save()
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"Failed to write DataFrame to BigQuery: {error_message}")
            raise BigQueryError(f"Failed to write DataFrame to BigQuery: {error_message}")
    
    def write_metrics_with_overwrite(self, df: DataFrame, target_table: str) -> Tuple[List[str], List[Dict]]:
        """
        Write DataFrame to BigQuery table with robust overwrite capability for existing metrics
        
        Args:
            df: Spark DataFrame containing metrics
            target_table: Target BigQuery table
            
        Returns:
            Tuple of (successful_metric_ids, failed_metrics)
        """
        try:
            if df.count() == 0:
                return [], []
            
            metric_records = df.select('metric_id', 'partition_dt').distinct().collect()
            if not metric_records:
                return [], []
            
            partition_dt = metric_records[0]['partition_dt']
            metric_ids = [row['metric_id'] for row in metric_records]
            
            existing_metrics = self.check_existing_metrics(metric_ids, partition_dt, target_table)
            
            if existing_metrics:
                self.delete_metrics(existing_metrics, partition_dt, target_table)
            
            self.write_dataframe_to_table(df, target_table, "append")
            return metric_ids, []
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"Failed to write to BigQuery with overwrite: {error_message}")
            
            # Create failed metrics list for error reporting
            failed_metrics = []
            try:
                metric_records = df.select('metric_id').distinct().collect()
                for row in metric_records:
                    failed_metrics.append({
                        'metric_id': row['metric_id'],
                        'error_message': error_message
                    })
                logger.error(f"Failed metrics: {[fm['metric_id'] for fm in failed_metrics]}")
            except Exception as inner_e:
                logger.error(f"Could not extract failed metric IDs: {str(inner_e)}")
            
            return [], failed_metrics
    
    def write_recon_records(self, recon_records: List[Dict], recon_table: str) -> None:
        """
        Write reconciliation records to BigQuery recon table
        
        Args:
            recon_records: List of reconciliation records
            recon_table: Target recon table name
            
        Raises:
            BigQueryError: If write operation fails
        """
        try:
            if not recon_records:
                logger.info("No recon records to write")
                return
            
            logger.info(f"Writing {len(recon_records)} recon records to {recon_table}")
            
            # Validate recon records before creating DataFrame
            validation_errors = []
            required_fields = [
                'module_id', 'module_type_nm', 'source_server_nm', 'target_server_nm',
                'source_vl', 'target_vl', 'rcncln_exact_pass_in', 'latest_source_parttn_dt',
                'latest_target_parttn_dt', 'load_ts', 'schdld_dt', 'source_system_id',
                'schdld_yr', 'Job_Name'
            ]
            
            for i, record in enumerate(recon_records):
                if record is None:
                    validation_errors.append(f"Record at index {i} is None")
                    continue
                
                for field in required_fields:
                    if field not in record or record[field] is None:
                        metric_id = record.get('source_system_id', 'UNKNOWN')
                        validation_errors.append(f"Field '{field}' is None/missing in record {i} for metric {metric_id}")
            
            if validation_errors:
                logger.error(f"Recon record validation failed with {len(validation_errors)} errors")
                for error in validation_errors[:5]:
                    logger.error(f"  {error}")
                if len(validation_errors) > 5:
                    logger.error(f"  ... and {len(validation_errors) - 5} more errors")
                raise BigQueryError(f"Recon record validation failed with {len(validation_errors)} errors")
            
            # Create DataFrame
            try:
                recon_df = self.spark.createDataFrame(recon_records, PipelineConfig.RECON_SCHEMA)
            except ValueError as ve:
                error_msg = f"DataFrame creation failed due to value error: {str(ve)}"
                logger.error(error_msg)
                
                # Find records with None values
                for i, record in enumerate(recon_records[:5]):  # Check first 5 records
                    if record is None:
                        logger.error(f"  Record {i}: entire record is None")
                    else:
                        none_values = [(k, v) for k, v in record.items() if v is None]
                        if none_values:
                            logger.error(f"  Record {i} has None values: {[k for k, v in none_values]}")
                
                raise BigQueryError(f"Recon DataFrame creation failed (ValueError): {str(ve)}")
            except TypeError as te:
                error_msg = f"DataFrame creation failed due to type error: {str(te)}"
                logger.error(error_msg)
                raise BigQueryError(f"Recon DataFrame creation failed (TypeError): {str(te)}")
            except Exception as df_error:
                error_type = type(df_error).__name__
                error_msg = f"DataFrame creation failed with {error_type}: {str(df_error)}"
                logger.error(error_msg)
                
                # Find records with None values
                for i, record in enumerate(recon_records[:5]):  # Check first 5 records
                    if record is None:
                        logger.error(f"  Record {i}: entire record is None")
                    else:
                        none_values = [(k, v) for k, v in record.items() if v is None]
                        if none_values:
                            logger.error(f"  Record {i} has None values: {[k for k, v in none_values]}")
                
                raise BigQueryError(f"Recon DataFrame creation failed ({error_type}): {str(df_error)}")
            
            # Write to BigQuery
            try:
                self.write_dataframe_to_table(recon_df, recon_table, "append")
            except NotFound as nf:
                error_msg = f"Recon table not found: {recon_table}. Error: {str(nf)}"
                logger.error(error_msg)
                raise BigQueryError(error_msg)
            except GoogleCloudError as gce:
                error_msg = f"Google Cloud error writing recon records to {recon_table}: {str(gce)}"
                logger.error(error_msg)
                raise BigQueryError(error_msg)
            except Exception as write_error:
                error_type = type(write_error).__name__
                error_msg = f"Failed to write recon DataFrame to BigQuery with {error_type}: {str(write_error)}"
                logger.error(error_msg)
                raise BigQueryError(error_msg)
            
            # Log summary
            success_count = sum(1 for r in recon_records if r.get('rcncln_exact_pass_in') == 'Passed')
            failed_count = sum(1 for r in recon_records if r.get('rcncln_exact_pass_in') == 'Failed')
            logger.info(f"Recon records written: {success_count} passed, {failed_count} failed")
            
        except BigQueryError:
            # Re-raise BigQueryError as-is
            raise
        except Exception as e:
            error_type = type(e).__name__
            error_msg = f"Unexpected error writing recon records with {error_type}: {str(e)}"
            logger.error(error_msg)
            raise BigQueryError(error_msg)
    
    # Delete Operations
    def delete_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> None:
        """
        Delete existing metrics from BigQuery table for the given partition date
        
        Args:
            metric_ids: List of metric IDs to delete
            partition_dt: Partition date
            target_table: Target table name
            
        Raises:
            BigQueryError: If delete operation fails
        """
        try:
            if not metric_ids:
                return
            
            escaped_metric_ids = [StringUtils.escape_sql_string(mid) for mid in metric_ids]
            metric_ids_str = "', '".join(escaped_metric_ids)
            
            delete_query = f"""
            DELETE FROM `{target_table}` 
            WHERE metric_id IN ('{metric_ids_str}') 
            AND partition_dt = '{partition_dt}'
            """
            
            logger.info(f"Deleting {len(metric_ids)} existing metrics for overwrite")
            results = self.execute_query(delete_query)
            
            try:
                deleted_count = results.num_dml_affected_rows if hasattr(results, 'num_dml_affected_rows') else 0
                if deleted_count > 0:
                    logger.info(f"Deleted {deleted_count} existing records")
            except:
                pass
            
        except Exception as e:
            logger.error(f"Failed to delete existing metrics: {str(e)}")
            raise BigQueryError(f"Failed to delete existing metrics: {str(e)}")
    
    def validate_partition_info_table(self, partition_info_table: str) -> bool:
        """
        Validate that the partition info table exists and has required structure
        
        Args:
            partition_info_table: Full table name (project.dataset.table)
            
        Returns:
            True if table exists and has valid structure, False otherwise
            
        Raises:
            BigQueryError: If validation fails or table doesn't exist
        """
        try:
            logger.info(f"Validating partition info table: {partition_info_table}")
            
            # Check if table exists by getting its schema
            table_schema = self.get_table_schema(partition_info_table)
            
            # Check for required columns
            required_columns = ['project_dataset', 'table_name', 'partition_dt']
            schema_columns = [field.name for field in table_schema]
            
            missing_columns = [col for col in required_columns if col not in schema_columns]
            if missing_columns:
                raise BigQueryError(
                    f"Partition info table {partition_info_table} is missing required columns: {missing_columns}. "
                    f"Available columns: {schema_columns}"
                )
            
            # Test query to ensure table is accessible
            test_query = f"SELECT COUNT(*) as record_count FROM `{partition_info_table}` LIMIT 1"
            results = self.execute_query(test_query)
            
            for row in results:
                logger.info(f"Partition info table validated: {row.record_count} records")
                return True
            
            logger.info("Partition info table validated")
            return True
            
        except NotFound:
            raise BigQueryError(f"Partition info table does not exist: {partition_info_table}")
        except Exception as e:
            logger.error(f"Partition info table validation failed: {str(e)}")
            raise BigQueryError(f"Partition info table validation failed: {str(e)}")

    # Utility Methods
    def test_connection(self) -> bool:
        """
        Test BigQuery connection
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Simple query to test connection
            query = "SELECT 1 as test_value"
            results = self.execute_query(query)
            
            for row in results:
                if row.test_value == 1:
                    logger.info("BigQuery connection test successful")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"BigQuery connection test failed: {str(e)}")
            return False
    
    def get_client(self) -> bigquery.Client:
        """
        Get the BigQuery client instance
        
        Returns:
            BigQuery client instance
        """
        return self.bq_client


# Factory function for easy instantiation
def create_bigquery_operations(spark: SparkSession, bq_client: Optional[bigquery.Client] = None) -> BigQueryOperations:
    """
    Factory function to create BigQueryOperations instance
    
    Args:
        spark: SparkSession instance
        bq_client: Optional BigQuery client
        
    Returns:
        BigQueryOperations instance
    """
    return BigQueryOperations(spark, bq_client) 