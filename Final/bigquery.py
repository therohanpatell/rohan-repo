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
        """
        Initialize BigQuery operations
        
        Args:
            spark: SparkSession instance
            bq_client: Optional BigQuery client, will create one if not provided
        """
        self.spark = spark
        self.bq_client = bq_client or bigquery.Client(location="europe-west2")
        logger.info("BigQuery operations initialized")
    
    # Schema Operations
    def get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """
        Get BigQuery table schema
        
        Args:
            table_name: Full table name (project.dataset.table)
            
        Returns:
            List of BigQuery schema fields
            
        Raises:
            BigQueryError: If table not found or schema retrieval fails
        """
        try:
            logger.info(f"Getting schema for table: {table_name}")
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
            logger.info(f"Fetching Spark schema from BigQuery table: {table_name}")
            
            # Fetch BigQuery schema using existing get_table_schema method
            bq_schema = self.get_table_schema(table_name)
            
            # Map BigQuery data types to Spark data types
            # This mapping supports dynamic schema conversion for any BigQuery table structure
            spark_fields = []
            for field in bq_schema:
                field_name = field.name
                field_type = field.field_type.upper()
                # Preserve nullable/required information from BigQuery schema
                nullable = field.mode != 'REQUIRED'  # REQUIRED fields are not nullable
                
                # Map BigQuery types to Spark types with appropriate precision
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
                else:
                    # Unsupported type - log error and raise exception
                    error_msg = f"Unsupported BigQuery data type '{field_type}' for field '{field_name}' in table {table_name}"
                    logger.error(error_msg)
                    raise BigQueryError(error_msg)
                
                spark_fields.append(StructField(field_name, spark_type, nullable))
            
            spark_schema = StructType(spark_fields)
            logger.info(f"Successfully converted BigQuery schema to Spark schema for {table_name}")
            logger.debug(f"Spark schema: {spark_schema}")
            
            return spark_schema
            
        except BigQueryError:
            # Re-raise BigQueryError as-is
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
        logger.info(f"Aligning DataFrame schema with BigQuery table: {target_table}")
        logger.info(f"DataFrame current columns: {df.columns}")
        
        # Fetch BigQuery table schema to compare against DataFrame
        bq_schema = self.get_table_schema(target_table)
        current_columns = df.columns
        bq_columns = [field.name for field in bq_schema]
        
        logger.info(f"BigQuery table columns: {bq_columns}")
        
        # Identify required (non-nullable) and nullable columns from BigQuery schema
        # This distinction is critical for validation - required columns must be present in SQL results
        required_columns = [field.name for field in bq_schema if field.mode == 'REQUIRED']
        nullable_columns = [field.name for field in bq_schema if field.mode != 'REQUIRED']
        
        logger.info(f"Required columns in target table: {required_columns}")
        logger.info(f"Nullable columns in target table: {nullable_columns}")
        
        # Validate: Check for missing required columns
        # This ensures SQL queries return all mandatory columns for the target table
        missing_required = [col_name for col_name in required_columns if col_name not in current_columns]
        if missing_required:
            error_msg = f"Missing required columns in DataFrame for table {target_table}: {missing_required}"
            logger.error(error_msg)
            logger.error(f"DataFrame has columns: {current_columns}")
            logger.error(f"Target table requires: {required_columns}")
            raise SchemaValidationError(error_msg)
        
        logger.info("All required columns are present in DataFrame")
        
        # Identify missing nullable columns that need to be added with null values
        missing_nullable = [col_name for col_name in nullable_columns if col_name not in current_columns]
        
        # Add null values for missing nullable columns
        # This allows SQL queries to omit optional columns - they'll be filled with nulls
        if missing_nullable:
            logger.info(f"Adding null values for {len(missing_nullable)} missing nullable columns: {missing_nullable}")
            from pyspark.sql.functions import lit
            for col_name in missing_nullable:
                logger.info(f"  Adding column '{col_name}' with null values")
                df = df.withColumn(col_name, lit(None))
        else:
            logger.info("No missing nullable columns to add")
        
        # Drop extra columns not in BigQuery schema
        # SQL queries may return additional columns (e.g., intermediate calculations) that aren't in target table
        columns_to_drop = [col_name for col_name in current_columns if col_name not in bq_columns]
        
        if columns_to_drop:
            logger.info(f"Dropping {len(columns_to_drop)} extra columns not in target schema: {columns_to_drop}")
            for col_name in columns_to_drop:
                logger.info(f"  Dropping column '{col_name}'")
            df = df.drop(*columns_to_drop)
        else:
            logger.info("No extra columns to drop")
        
        # Reorder columns to match BigQuery schema order
        # This ensures consistent column ordering regardless of SQL query structure
        logger.info("Reordering columns to match BigQuery schema")
        df = df.select(*[col(c) for c in bq_columns])
        
        # Handle type conversions for BigQuery compatibility
        # Convert Spark types to match BigQuery expectations for proper data loading
        logger.info("Applying type conversions for BigQuery compatibility")
        for field in bq_schema:
            if field.name in df.columns:
                if field.field_type == 'DATE':
                    logger.info(f"  Converting column '{field.name}' to DATE type")
                    df = df.withColumn(field.name, to_date(col(field.name)))
                elif field.field_type == 'TIMESTAMP':
                    logger.info(f"  Converting column '{field.name}' to TIMESTAMP type")
                    df = df.withColumn(field.name, col(field.name).cast(TimestampType()))
                elif field.field_type == 'NUMERIC':
                    logger.info(f"  Converting column '{field.name}' to NUMERIC type (Decimal 38,9)")
                    df = df.withColumn(field.name, col(field.name).cast(DecimalType(38, 9)))
                elif field.field_type == 'FLOAT':
                    logger.info(f"  Converting column '{field.name}' to FLOAT type (Double)")
                    df = df.withColumn(field.name, col(field.name).cast(DoubleType()))
        
        logger.info(f"Schema alignment complete. Final columns: {df.columns}")
        logger.info("DataFrame schema after alignment:")
        df.printSchema()
        
        return df
    
    # Query Operations
    def execute_query(self, query: str, timeout: int = PipelineConfig.QUERY_TIMEOUT) -> bigquery.table.RowIterator:
        """
        Execute a BigQuery SQL query
        
        Args:
            query: SQL query to execute
            timeout: Query timeout in seconds
            
        Returns:
            BigQuery query results
            
        Raises:
            BigQueryError: If query execution fails
        """
        try:
            logger.info("Executing BigQuery SQL query")
            logger.debug(f"Query: {query}")
            
            query_job = self.bq_client.query(query)
            results = query_job.result(timeout=timeout)
            
            logger.info("Query executed successfully")
            return results
            
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
            logger.info("Executing SQL query and returning all results")
            
            query_job = self.bq_client.query(sql)
            results = query_job.result(timeout=PipelineConfig.QUERY_TIMEOUT)
            
            # Iterate through all rows and return all columns (multi-record support)
            # No longer limited to first row - supports SQL queries returning multiple records
            all_results = []
            for row in results:
                row_dict = dict(row)
                
                # Convert all values to appropriate types for DataFrame compatibility
                processed_row = {}
                for key, value in row_dict.items():
                    # Handle datetime objects by converting to string format
                    if hasattr(value, 'strftime'):
                        processed_row[key] = value.strftime('%Y-%m-%d')
                    else:
                        processed_row[key] = value
                
                all_results.append(processed_row)
            
            # Log record count for monitoring multi-record queries
            logger.info(f"SQL query returned {len(all_results)} records")
            return all_results
            
        except (DeadlineExceeded, TimeoutError):
            error_msg = f"Query for metric '{metric_id}' timed out after {PipelineConfig.QUERY_TIMEOUT} seconds."
            logger.error(error_msg)
            raise SQLExecutionError(error_msg, metric_id)
        
        except Exception as e:
            error_msg = f"Failed to execute SQL: {str(e)}"
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
            
            logger.info(f"Querying partition info for {project_dataset}.{table_name}")
            
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
            
            logger.info(f"Checking existing metrics for partition_dt: {partition_dt}")
            logger.debug(f"Query: {query}")
            
            results = self.execute_query(query)
            existing_metrics = [row.metric_id for row in results]
            
            if existing_metrics:
                logger.info(f"Found {len(existing_metrics)} existing metrics: {existing_metrics}")
            else:
                logger.info("No existing metrics found")
            
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
            logger.info(f"Writing DataFrame to BigQuery table: {target_table}")
            logger.info(f"Write mode: {write_mode}")
            logger.info(f"Records to write: {df.count()}")
            
            df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode(write_mode) \
                .save()
            
            logger.info(f"Successfully wrote DataFrame to {target_table}")
            
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
            logger.info(f"Writing DataFrame to BigQuery table with overwrite: {target_table}")
            
            # Validate input DataFrame
            if df.count() == 0:
                logger.warning("No records to process - DataFrame is empty")
                return [], []
            
            # Get metrics info for processing
            metric_records = df.select('metric_id', 'partition_dt').distinct().collect()
            
            if not metric_records:
                logger.warning("No valid metric records found")
                return [], []
            
            partition_dt = metric_records[0]['partition_dt']
            metric_ids = [row['metric_id'] for row in metric_records]
            record_count = df.count()
            
            logger.info(f"Processing {len(metric_ids)} unique metrics ({record_count} total records) for partition_dt: {partition_dt}")
            
            # Check which metrics already exist for robust overwrite
            existing_metrics = self.check_existing_metrics(metric_ids, partition_dt, target_table)
            new_metrics = [mid for mid in metric_ids if mid not in existing_metrics]
            
            # Delete existing metrics if any (part of overwrite operation)
            if existing_metrics:
                logger.info(f"Found {len(existing_metrics)} existing metrics to overwrite")
                self.delete_metrics(existing_metrics, partition_dt, target_table)
            
            if new_metrics:
                logger.info(f"Adding {len(new_metrics)} new metrics")
            
            # Write the DataFrame to BigQuery (append mode after deletion = overwrite)
            logger.info(f"Writing {record_count} records to {target_table}")
            self.write_dataframe_to_table(df, target_table, "append")
            
            # Final success logging
            logger.info(f"Successfully completed overwrite operation for {target_table}")
            logger.info(f"Total metrics processed: {len(metric_ids)} ({len(existing_metrics)} overwritten, {len(new_metrics)} new)")
            
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
            
            logger.info("Starting recon records write to BigQuery...")
            logger.info(f"   Number of records: {len(recon_records)}")
            logger.info(f"   Target table: {recon_table}")
            
            # Validate recon records before creating DataFrame
            logger.info("Validating recon records before DataFrame creation...")
            validation_errors = []
            
            for i, record in enumerate(recon_records):
                if record is None:
                    validation_errors.append(f"Record at index {i} is None")
                    continue
                
                # Check required non-nullable fields
                required_fields = [
                    'module_id', 'module_type_nm', 'source_server_nm', 'target_server_nm',
                    'source_vl', 'target_vl', 'rcncln_exact_pass_in', 'latest_source_parttn_dt',
                    'latest_target_parttn_dt', 'load_ts', 'schdld_dt', 'source_system_id',
                    'schdld_yr', 'Job_Name'
                ]
                
                for field in required_fields:
                    if field not in record or record[field] is None:
                        metric_id = record.get('source_system_id', 'UNKNOWN')
                        validation_errors.append(f"Field '{field}' is None/missing in record {i} for metric {metric_id}")
            
            if validation_errors:
                logger.error("Recon record validation failed:")
                for error in validation_errors[:10]:  # Show first 10 errors
                    logger.error(f"   {error}")
                if len(validation_errors) > 10:
                    logger.error(f"   ... and {len(validation_errors) - 10} more validation errors")
                raise BigQueryError(f"Recon record validation failed with {len(validation_errors)} errors")
            
            logger.info("All recon records validated successfully")
            
            logger.info("Creating Spark DataFrame from recon records...")
            
            # Validate recon records before DataFrame creation
            logger.info("PRE-VALIDATING RECON RECORDS FOR DATAFRAME CREATION...")
            logger.info(f"   Total records to validate: {len(recon_records)}")
            
            for i, record in enumerate(recon_records):
                logger.info(f"VALIDATING RECORD {i+1}/{len(recon_records)}:")
                
                if record is None:
                    logger.error(f"CRITICAL: Recon record at index {i} is None!")
                    raise BigQueryError(f"Recon record at index {i} is None")
                
                logger.info(f"   Record type: {type(record)}")
                logger.info(f"   Record keys: {list(record.keys()) if isinstance(record, dict) else 'NOT A DICT'}")
                
                # Check critical non-nullable fields
                required_fields = [
                    'module_id', 'module_type_nm', 'source_server_nm', 'target_server_nm',
                    'source_vl', 'target_vl', 'rcncln_exact_pass_in', 'latest_source_parttn_dt',
                    'latest_target_parttn_dt', 'load_ts', 'schdld_dt', 'source_system_id',
                    'schdld_yr', 'Job_Name'
                ]
                
                metric_id = record.get('source_system_id', 'UNKNOWN') if isinstance(record, dict) else 'UNKNOWN'
                logger.info(f"   Metric ID: {metric_id}")
                
                none_fields = []
                missing_fields = []
                
                for field in required_fields:
                    if field not in record:
                        missing_fields.append(field)
                        logger.error(f"   MISSING FIELD: '{field}' not in record")
                    elif record[field] is None:
                        none_fields.append(field)
                        logger.error(f"   NONE VALUE: '{field}' is None")
                    else:
                        field_value = record[field]
                        field_type = type(field_value).__name__
                        logger.info(f"   {field}: {repr(field_value)} (type: {field_type})")
                
                if missing_fields or none_fields:
                    logger.error(f"VALIDATION FAILED FOR RECORD {i} (metric: {metric_id}):")
                    if missing_fields:
                        logger.error(f"   Missing fields: {missing_fields}")
                    if none_fields:
                        logger.error(f"   None value fields: {none_fields}")
                    
                    logger.error(f"   FULL RECORD CONTENT:")
                    for key, value in record.items():
                        logger.error(f"     {key}: {repr(value)} (type: {type(value)})")
                    
                    error_msg = f"Validation failed for record {i} (metric: {metric_id})"
                    if missing_fields:
                        error_msg += f" - Missing fields: {missing_fields}"
                    if none_fields:
                        error_msg += f" - None value fields: {none_fields}"
                    
                    raise BigQueryError(error_msg)
                
                logger.info(f"   Record {i+1} validation PASSED")
            
            logger.info("ALL RECON RECORDS PRE-VALIDATION COMPLETED SUCCESSFULLY")
            
            logger.info("ATTEMPTING TO CREATE SPARK DATAFRAME...")
            logger.info(f"   Number of records: {len(recon_records)}")
            logger.info(f"   Schema: {PipelineConfig.RECON_SCHEMA}")
            
            try:
                logger.info("   Calling spark.createDataFrame()...")
                recon_df = self.spark.createDataFrame(recon_records, PipelineConfig.RECON_SCHEMA)
                logger.info("SPARK DATAFRAME CREATED SUCCESSFULLY")
            except Exception as df_error:
                logger.error("FAILED TO CREATE SPARK DATAFRAME!")
                logger.error(f"   Error type: {type(df_error).__name__}")
                logger.error(f"   Error message: {str(df_error)}")
                
                # Log detailed information about the error
                logger.error("DEBUGGING DATAFRAME CREATION FAILURE:")
                logger.error(f"   Total records: {len(recon_records)}")
                
                # Show first few records that caused the error
                logger.error("   FIRST 3 RECON RECORDS THAT CAUSED THE ERROR:")
                for i, record in enumerate(recon_records[:3]):
                    logger.error(f"     Record {i}:")
                    if record is None:
                        logger.error(f"       RECORD IS NONE!")
                    else:
                        for key, value in record.items():
                            if value is None:
                                logger.error(f"       {key}: None (THIS IS THE PROBLEM!)")
                            else:
                                logger.error(f"       {key}: {repr(value)} (type: {type(value)})")
                
                # Check if it's the specific "Argument obj can not be None" error
                if "can not be None" in str(df_error) or "cannot be None" in str(df_error):
                    logger.error("THIS IS THE 'ARGUMENT OBJ CAN NOT BE NONE' ERROR!")
                    logger.error("   This means one of the record values is None when it shouldn't be")
                    
                    # Find the exact None values
                    logger.error("SCANNING ALL RECORDS FOR NONE VALUES:")
                    for i, record in enumerate(recon_records):
                        if record is None:
                            logger.error(f"   Record {i}: ENTIRE RECORD IS NONE!")
                        else:
                            none_values = [(k, v) for k, v in record.items() if v is None]
                            if none_values:
                                logger.error(f"   Record {i} has None values: {none_values}")
                
                raise BigQueryError(f"Failed to create Spark DataFrame from recon records: {str(df_error)}")
            
            logger.info("Recon DataFrame Schema:")
            recon_df.printSchema()
            
            logger.info("Sample Recon Data (first 5 records):")
            recon_df.show(5, truncate=False)
            
            # Count success/failure records for logging
            success_records = [r for r in recon_records if r.get('rcncln_exact_pass_in') == 'Passed']
            failed_records = [r for r in recon_records if r.get('rcncln_exact_pass_in') == 'Failed']
            
            logger.info("Recon Records Summary:")
            logger.info(f"   Success records: {len(success_records)}")
            logger.info(f"   Failed records: {len(failed_records)}")
            
            if failed_records:
                logger.info("Failed Records Details (first 5):")
                for i, record in enumerate(failed_records[:5], 1):
                    metric_id = record.get('source_system_id', 'UNKNOWN')
                    reason = record.get('excldd_reason_tx', 'No reason provided')
                    reason_short = reason[:100] + "..." if len(reason) > 100 else reason
                    logger.info(f"   {i}. Metric {metric_id}: {reason_short}")
            
            logger.info("Writing DataFrame to BigQuery table...")
            self.write_dataframe_to_table(recon_df, recon_table, "append")
            
            logger.info("RECON RECORDS WRITE COMPLETED SUCCESSFULLY")
            logger.info(f"   Total records written: {len(recon_records)}")
            logger.info(f"   Target table: {recon_table}")
            
        except Exception as e:
            logger.error("RECON RECORDS WRITE FAILED")
            logger.error(f"   Error: {str(e)}")
            logger.error(f"   Error type: {type(e).__name__}")
            raise BigQueryError(f"Failed to write recon records: {str(e)}")
    
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
                logger.info("No metrics to delete")
                return
            
            escaped_metric_ids = [StringUtils.escape_sql_string(mid) for mid in metric_ids]
            metric_ids_str = "', '".join(escaped_metric_ids)
            
            delete_query = f"""
            DELETE FROM `{target_table}` 
            WHERE metric_id IN ('{metric_ids_str}') 
            AND partition_dt = '{partition_dt}'
            """
            
            logger.info(f"Overwriting {len(metric_ids)} existing metrics for partition_dt: {partition_dt}")
            logger.debug(f"Delete query: {delete_query}")
            
            results = self.execute_query(delete_query)
            
            # Try to get the number of affected rows (not always available)
            try:
                deleted_count = results.num_dml_affected_rows if hasattr(results, 'num_dml_affected_rows') else 0
                logger.info(f"Successfully deleted {deleted_count} existing records for overwrite")
            except:
                logger.info(f"Successfully executed delete query for overwrite operation")
            
        except Exception as e:
            logger.error(f"Failed to delete existing metrics for overwrite: {str(e)}")
            raise BigQueryError(f"Failed to delete existing metrics for overwrite: {str(e)}")
    
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
                logger.info(f"Partition info table validation successful. Table has {row.record_count} records.")
                return True
            
            logger.info("Partition info table validation successful")
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