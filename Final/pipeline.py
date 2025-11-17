"""
Core pipeline logic for the Metrics Pipeline
Contains the main MetricsPipeline class with all business logic
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from config import PipelineConfig, ValidationConfig, setup_logging
from exceptions import MetricsPipelineError, ValidationError, SQLExecutionError, BigQueryError, GCSError
from utils import (DateUtils, NumericUtils, SQLUtils, ValidationUtils, StringUtils, ExecutionUtils)
from bigquery import BigQueryOperations

logger = setup_logging()


class MetricsPipeline:
    """Main pipeline class for processing metrics"""

    def __init__(self, spark: SparkSession, bq_operations: BigQueryOperations):
        self.spark = spark  # Spark session for data processing
        self.bq_operations = bq_operations  # BigQuery client operations
        self.execution_id = ExecutionUtils.generate_execution_id()  # Unique ID for this pipeline run
        self.processed_metrics = []  # Track successfully processed metrics
        self.target_tables = set()  # Track unique target tables used
        self.schema_cache: Dict[str, StructType] = {}  # Cache schemas by table name to avoid redundant BigQuery calls

    # Schema Caching Operations
    def get_cached_schema(self, target_table: str) -> StructType:
        """
        Get schema for target table with caching to avoid redundant BigQuery calls
        
        Schema caching optimization: When multiple metrics target the same table, this method
        reuses the fetched schema instead of making repeated BigQuery API calls. This significantly
        reduces API overhead and improves pipeline performance.
        
        Args:
            target_table: Full table name (project.dataset.table)
            
        Returns:
            Spark StructType schema for the table (either from cache or freshly fetched)
            
        Raises:
            BigQueryError: If schema fetch fails
        """
        if target_table in self.schema_cache:
            return self.schema_cache[target_table]
        
        try:
            schema = self.bq_operations.get_spark_schema_from_bq_table(target_table)
            self.schema_cache[target_table] = schema
            return schema
            
        except Exception as e:
            error_msg = f"Failed to fetch schema for target table {target_table}: {str(e)}"
            logger.error(error_msg)
            raise BigQueryError(error_msg)

    def _merge_sql_json_with_schema(self, sql_row: Dict, json_record: Dict, 
                                     table_schema: StructType, partition_dt: str, 
                                     metric_id: str) -> Dict:
        """
        Dynamically merge SQL results and JSON metadata based on BigQuery schema
        
        Merge priority for each column in BigQuery schema:
        1. SQL result (if column exists in SQL)
        2. JSON metadata (if column exists in JSON)
        3. NULL (if column is nullable)
        4. ERROR (if column is required/non-nullable)
        
        Args:
            sql_row: Dictionary of SQL query results
            json_record: Dictionary of JSON metadata
            table_schema: BigQuery table schema (StructType)
            partition_dt: Partition date for this record
            metric_id: Metric ID for error messages
            
        Returns:
            Final merged record with all BigQuery columns populated
            
        Raises:
            ValidationError: If required column is missing from both SQL and JSON
        """
        final_record = {}
        missing_required_columns = []
        
        # Reserved columns that are added by pipeline (not from SQL or JSON)
        pipeline_columns = {'partition_dt', 'pipeline_execution_ts'}
        
        # Iterate through all columns in BigQuery schema
        for field in table_schema.fields:
            column_name = field.name
            is_nullable = field.nullable
            
            # Skip pipeline-managed columns (handled separately)
            if column_name in pipeline_columns:
                continue
            
            # Priority 1: Check if column exists in SQL result
            if column_name in sql_row:
                final_record[column_name] = self._convert_value_to_schema_type(
                    sql_row[column_name], field, column_name
                )
                logger.debug(f"         Column '{column_name}' populated from SQL result")
                continue
            
            # Priority 2: Check if column exists in JSON metadata
            if column_name in json_record and json_record[column_name] is not None:
                final_record[column_name] = self._convert_value_to_schema_type(
                    json_record[column_name], field, column_name
                )
                logger.debug(f"         Column '{column_name}' populated from JSON metadata")
                continue
            
            # Priority 3: If nullable, set to None
            if is_nullable:
                final_record[column_name] = None
                logger.debug(f"         Column '{column_name}' set to NULL (nullable column, not in SQL or JSON)")
                continue
            
            # Priority 4: If required (non-nullable), this is an error
            missing_required_columns.append(column_name)
            logger.error(f"         Column '{column_name}' is REQUIRED but missing from both SQL and JSON")
        
        # If any required columns are missing, raise validation error
        if missing_required_columns:
            error_msg = (f"Metric {metric_id}: Required columns missing from both SQL and JSON: "
                        f"{missing_required_columns}. These columns are non-nullable in BigQuery schema.")
            raise ValidationError(error_msg)
        
        # Add pipeline metadata columns with type conversion
        # Convert partition_dt string to appropriate type based on schema
        for field in table_schema.fields:
            if field.name == 'partition_dt':
                final_record['partition_dt'] = self._convert_value_to_schema_type(
                    partition_dt, field, 'partition_dt'
                )
            elif field.name == 'pipeline_execution_ts':
                final_record['pipeline_execution_ts'] = DateUtils.get_current_timestamp()
        
        return final_record

    def _convert_value_to_schema_type(self, value, field, column_name: str):
        """
        Convert a value to match the BigQuery schema field type
        
        Handles all common BigQuery/Spark data types with proper conversion:
        - DateType: String "YYYY-MM-DD" → date object
        - TimestampType: String ISO format → datetime object
        - IntegerType: String/float → int
        - DoubleType/FloatType: String → float
        - DecimalType: String/int/float → Decimal
        - BooleanType: String/int → bool
        - StringType: Any → string
        
        Args:
            value: The value to convert
            field: StructField from BigQuery schema
            column_name: Name of the column (for error messages)
            
        Returns:
            Converted value matching the field type
            
        Raises:
            ValidationError: If conversion fails
        """
        from pyspark.sql.types import (
            DateType, TimestampType, StringType, IntegerType, 
            LongType, DoubleType, FloatType, DecimalType, BooleanType
        )
        from datetime import datetime, date
        from decimal import Decimal, InvalidOperation
        
        # Handle None/NULL values for all types
        if value is None:
            return None
        
        try:
            # DateType: Convert string "YYYY-MM-DD" to date object
            if isinstance(field.dataType, DateType):
                if isinstance(value, date):
                    return value
                elif isinstance(value, datetime):
                    return value.date()
                elif isinstance(value, str):
                    # Try multiple date formats
                    for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y']:
                        try:
                            return datetime.strptime(value, fmt).date()
                        except ValueError:
                            continue
                    raise ValueError(f"Date string '{value}' does not match expected formats (YYYY-MM-DD, etc.)")
                else:
                    raise ValueError(f"Cannot convert {type(value).__name__} to date")
            
            # TimestampType: Convert string to datetime object
            elif isinstance(field.dataType, TimestampType):
                if isinstance(value, datetime):
                    return value
                elif isinstance(value, date):
                    return datetime.combine(value, datetime.min.time())
                elif isinstance(value, str):
                    # Try multiple timestamp formats
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']:
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                    # Try ISO format parsing
                    try:
                        return datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except:
                        raise ValueError(f"Timestamp string '{value}' does not match expected formats")
                else:
                    return value
            
            # IntegerType/LongType: Convert to int
            elif isinstance(field.dataType, (IntegerType, LongType)):
                if isinstance(value, (int, bool)):
                    return int(value)
                elif isinstance(value, float):
                    return int(value)
                elif isinstance(value, str):
                    # Handle empty strings
                    if value.strip() == '':
                        return None
                    return int(float(value))  # Handle "123.0" strings
                elif isinstance(value, Decimal):
                    return int(value)
                else:
                    raise ValueError(f"Cannot convert {type(value).__name__} to integer")
            
            # DoubleType/FloatType: Convert to float
            elif isinstance(field.dataType, (DoubleType, FloatType)):
                if isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, str):
                    if value.strip() == '':
                        return None
                    return float(value)
                elif isinstance(value, Decimal):
                    return float(value)
                else:
                    raise ValueError(f"Cannot convert {type(value).__name__} to float")
            
            # DecimalType: Convert to Decimal
            elif isinstance(field.dataType, DecimalType):
                if isinstance(value, Decimal):
                    return value
                elif isinstance(value, (int, float)):
                    return Decimal(str(value))
                elif isinstance(value, str):
                    if value.strip() == '':
                        return None
                    return Decimal(value)
                else:
                    raise ValueError(f"Cannot convert {type(value).__name__} to Decimal")
            
            # BooleanType: Convert to bool
            elif isinstance(field.dataType, BooleanType):
                if isinstance(value, bool):
                    return value
                elif isinstance(value, int):
                    return bool(value)
                elif isinstance(value, str):
                    value_lower = value.lower().strip()
                    if value_lower in ['true', 't', 'yes', 'y', '1']:
                        return True
                    elif value_lower in ['false', 'f', 'no', 'n', '0', '']:
                        return False
                    else:
                        raise ValueError(f"Cannot convert string '{value}' to boolean")
                else:
                    return bool(value)
            
            # StringType: Convert to string
            elif isinstance(field.dataType, StringType):
                if isinstance(value, str):
                    return value
                elif isinstance(value, (date, datetime)):
                    return value.isoformat()
                elif isinstance(value, Decimal):
                    return str(value)
                else:
                    return str(value)
            
            # For any other types, return as-is
            else:
                logger.debug(f"No conversion defined for type {type(field.dataType).__name__}, returning value as-is")
                return value
                
        except (ValueError, InvalidOperation) as e:
            error_msg = f"Type conversion failed for column '{column_name}': Cannot convert value '{value}' (type: {type(value).__name__}) to {type(field.dataType).__name__}. Error: {str(e)}"
            logger.error(error_msg)
            raise ValidationError(error_msg)

    # Validation Operations
    def validate_partition_info_table(self, partition_info_table: str) -> None:
        """Validate partition info table exists and has required structure"""
        try:
            self.bq_operations.validate_partition_info_table(partition_info_table)
        except Exception as e:
            error_msg = f"Partition info table validation failed: {str(e)}"
            logger.error(error_msg)
            raise MetricsPipelineError(error_msg)

    # GCS Operations
    def validate_gcs_path(self, gcs_path: str) -> str:
        """Validate GCS path format and accessibility"""
        if not gcs_path.startswith('gs://'):
            raise GCSError(f"Invalid GCS path format: {gcs_path}. Must start with 'gs://'")

        path_parts = gcs_path.replace('gs://', '').split('/')
        if len(path_parts) < 2:
            raise GCSError(f"Invalid GCS path structure: {gcs_path}")

        try:
            test_df = self.spark.read.option("multiline", "true").json(gcs_path).limit(0)
            test_df.count()
            return gcs_path
        except Exception as e:
            raise GCSError(f"GCS path inaccessible: {gcs_path}. Error: {str(e)}")

    def read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """Read JSON file from GCS and return as list of dictionaries"""
        try:
            validated_path = self.validate_gcs_path(gcs_path)
            df = self.spark.read.option("multiline", "true").json(validated_path)

            if df.count() == 0:
                raise GCSError(f"No data found in JSON file: {validated_path}")

            if len(df.columns) == 0:
                raise GCSError(f"Invalid JSON structure in {validated_path}")

            json_data = [row.asDict() for row in df.collect()]
            return json_data

        except Exception as e:
            logger.error(f"Failed to read JSON from GCS: {str(e)}")
            raise GCSError(f"Failed to read JSON from GCS: {str(e)}")

    # Data Validation
    def validate_json(self, json_data: List[Dict]) -> List[Dict]:
        """Validate JSON data for required fields and duplicates"""
        metric_ids = set()
        for i, record in enumerate(json_data):
            ValidationUtils.validate_json_record(record, i, metric_ids)
        return json_data

    # SQL Processing
    def get_partition_dt(self, project_dataset: str, table_name: str, partition_info_table: str) -> Optional[str]:
        """Get latest partition_dt from metadata table"""
        return self.bq_operations.get_partition_date(project_dataset, table_name, partition_info_table)

    def replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str) -> str:
        """Replace {currently} and {partition_info} placeholders in SQL with appropriate dates"""
        try:
            placeholders = SQLUtils.find_placeholder_positions(sql)
            if not placeholders:
                return sql

            final_sql = sql
            for placeholder_type, start_pos, end_pos in reversed(placeholders):
                if placeholder_type == 'currently':
                    replacement_date = run_date
                elif placeholder_type == 'partition_info':
                    table_info = SQLUtils.get_table_for_placeholder(sql, start_pos)
                    if table_info:
                        dataset, table_name = table_info
                        replacement_date = self.get_partition_dt(dataset, table_name, partition_info_table)
                        if not replacement_date:
                            raise SQLExecutionError(f"Could not determine partition_dt for table {dataset}.{table_name}")
                    else:
                        raise SQLExecutionError(f"Could not find table reference for {{partition_info}} placeholder at position {start_pos}")

                final_sql = final_sql[:start_pos] + f"'{replacement_date}'" + final_sql[end_pos:]

            return final_sql

        except Exception as e:
            logger.error(f"Failed to replace SQL placeholders: {str(e)}")
            raise SQLExecutionError(f"Failed to replace SQL placeholders: {str(e)}")

    def execute_sql(self, sql: str, run_date: str, partition_info_table: str, metric_id: Optional[str] = None) -> List[Dict]:
        """Execute SQL query with dynamic placeholder replacement and return all result rows"""
        final_sql = self.replace_sql_placeholders(sql, run_date, partition_info_table)
        return self.bq_operations.execute_sql_with_results(final_sql, metric_id)

    # Metrics Processing
    def check_dependencies_exist(self, json_data: List[Dict], dependencies: List[str]) -> None:
        """Check if all specified dependencies exist in the JSON data"""
        available_dependencies = set(record['dependency'] for record in json_data)
        missing_dependencies = set(dependencies) - available_dependencies

        if missing_dependencies:
            raise ValidationError(f"Missing dependencies in JSON data: {missing_dependencies}. "
                                  f"Available dependencies: {available_dependencies}")

    def process_metrics(self, json_data: List[Dict], run_date: str, dependencies: List[str],
                        partition_info_table: str) -> Tuple[Dict[str, DataFrame], List[Dict], List[Dict]]:
        """
        Process metrics and create Spark DataFrames grouped by target_table with dynamic schema support
        
        Major changes for dynamic schema implementation:
        1. Fetches schema dynamically from BigQuery target tables (with caching)
        2. Supports multi-record SQL results (processes all rows, not just first)
        3. Creates DataFrames without hardcoded schema (Spark infers from data)
        4. Validates schema alignment with comprehensive error handling
        
        Args:
            json_data: List of metric configuration dictionaries from JSON
            run_date: Run date for SQL placeholder replacement
            dependencies: List of dependencies to filter and process
            partition_info_table: Table for partition date lookups
            
        Returns:
            Tuple of (result_dataframes, successful_metrics, failed_metrics) where:
            - result_dataframes: Dict mapping target_table to aligned DataFrame
            - successful_metrics: List of successfully processed metric records
            - failed_metrics: List of dicts with metric_record, error_message, error_category
        """
        self.check_dependencies_exist(json_data, dependencies)
        partition_dt = run_date
        filtered_data = [record for record in json_data if record['dependency'] in dependencies]

        if not filtered_data:
            logger.error(f"No records found for dependencies: {dependencies}")
            raise ValidationError(f"No records found for dependencies: {dependencies}")

        records_by_table = {}
        for record in filtered_data:
            target_table = record['target_table'].strip()
            if target_table not in records_by_table:
                records_by_table[target_table] = []
            records_by_table[target_table].append(record)

        result_dfs = {}
        successful_metrics = []
        failed_metrics = []

        for target_table, records in records_by_table.items():
            try:
                table_schema = self.get_cached_schema(target_table)
            except Exception as schema_error:
                error_message = f"Failed to fetch schema for target table {target_table}: {str(schema_error)}"
                logger.error(error_message)
                for record in records:
                    failed_metrics.append({
                        'metric_record': record,
                        'error_message': error_message,
                        'error_category': 'SCHEMA_VALIDATION_ERROR'
                    })
                continue

            processed_records = []

            for record in records:
                metric_id = record['metric_id']
                try:
                    logger.info(f"Executing metric: {metric_id}")
                    sql_results = self.execute_sql(record['sql'], run_date, partition_info_table, record['metric_id'])

                    if isinstance(sql_results, list):
                        for sql_row in sql_results:
                            final_record = self._merge_sql_json_with_schema(
                                sql_row, record, table_schema, partition_dt, metric_id
                            )
                            processed_records.append(final_record)
                    else:
                        final_record = self._merge_sql_json_with_schema(
                            sql_results, record, table_schema, partition_dt, metric_id
                        )
                        processed_records.append(final_record)

                    # Store the actual metric_id that will be written to BigQuery
                    # If SQL returns metric_id, it takes priority over JSON metric_id
                    # This ensures recon records and delete operations use the correct metric_id
                    actual_metric_id = final_record.get('metric_id', metric_id)
                    record['_actual_metric_id'] = actual_metric_id
                    successful_metrics.append(record)
                    logger.info(f"Metric {metric_id} executed successfully")

                except ValidationError as ve:
                    error_message = str(ve)
                    error_category = "VALIDATION_ERROR"
                    logger.error(f"Metric {metric_id} validation failed: {error_message}")
                    failed_metrics.append(
                        {'metric_record': record, 'error_message': error_message, 'error_category': error_category})
                except SQLExecutionError as se:
                    error_message = str(se)
                    error_category = "SQL_EXECUTION_ERROR"
                    logger.error(f"Metric {metric_id} SQL execution failed: {error_message}")
                    failed_metrics.append(
                        {'metric_record': record, 'error_message': error_message, 'error_category': error_category})
                except BigQueryError as bqe:
                    error_message = str(bqe)
                    error_category = "BIGQUERY_ERROR"
                    logger.error(f"Metric {metric_id} BigQuery error: {error_message}")
                    failed_metrics.append(
                        {'metric_record': record, 'error_message': error_message, 'error_category': error_category})
                except Exception as e:
                    error_message = str(e)
                    error_type = type(e).__name__
                    logger.error(f"Metric {metric_id} execution failed with {error_type}: {error_message}")

                    # Categorize based on error message content
                    error_category = "SQL_EXECUTION_ERROR"
                    if "Braced constructors are not supported" in error_message:
                        error_category = "SQL_SYNTAX_ERROR"
                    elif "timeout" in error_message.lower() or "deadline" in error_message.lower():
                        error_category = "SQL_TIMEOUT_ERROR"
                    elif "not found" in error_message.lower():
                        error_category = "SQL_TABLE_NOT_FOUND_ERROR"
                    elif "permission" in error_message.lower() or "denied" in error_message.lower():
                        error_category = "PERMISSION_ERROR"
                    elif "invalid" in error_message.lower():
                        error_category = "SQL_SYNTAX_ERROR"
                    elif "schema" in error_message.lower():
                        error_category = "SCHEMA_VALIDATION_ERROR"
                    else:
                        error_category = f"{error_type}_ERROR"

                    failed_metrics.append(
                        {'metric_record': record, 'error_message': error_message, 'error_category': error_category})

            if processed_records:
                try:
                    df = self.spark.createDataFrame(processed_records, schema=table_schema)
                    aligned_df = self.align_schema_with_bq(df, target_table)
                    result_dfs[target_table] = aligned_df
                    
                except Exception as schema_validation_error:
                    error_message = str(schema_validation_error)
                    logger.error(f"Schema validation failed for {target_table}: {error_message}")
                    
                    for record in records:
                        if record in successful_metrics:
                            successful_metrics.remove(record)
                            metric_id = record['metric_id']
                            detailed_error = f"Schema validation failed for metric {metric_id}: {error_message}"
                            failed_metrics.append({
                                'metric_record': record,
                                'error_message': detailed_error,
                                'error_category': 'SCHEMA_VALIDATION_ERROR'
                            })

        logger.info(f"Processing complete: {len(successful_metrics)} success, {len(failed_metrics)} failed")
        return result_dfs, successful_metrics, failed_metrics

    # BigQuery Operations
    def get_bq_table_schema(self, table_name: str):
        """Get BigQuery table schema"""
        return self.bq_operations.get_table_schema(table_name)

    def align_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame:
        """Align Spark DataFrame with BigQuery table schema"""
        return self.bq_operations.align_dataframe_schema_with_bq(df, target_table)

    def check_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]:
        """Check which metric IDs already exist in BigQuery table for the given partition date"""
        return self.bq_operations.check_existing_metrics(metric_ids, partition_dt, target_table)

    def delete_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> None:
        """Delete existing metrics from BigQuery table for the given partition date"""
        self.bq_operations.delete_metrics(metric_ids, partition_dt, target_table)

    def write_to_bq_with_overwrite(self, df: DataFrame, target_table: str) -> Tuple[List[str], List[Dict]]:
        """Write DataFrame to BigQuery table with robust overwrite capability for existing metrics"""
        try:
            self.target_tables.add(target_table)

            metric_records = df.select('metric_id', 'partition_dt').distinct().collect()

            if not metric_records:
                logger.warning("No records to process")
                return [], []

            metric_ids = [row['metric_id'] for row in metric_records]
            self.processed_metrics.extend(metric_ids)

            # Use BigQuery operations to write with robust overwrite
            return self.bq_operations.write_metrics_with_overwrite(df, target_table)

        except BigQueryError as bqe:
            error_message = str(bqe)
            error_type = "BigQueryError"
            logger.error(f"BigQuery error writing to {target_table}: {error_message}")

            failed_metrics = []
            try:
                metric_records = df.select('metric_id').distinct().collect()
                for row in metric_records:
                    failed_metrics.append({
                        'metric_id': row['metric_id'], 
                        'error_message': f"{error_type}: {error_message}"
                    })
            except Exception as extract_error:
                logger.error(f"Failed to extract metric IDs from failed write: {str(extract_error)}")

            return [], failed_metrics
        except Exception as e:
            error_message = str(e)
            error_type = type(e).__name__
            logger.error(f"Failed to write to BigQuery with {error_type}: {error_message}")

            failed_metrics = []
            try:
                metric_records = df.select('metric_id').distinct().collect()
                for row in metric_records:
                    failed_metrics.append({
                        'metric_id': row['metric_id'], 
                        'error_message': f"{error_type}: {error_message}"
                    })
            except Exception as extract_error:
                logger.error(f"Failed to extract metric IDs from failed write: {str(extract_error)}")

            return [], failed_metrics

    # Reconciliation Operations
    def build_recon_record(self, metric_record: Dict, sql: str, run_date: str, env: str, execution_status: str,
                           partition_dt: str, error_message: Optional[str] = None,
                           error_category: Optional[str] = None) -> Dict:
        """Build a reconciliation record for a metric with comprehensive None value protection"""
        try:
            # Validate and sanitize required parameters to prevent None values
            if metric_record is None:
                raise MetricsPipelineError("metric_record cannot be None")

            # Sanitize required parameters with safe defaults
            safe_run_date = run_date or DateUtils.get_current_partition_dt()
            safe_env = env or 'UNKNOWN'
            safe_partition_dt = partition_dt or DateUtils.get_current_partition_dt()
            safe_sql = sql or 'SELECT 1'  # Fallback SQL
            safe_execution_status = execution_status or 'failed'

            # Validate required fields in metric_record with safe defaults
            metric_id = metric_record.get('metric_id') or 'UNKNOWN'
            metric_name = metric_record.get('metric_name') or 'UNKNOWN'
            target_table_name = metric_record.get('target_table') or 'UNKNOWN'

            # Ensure no critical fields are empty strings
            if not metric_id or metric_id.strip() == '':
                metric_id = 'UNKNOWN'
            if not metric_name or metric_name.strip() == '':
                metric_name = 'UNKNOWN'
            if not target_table_name or target_table_name.strip() == '':
                target_table_name = 'UNKNOWN'

            # Safely extract source table info
            try:
                source_dataset, source_table = SQLUtils.get_source_table_info(safe_sql)
            except Exception:
                source_dataset, source_table = 'UNKNOWN', 'UNKNOWN'

            # Safely parse target table name
            try:
                target_table_parts = target_table_name.split('.')
                target_dataset = target_table_parts[1] if len(target_table_parts) >= 2 else 'UNKNOWN'
                target_table_name_only = target_table_parts[2] if len(target_table_parts) >= 3 else 'UNKNOWN'
            except Exception:
                target_dataset, target_table_name_only = 'UNKNOWN', 'UNKNOWN'

            # Safely get timestamp and year
            try:
                current_timestamp = DateUtils.get_current_timestamp()
                current_year = current_timestamp.year
            except Exception:
                current_timestamp = datetime.now()
                current_year = current_timestamp.year

            is_success = safe_execution_status == 'success'

            if is_success:
                exclusion_reason = 'Metric data was successfully written to target table.'
            else:
                exclusion_reason = 'Metric data failed to be written to target table.'
                if error_message:
                    if error_category:
                        formatted_error = StringUtils.format_error_with_category(error_message, error_category)
                        exclusion_reason += f' {formatted_error}'
                    else:
                        clean_error = StringUtils.clean_error_message(error_message)
                        exclusion_reason += f' Error: {clean_error}'

            # Build recon record with default values and safe parsing
            default_values = ValidationConfig.get_default_recon_values()

            # Safely parse partition date
            try:
                scheduled_date = datetime.strptime(safe_partition_dt, '%Y-%m-%d').date()
            except Exception:
                scheduled_date = datetime.now().date()

            recon_record = {'module_id': str(PipelineConfig.RECON_MODULE_ID),  # Ensure string
                'module_type_nm': str(PipelineConfig.RECON_MODULE_TYPE),  # Ensure string
                'source_server_nm': str(safe_env),  # Ensure string
                'target_server_nm': str(safe_env),  # Ensure string
                'source_vl': '0', 'target_vl': '0' if is_success else '1',
                'rcncln_exact_pass_in': 'Passed' if is_success else 'Failed',
                'latest_source_parttn_dt': str(safe_run_date),  # Ensure string
                'latest_target_parttn_dt': str(safe_run_date),  # Ensure string
                'load_ts': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'), 'schdld_dt': scheduled_date,  # Date object
                'source_system_id': str(metric_id),  # Ensure string
                'schdld_yr': int(current_year),  # Ensure integer
                'Job_Name': str(metric_name)  # Ensure string
            }

            # Add optional columns with safe string conversion
            recon_record.update({'source_databs_nm': str(source_dataset or 'UNKNOWN'),
                'source_table_nm': str(source_table or 'UNKNOWN'), 'target_databs_nm': str(target_dataset or 'UNKNOWN'),
                'target_table_nm': str(target_table_name_only or 'UNKNOWN'),
                'clcltn_ds': 'Success' if is_success else 'Failed', 'excldd_vl': '0' if is_success else '1',
                'excldd_reason_tx': str(exclusion_reason or 'No reason provided'), **default_values})

            # Final validation - ensure no None values in required fields
            required_fields = ['module_id', 'module_type_nm', 'source_server_nm', 'target_server_nm', 'source_vl',
                'target_vl', 'rcncln_exact_pass_in', 'latest_source_parttn_dt', 'latest_target_parttn_dt', 'load_ts',
                'schdld_dt', 'source_system_id', 'schdld_yr', 'Job_Name']

            for field in required_fields:
                if recon_record.get(field) is None:
                    logger.error(
                        f"Critical error: Required field '{field}' is None in recon record for metric {metric_id}")
                    # Set safe default based on field type
                    if field == 'schdld_dt':
                        recon_record[field] = datetime.now().date()
                    elif field == 'schdld_yr':
                        recon_record[field] = datetime.now().year
                    else:
                        recon_record[field] = 'UNKNOWN'

            logger.debug(f"Built recon record for metric {metric_id}: {safe_execution_status}")
            return recon_record

        except Exception as e:
            logger.error(
                f"Failed to build recon record for metric {metric_record.get('metric_id', 'UNKNOWN') if metric_record else 'UNKNOWN'}: {str(e)}")
            raise MetricsPipelineError(f"Failed to build recon record: {str(e)}")

    def create_safe_recon_record(self, metric_id: str, metric_name: str, run_date: str, env: str, partition_dt: str,
                                 error_message: str, error_category: str = 'UNKNOWN_ERROR') -> Dict:
        """Create a safe recon record with guaranteed no None values"""
        try:
            # Use safe defaults for all parameters
            safe_metric_id = str(metric_id or 'UNKNOWN')
            safe_metric_name = str(metric_name or 'UNKNOWN')
            safe_run_date = str(run_date or DateUtils.get_current_partition_dt())
            safe_env = str(env or 'UNKNOWN')
            safe_partition_dt = str(partition_dt or DateUtils.get_current_partition_dt())
            safe_error_message = str(error_message or 'Error occurred without specific details')
            safe_error_category = str(error_category or 'UNSPECIFIED_ERROR')

            # Get safe timestamp
            try:
                safe_timestamp = DateUtils.get_current_timestamp()
            except Exception as ts_error:
                logger.error(f"Error getting timestamp: {str(ts_error)}")
                safe_timestamp = datetime.now()

            # Safely parse partition date
            try:
                scheduled_date = datetime.strptime(safe_partition_dt, '%Y-%m-%d').date()
            except Exception as date_error:
                logger.error(f"Error parsing partition date: {str(date_error)}")
                scheduled_date = datetime.now().date()

            # Create exclusion reason with proper error formatting
            try:
                clean_error = StringUtils.clean_error_message(safe_error_message)
                exclusion_reason = f'Metric processing failed. Error Category: {safe_error_category}. Details: {clean_error}'
            except Exception as format_error:
                logger.error(f"Error formatting error message: {str(format_error)}")
                exclusion_reason = f'Metric processing failed. Error: {safe_error_message}'

            # Get default values
            try:
                default_values = ValidationConfig.get_default_recon_values()
            except Exception as default_error:
                logger.error(f"Error getting default values: {str(default_error)}")
                default_values = {
                    'source_column_nm': 'NA',
                    'source_file_nm': 'NA',
                    'source_contrl_file_nm': 'NA',
                    'target_column_nm': 'NA',
                    'target_file_nm': 'NA',
                    'target_contrl_file_nm': 'NA',
                    'tolrnc_pc': 'NA',
                    'rcncln_tolrnc_pass_in': 'NA'
                }

            # Core required fields
            module_id_val = str(PipelineConfig.RECON_MODULE_ID)
            module_type_nm_val = str(PipelineConfig.RECON_MODULE_TYPE)
            source_server_nm_val = safe_env
            target_server_nm_val = safe_env
            source_vl_val = '1'
            target_vl_val = '1'
            rcncln_exact_pass_in_val = 'Failed'
            latest_source_parttn_dt_val = safe_run_date
            latest_target_parttn_dt_val = safe_run_date
            load_ts_val = safe_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            schdld_dt_val = scheduled_date
            source_system_id_val = safe_metric_id
            schdld_yr_val = int(safe_timestamp.year)
            job_name_val = safe_metric_name
            
            # Check for None values in required fields
            required_field_values = {
                'module_id': module_id_val,
                'module_type_nm': module_type_nm_val,
                'source_server_nm': source_server_nm_val,
                'target_server_nm': target_server_nm_val,
                'source_vl': source_vl_val,
                'target_vl': target_vl_val,
                'rcncln_exact_pass_in': rcncln_exact_pass_in_val,
                'latest_source_parttn_dt': latest_source_parttn_dt_val,
                'latest_target_parttn_dt': latest_target_parttn_dt_val,
                'load_ts': load_ts_val,
                'schdld_dt': schdld_dt_val,
                'source_system_id': source_system_id_val,
                'schdld_yr': schdld_yr_val,
                'Job_Name': job_name_val
            }
            
            none_fields = [field_name for field_name, field_value in required_field_values.items() if field_value is None]
            
            if none_fields:
                logger.error(f"Required fields contain None values: {none_fields}")
                raise ValueError(f"Required fields are None: {none_fields}")
            
            safe_record = {
                'module_id': module_id_val,
                'module_type_nm': module_type_nm_val,
                'source_server_nm': source_server_nm_val,
                'target_server_nm': target_server_nm_val,
                'source_vl': source_vl_val,
                'target_vl': target_vl_val,
                'rcncln_exact_pass_in': rcncln_exact_pass_in_val,
                'latest_source_parttn_dt': latest_source_parttn_dt_val,
                'latest_target_parttn_dt': latest_target_parttn_dt_val,
                'load_ts': load_ts_val,
                'schdld_dt': schdld_dt_val,
                'source_system_id': source_system_id_val,
                'schdld_yr': schdld_yr_val,
                'Job_Name': job_name_val,
                'source_databs_nm': 'UNKNOWN',
                'source_table_nm': 'UNKNOWN',
                'target_databs_nm': 'UNKNOWN',
                'target_table_nm': 'UNKNOWN',
                'clcltn_ds': 'Failed',
                'excldd_vl': '1',
                'excldd_reason_tx': str(exclusion_reason),
                **default_values
            }
            
            return safe_record

        except Exception as e:
            logger.error(f"Failed to create safe recon record: {str(e)}")
            # Return absolute minimal record
            return {'module_id': '103', 'module_type_nm': 'Metrics', 'source_server_nm': 'UNKNOWN',
                'target_server_nm': 'UNKNOWN', 'source_vl': '1', 'target_vl': '1', 'rcncln_exact_pass_in': 'Failed',
                'latest_source_parttn_dt': DateUtils.get_current_partition_dt(),
                'latest_target_parttn_dt': DateUtils.get_current_partition_dt(),
                'load_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'schdld_dt': datetime.now().date(),
                'source_system_id': 'UNKNOWN', 'schdld_yr': datetime.now().year, 'Job_Name': 'UNKNOWN',
                'source_databs_nm': 'UNKNOWN', 'source_table_nm': 'UNKNOWN', 'target_databs_nm': 'UNKNOWN',
                'target_table_nm': 'UNKNOWN', 'clcltn_ds': 'Failed', 'excldd_vl': '1',
                'excldd_reason_tx': 'Critical error in recon record creation', 'source_column_nm': 'NA',
                'source_file_nm': 'NA', 'source_contrl_file_nm': 'NA', 'target_column_nm': 'NA', 'target_file_nm': 'NA',
                'target_contrl_file_nm': 'NA', 'tolrnc_pc': 'NA', 'rcncln_tolrnc_pass_in': 'NA'}

    def create_recon_records_from_write_results(self, json_data: List[Dict], run_date: str, dependencies: List[str],
                                                partition_info_table: str, env: str,
                                                successful_writes: Dict[str, List[str]],
                                                failed_execution_metrics: List[Dict],
                                                failed_write_metrics: Dict[str, List[Dict]], partition_dt: str) -> List[
        Dict]:
        """Create recon records based on execution results and write success/failure to target tables"""
        logger.info("Creating reconciliation records")

        # Validate required parameters
        if json_data is None or run_date is None or dependencies is None or env is None or partition_dt is None:
            logger.error("Missing required parameters for recon record creation")
            return []

        filtered_data = [record for record in json_data if record['dependency'] in dependencies]
        logger.info(f"Processing {len(filtered_data)} metrics for reconciliation")

        # Create lookup dictionaries for failed metrics (using both JSON and actual metric_id)
        failed_execution_lookup = {}
        failed_execution_categories = {}
        for failed_metric in failed_execution_metrics:
            json_metric_id = failed_metric['metric_record']['metric_id']
            actual_metric_id = failed_metric['metric_record'].get('_actual_metric_id', json_metric_id)
            # Store under both IDs to handle lookups
            failed_execution_lookup[json_metric_id] = failed_metric['error_message']
            failed_execution_lookup[actual_metric_id] = failed_metric['error_message']
            failed_execution_categories[json_metric_id] = failed_metric.get('error_category', 'SQL_EXECUTION_ERROR')
            failed_execution_categories[actual_metric_id] = failed_metric.get('error_category', 'SQL_EXECUTION_ERROR')

        failed_write_lookup = {}
        for target_table, failed_metrics in failed_write_metrics.items():
            for failed_metric in failed_metrics:
                metric_id = failed_metric['metric_id']
                failed_write_lookup[metric_id] = failed_metric['error_message']

        all_recon_records = []
        success_count = 0
        failed_count = 0
        safe_record_count = 0

        for record in filtered_data:
            # Use actual metric_id from SQL if available (stored during processing), 
            # otherwise fallback to JSON metric_id
            json_metric_id = record.get('metric_id', 'UNKNOWN')
            actual_metric_id = record.get('_actual_metric_id', json_metric_id)
            metric_name = record.get('metric_name', 'UNKNOWN')
            target_table = record.get('target_table', 'UNKNOWN').strip()

            # Determine status and error message
            is_success = False
            error_message = None
            error_category = None

            # Check if metric was successfully written to target table using actual metric_id
            if target_table in successful_writes and actual_metric_id in successful_writes[target_table]:
                is_success = True
                success_count += 1
            else:
                failed_count += 1
                # Determine the specific failure reason (check both JSON and actual metric_id)
                if json_metric_id in failed_execution_lookup:
                    error_message = failed_execution_lookup[json_metric_id]
                    error_category = failed_execution_categories.get(json_metric_id, "SQL_EXECUTION_ERROR")
                elif actual_metric_id in failed_execution_lookup:
                    error_message = failed_execution_lookup[actual_metric_id]
                    error_category = failed_execution_categories.get(actual_metric_id, "SQL_EXECUTION_ERROR")
                elif json_metric_id in failed_write_lookup:
                    error_message = failed_write_lookup[json_metric_id]
                    error_category = "BIGQUERY_WRITE_ERROR"
                elif actual_metric_id in failed_write_lookup:
                    error_message = failed_write_lookup[actual_metric_id]
                    error_category = "BIGQUERY_WRITE_ERROR"
                else:
                    # Metric not found in any success or failure list - this is unexpected
                    error_message = f"Metric {actual_metric_id} not found in processing results. Target table: {target_table}. This may indicate a tracking issue in the pipeline."
                    error_category = "METRIC_TRACKING_ERROR"
                    logger.warning(error_message)

            execution_status = 'success' if is_success else 'failed'

            # Create recon record using actual metric_id
            try:
                if is_success:
                    final_sql = self.replace_sql_placeholders(record.get('sql', 'SELECT 1'), run_date,
                                                              partition_info_table)
                    # Update record with actual metric_id for recon
                    recon_metric_record = record.copy()
                    recon_metric_record['metric_id'] = actual_metric_id
                    recon_record = self.build_recon_record(recon_metric_record, final_sql, run_date, env, execution_status,
                        partition_dt, error_message, error_category)
                else:
                    # Ensure we have a valid error message
                    if not error_message:
                        error_message = f"Metric processing failed without specific error details. Category: {error_category}"
                        logger.warning(f"Missing error message for failed metric {actual_metric_id}")
                    
                    recon_record = self.create_safe_recon_record(actual_metric_id, metric_name, run_date, env, partition_dt,
                        error_message, error_category or "UNSPECIFIED_ERROR")
                    safe_record_count += 1

                # Validate the created record
                if recon_record is None:
                    logger.error(f"Recon record is None for metric {actual_metric_id}")
                    raise ValueError(f"Recon record is None for metric {actual_metric_id}")
                
                none_fields = [k for k, v in recon_record.items() if v is None]
                if none_fields:
                    logger.error(f"Recon record for metric {actual_metric_id} has None values in fields: {none_fields}")
                    raise ValueError(f"Recon record has None values in fields: {none_fields}")

                all_recon_records.append(recon_record)

            except ValidationError as ve:
                logger.error(f"Validation error creating recon record for metric {actual_metric_id}: {str(ve)}")
                try:
                    minimal_record = self.create_safe_recon_record(actual_metric_id, metric_name, run_date, env, partition_dt,
                        f"Recon validation failed: {str(ve)}", "RECON_VALIDATION_ERROR")
                    all_recon_records.append(minimal_record)
                    safe_record_count += 1
                except Exception as minimal_error:
                    logger.error(f"Failed to create minimal recon record for metric {actual_metric_id}: {str(minimal_error)}")
            except MetricsPipelineError as mpe:
                logger.error(f"Pipeline error creating recon record for metric {actual_metric_id}: {str(mpe)}")
                try:
                    minimal_record = self.create_safe_recon_record(actual_metric_id, metric_name, run_date, env, partition_dt,
                        f"Recon pipeline error: {str(mpe)}", "RECON_PIPELINE_ERROR")
                    all_recon_records.append(minimal_record)
                    safe_record_count += 1
                except Exception as minimal_error:
                    logger.error(f"Failed to create minimal recon record for metric {actual_metric_id}: {str(minimal_error)}")
            except Exception as recon_error:
                error_type = type(recon_error).__name__
                logger.error(f"Failed to create recon record for metric {actual_metric_id} with {error_type}: {str(recon_error)}")
                # Last resort: create minimal record
                try:
                    minimal_record = self.create_safe_recon_record(actual_metric_id, metric_name, run_date, env, partition_dt,
                        f"Recon creation failed ({error_type}): {str(recon_error)}", f"RECON_{error_type.upper()}")
                    
                    if minimal_record is None:
                        logger.error(f"Minimal recon record is None for metric {metric_id}")
                        raise ValueError(f"Minimal recon record is None for metric {metric_id}")
                    
                    none_fields = [k for k, v in minimal_record.items() if v is None]
                    if none_fields:
                        logger.error(f"Minimal recon record has None values: {none_fields}")
                        raise ValueError(f"Minimal recon record has None values: {none_fields}")
                    
                    all_recon_records.append(minimal_record)
                    safe_record_count += 1
                except Exception as minimal_error:
                    logger.error(f"Failed to create minimal recon record for metric {metric_id}: {str(minimal_error)}")

        logger.info(f"Reconciliation complete: {len(all_recon_records)} records created ({success_count} passed, {failed_count} failed)")

        if len(all_recon_records) != len(filtered_data):
            missing_count = len(filtered_data) - len(all_recon_records)
            logger.error(f"Warning: {missing_count} metrics have no recon records")

        return all_recon_records

    def write_recon_to_bq(self, recon_records: List[Dict], recon_table: str) -> None:
        """Write reconciliation records to BigQuery recon table"""
        self.bq_operations.write_recon_records(recon_records, recon_table)

    def create_pipeline_failure_recon_records(self, json_data: List[Dict], run_date: str, dependencies: List[str],
                                              env: str, error_message: str, error_category: str = "PIPELINE_ERROR") -> \
    List[Dict]:
        """Create recon records for pipeline-level failures"""
        filtered_data = [record for record in json_data if record.get('dependency') in dependencies]
        partition_dt = DateUtils.get_current_partition_dt()
        recon_records = []

        for record in filtered_data:
            metric_id = record.get('metric_id', 'UNKNOWN')
            metric_name = record.get('metric_name', 'UNKNOWN')

            try:
                safe_record = self.create_safe_recon_record(metric_id, metric_name, run_date, env, partition_dt,
                    error_message, error_category)
                recon_records.append(safe_record)
            except Exception as e:
                logger.error(f"Failed to create pipeline failure recon record for {metric_id}: {str(e)}")

        logger.info(f"Created {len(recon_records)} pipeline failure recon records")
        return recon_records
