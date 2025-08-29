"""
Core pipeline logic for the Metrics Pipeline
Contains the main MetricsPipeline class with all business logic
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame

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

    # Validation Operations
    def validate_partition_info_table(self, partition_info_table: str) -> None:
        """
        Validate partition info table exists and has required structure
        
        Args:
            partition_info_table: Full table name (project.dataset.table)
            
        Raises:
            MetricsPipelineError: If validation fails
        """
        try:
            logger.info("Step 0: Validating partition info table before processing metrics")
            self.bq_operations.validate_partition_info_table(
                partition_info_table)  # Check table exists and has required structure
            logger.info("Partition info table validation completed successfully")
        except Exception as e:
            error_msg = f"Partition info table validation failed: {str(e)}"
            logger.error(error_msg)
            raise MetricsPipelineError(error_msg)  # Convert to pipeline error

    # GCS Operations
    def validate_gcs_path(self, gcs_path: str) -> str:
        """Validate GCS path format and accessibility"""
        if not gcs_path.startswith('gs://'):  # Check GCS path format
            raise GCSError(f"Invalid GCS path format: {gcs_path}. Must start with 'gs://'")

        path_parts = gcs_path.replace('gs://', '').split('/')  # Parse path components
        if len(path_parts) < 2:  # Need at least bucket and file
            raise GCSError(f"Invalid GCS path structure: {gcs_path}")

        try:
            test_df = self.spark.read.option("multiline", "true").json(gcs_path).limit(0)
            test_df.count()
            logger.info(f"GCS path validated successfully: {gcs_path}")
            return gcs_path
        except Exception as e:
            raise GCSError(f"GCS path inaccessible: {gcs_path}. Error: {str(e)}")

    def read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """Read JSON file from GCS and return as list of dictionaries"""
        try:
            validated_path = self.validate_gcs_path(gcs_path)
            logger.info(f"Reading JSON from GCS: {validated_path}")
            df = self.spark.read.option("multiline", "true").json(validated_path)

            if df.count() == 0:
                raise GCSError(f"No data found in JSON file: {validated_path}. The file might be an empty array [].")

            if len(df.columns) == 0:
                raise GCSError(
                    f"Invalid JSON structure in {validated_path}. Expected a JSON array of objects, but received a malformed object like '{{}}'.")

            json_data = [row.asDict() for row in df.collect()]
            logger.info(f"Successfully read {len(json_data)} records from JSON")
            return json_data

        except Exception as e:
            logger.error(f"Failed to read JSON from GCS: {str(e)}")
            raise GCSError(f"Failed to read JSON from GCS: {str(e)}")

    # Data Validation
    def validate_json(self, json_data: List[Dict]) -> List[Dict]:
        """Validate JSON data for required fields and duplicates"""
        logger.info("Validating JSON data")
        metric_ids = set()

        for i, record in enumerate(json_data):
            ValidationUtils.validate_json_record(record, i, metric_ids)

        logger.info(f"Successfully validated {len(json_data)} records with {len(metric_ids)} unique metric IDs")
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
                logger.info("No placeholders found in SQL query")
                return sql

            logger.info(f"Found {len(placeholders)} placeholders in SQL: {[p[0] for p in placeholders]}")

            final_sql = sql

            for placeholder_type, start_pos, end_pos in reversed(placeholders):
                if placeholder_type == 'currently':
                    replacement_date = run_date
                    logger.info(f"Replacing {{currently}} placeholder with run_date: {replacement_date}")

                elif placeholder_type == 'partition_info':
                    table_info = SQLUtils.get_table_for_placeholder(sql, start_pos)

                    if table_info:
                        dataset, table_name = table_info
                        replacement_date = self.get_partition_dt(dataset, table_name, partition_info_table)

                        if not replacement_date:
                            raise SQLExecutionError(
                                f"Could not determine partition_dt for table {dataset}.{table_name}")

                        logger.info(
                            f"Replacing {{partition_info}} placeholder with partition_dt: {replacement_date} for table {dataset}.{table_name}")
                    else:
                        raise SQLExecutionError(
                            f"Could not find table reference for {{partition_info}} placeholder at position {start_pos}")

                final_sql = final_sql[:start_pos] + f"'{replacement_date}'" + final_sql[end_pos:]

            logger.info(f"Successfully replaced {len(placeholders)} placeholders in SQL")
            logger.debug(f"Final SQL after placeholder replacement: {final_sql}")

            return final_sql

        except Exception as e:
            logger.error(f"Failed to replace SQL placeholders: {str(e)}")
            raise SQLExecutionError(f"Failed to replace SQL placeholders: {str(e)}")

    def execute_sql(self, sql: str, run_date: str, partition_info_table: str, metric_id: Optional[str] = None) -> Dict:
        """Execute SQL query with dynamic placeholder replacement"""
        logger.info(f"Executing SQL for metric {metric_id or 'UNKNOWN'}:")
        logger.info(f"Original SQL: {sql}")

        final_sql = self.replace_sql_placeholders(sql, run_date, partition_info_table)

        logger.info("=" * 80)
        logger.info("FINAL SQL QUERY (ready for BigQuery console):")
        logger.info("=" * 80)
        logger.info(final_sql)
        logger.info("=" * 80)
        logger.info("Copy the above query to run directly in BigQuery console for debugging")
        logger.info("=" * 80)

        return self.bq_operations.execute_sql_with_results(final_sql, metric_id)

    # Metrics Processing
    def check_dependencies_exist(self, json_data: List[Dict], dependencies: List[str]) -> None:
        """Check if all specified dependencies exist in the JSON data"""
        available_dependencies = set(record['dependency'] for record in json_data)
        missing_dependencies = set(dependencies) - available_dependencies

        if missing_dependencies:
            raise ValidationError(f"Missing dependencies in JSON data: {missing_dependencies}. "
                                  f"Available dependencies: {available_dependencies}")

        logger.info(f"All dependencies found: {dependencies}")

    def process_metrics(self, json_data: List[Dict], run_date: str, dependencies: List[str],
                        partition_info_table: str) -> Tuple[Dict[str, DataFrame], List[Dict], List[Dict]]:
        """Process metrics and create Spark DataFrames grouped by target_table"""
        logger.info("Starting metrics processing...")
        logger.info(f"Target dependencies: {dependencies}")
        logger.info(f"Run date: {run_date}")
        logger.info(f"Partition info table: {partition_info_table}")

        logger.info("Checking if all dependencies exist in JSON data...")
        self.check_dependencies_exist(json_data, dependencies)
        logger.info("All dependencies found in JSON data")

        partition_dt = DateUtils.get_current_partition_dt()
        logger.info(f"Using pipeline run date as partition_dt: {partition_dt}")

        logger.info("Filtering data by dependencies...")
        filtered_data = [record for record in json_data if record['dependency'] in dependencies]

        if not filtered_data:
            logger.error(f"No records found for dependencies: {dependencies}")
            raise ValidationError(f"No records found for dependencies: {dependencies}")

        logger.info(f"Found {len(filtered_data)} records to process after filtering")

        # Group records by target_table
        logger.info("Grouping records by target table...")
        records_by_table = {}
        for record in filtered_data:
            target_table = record['target_table'].strip()
            if target_table not in records_by_table:
                records_by_table[target_table] = []
            records_by_table[target_table].append(record)

        logger.info(f"Records grouped into {len(records_by_table)} target tables:")
        for table, records in records_by_table.items():
            logger.info(f"   {table}: {len(records)} metrics")

        # Process each group and create DataFrames
        logger.info("Starting metric processing by target table...")
        result_dfs = {}
        successful_metrics = []
        failed_metrics = []

        for target_table, records in records_by_table.items():
            logger.info("-" * 50)
            logger.info(f"Processing target table: {target_table}")
            logger.info(f"Number of metrics to process: {len(records)}")

            processed_records = []
            table_successful = 0
            table_failed = 0

            for i, record in enumerate(records, 1):
                metric_id = record['metric_id']
                logger.info(f"   [{i}/{len(records)}] Processing metric: {metric_id}")

                try:
                    logger.debug(f"      Executing SQL for metric: {metric_id}")
                    sql_results = self.execute_sql(record['sql'], run_date, partition_info_table, record['metric_id'])
                    logger.debug(f"      SQL execution successful for metric: {metric_id}")

                    final_record = {# Use SQL results if available, otherwise fall back to JSON values
                        'metric_id': sql_results.get('metric_id', record['metric_id']),
                        'metric_name': sql_results.get('metric_name', record['metric_name']),
                        'metric_type': sql_results.get('metric_type', record['metric_type']),
                        'metric_description': sql_results.get('metric_description', record.get('metric_description')),
                        'frequency': sql_results.get('frequency', record.get('frequency')),
                        'numerator_value': NumericUtils.safe_decimal_conversion(sql_results['numerator_value']),
                        'denominator_value': NumericUtils.safe_decimal_conversion(sql_results['denominator_value']),
                        'metric_output': NumericUtils.safe_decimal_conversion(sql_results['metric_output']),
                        'business_data_date': sql_results['business_data_date'], 'partition_dt': partition_dt,
                        'pipeline_execution_ts': DateUtils.get_current_timestamp()}

                    processed_records.append(final_record)
                    successful_metrics.append(record)
                    table_successful += 1
                    logger.info(f"      [{i}/{len(records)}] Successfully processed metric: {metric_id}")

                except Exception as e:
                    error_message = str(e)
                    table_failed += 1
                    logger.error(f"      [{i}/{len(records)}] Failed to process metric: {metric_id}")
                    logger.error(f"         Error: {error_message}")

                    # Categorize the error type for better tracking
                    error_category = "SQL_EXECUTION_ERROR"
                    if "Braced constructors are not supported" in error_message:
                        error_category = "SQL_SYNTAX_ERROR"
                        logger.error(f"         Error type: SQL Syntax Error - Braced constructors not supported")
                    elif "timeout" in error_message.lower():
                        error_category = "SQL_TIMEOUT_ERROR"
                        logger.error(f"         Error type: SQL Timeout Error")
                    elif "not found" in error_message.lower():
                        error_category = "SQL_TABLE_NOT_FOUND_ERROR"
                        logger.error(f"         Error type: SQL Table Not Found Error")
                    else:
                        logger.error(f"         Error type: General SQL Execution Error")

                    failed_metrics.append(
                        {'metric_record': record, 'error_message': error_message, 'error_category': error_category})
                    continue

            # Create Spark DataFrame for this target table if we have successful records
            if processed_records:
                logger.info(f"   Creating DataFrame for {target_table} with {len(processed_records)} records...")
                df = self.spark.createDataFrame(processed_records, PipelineConfig.METRICS_SCHEMA)
                result_dfs[target_table] = df
                logger.info(f"   Successfully created DataFrame for {target_table}")
            else:
                logger.warning(f"   No records processed successfully for target table: {target_table}")

            logger.info(f"   Table {target_table} summary: {table_successful} successful, {table_failed} failed")

        logger.info("-" * 50)
        logger.info("METRICS PROCESSING SUMMARY:")
        logger.info(f"   Total successful metrics: {len(successful_metrics)}")
        logger.info(f"   Total failed metrics: {len(failed_metrics)}")
        logger.info(f"   Target tables with data: {len(result_dfs)}")

        if failed_metrics:
            logger.warning("FAILED METRICS DETAILS:")
            for i, fm in enumerate(failed_metrics[:10], 1):  # Show first 10 failures
                metric_id = fm['metric_record']['metric_id']
                error_category = fm.get('error_category', 'UNKNOWN')
                error_msg = fm['error_message'][:100] + "..." if len(fm['error_message']) > 100 else fm['error_message']
                logger.warning(f"   {i}. {metric_id} [{error_category}]: {error_msg}")
            if len(failed_metrics) > 10:
                logger.warning(f"   ... and {len(failed_metrics) - 10} more failed metrics")

        logger.info("Metrics processing completed")
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

        except Exception as e:
            error_message = str(e)
            logger.error(f"Failed to write to BigQuery with overwrite: {error_message}")

            failed_metrics = []
            try:
                metric_records = df.select('metric_id').distinct().collect()
                for row in metric_records:
                    failed_metrics.append({'metric_id': row['metric_id'], 'error_message': error_message})
            except:
                pass

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
            logger.info(f"CREATING SAFE RECON RECORD - Input parameters:")
            logger.info(f"   metric_id: {repr(metric_id)} (type: {type(metric_id)})")
            logger.info(f"   metric_name: {repr(metric_name)} (type: {type(metric_name)})")
            logger.info(f"   run_date: {repr(run_date)} (type: {type(run_date)})")
            logger.info(f"   env: {repr(env)} (type: {type(env)})")
            logger.info(f"   partition_dt: {repr(partition_dt)} (type: {type(partition_dt)})")
            logger.info(f"   error_message: {repr(error_message)} (type: {type(error_message)})")
            logger.info(f"   error_category: {repr(error_category)} (type: {type(error_category)})")
            
            # Use safe defaults for all parameters
            safe_metric_id = str(metric_id or 'UNKNOWN')
            safe_metric_name = str(metric_name or 'UNKNOWN')
            safe_run_date = str(run_date or DateUtils.get_current_partition_dt())
            safe_env = str(env or 'UNKNOWN')
            safe_partition_dt = str(partition_dt or DateUtils.get_current_partition_dt())
            safe_error_message = str(error_message or 'Unknown error occurred')
            safe_error_category = str(error_category or 'UNKNOWN_ERROR')

            logger.info(f"SAFE VALUES AFTER SANITIZATION:")
            logger.info(f"   safe_metric_id: {repr(safe_metric_id)} (type: {type(safe_metric_id)})")
            logger.info(f"   safe_metric_name: {repr(safe_metric_name)} (type: {type(safe_metric_name)})")
            logger.info(f"   safe_run_date: {repr(safe_run_date)} (type: {type(safe_run_date)})")
            logger.info(f"   safe_env: {repr(safe_env)} (type: {type(safe_env)})")
            logger.info(f"   safe_partition_dt: {repr(safe_partition_dt)} (type: {type(safe_partition_dt)})")
            logger.info(f"   safe_error_message: {repr(safe_error_message)} (type: {type(safe_error_message)})")
            logger.info(f"   safe_error_category: {repr(safe_error_category)} (type: {type(safe_error_category)})")

            # Get safe timestamp
            try:
                safe_timestamp = DateUtils.get_current_timestamp()
                logger.info(f"   safe_timestamp: {repr(safe_timestamp)} (type: {type(safe_timestamp)})")
            except Exception as ts_error:
                logger.error(f"   Error getting timestamp: {str(ts_error)}")
                safe_timestamp = datetime.now()
                logger.info(f"   fallback safe_timestamp: {repr(safe_timestamp)} (type: {type(safe_timestamp)})")

            # Safely parse partition date
            try:
                scheduled_date = datetime.strptime(safe_partition_dt, '%Y-%m-%d').date()
                logger.info(f"   scheduled_date: {repr(scheduled_date)} (type: {type(scheduled_date)})")
            except Exception as date_error:
                logger.error(f"   Error parsing partition date: {str(date_error)}")
                scheduled_date = datetime.now().date()
                logger.info(f"   fallback scheduled_date: {repr(scheduled_date)} (type: {type(scheduled_date)})")

            # Create minimal exclusion reason
            try:
                formatted_error = StringUtils.format_error_with_category(safe_error_message, safe_error_category)
                exclusion_reason = f'Metric processing failed: {formatted_error}'
                logger.info(f"   exclusion_reason: {repr(exclusion_reason)} (type: {type(exclusion_reason)})")
            except Exception as format_error:
                logger.error(f"   Error formatting error message: {str(format_error)}")
                exclusion_reason = f'Metric processing failed: {safe_error_message}'
                logger.info(f"   fallback exclusion_reason: {repr(exclusion_reason)} (type: {type(exclusion_reason)})")

            # Get default values
            try:
                default_values = ValidationConfig.get_default_recon_values()
                logger.info(f"   default_values: {repr(default_values)} (type: {type(default_values)})")
            except Exception as default_error:
                logger.error(f"   Error getting default values: {str(default_error)}")
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
                logger.info(f"   fallback default_values: {repr(default_values)} (type: {type(default_values)})")

            # Build the record step by step with detailed logging
            logger.info(f"BUILDING RECON RECORD FIELDS:")
            
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
            
            logger.info(f"   module_id: {repr(module_id_val)} (type: {type(module_id_val)})")
            logger.info(f"   module_type_nm: {repr(module_type_nm_val)} (type: {type(module_type_nm_val)})")
            logger.info(f"   source_server_nm: {repr(source_server_nm_val)} (type: {type(source_server_nm_val)})")
            logger.info(f"   target_server_nm: {repr(target_server_nm_val)} (type: {type(target_server_nm_val)})")
            logger.info(f"   source_vl: {repr(source_vl_val)} (type: {type(source_vl_val)})")
            logger.info(f"   target_vl: {repr(target_vl_val)} (type: {type(target_vl_val)})")
            logger.info(f"   rcncln_exact_pass_in: {repr(rcncln_exact_pass_in_val)} (type: {type(rcncln_exact_pass_in_val)})")
            logger.info(f"   latest_source_parttn_dt: {repr(latest_source_parttn_dt_val)} (type: {type(latest_source_parttn_dt_val)})")
            logger.info(f"   latest_target_parttn_dt: {repr(latest_target_parttn_dt_val)} (type: {type(latest_target_parttn_dt_val)})")
            logger.info(f"   load_ts: {repr(load_ts_val)} (type: {type(load_ts_val)})")
            logger.info(f"   schdld_dt: {repr(schdld_dt_val)} (type: {type(schdld_dt_val)})")
            logger.info(f"   source_system_id: {repr(source_system_id_val)} (type: {type(source_system_id_val)})")
            logger.info(f"   schdld_yr: {repr(schdld_yr_val)} (type: {type(schdld_yr_val)})")
            logger.info(f"   Job_Name: {repr(job_name_val)} (type: {type(job_name_val)})")
            
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
            
            logger.info(f"CHECKING FOR NONE VALUES IN REQUIRED FIELDS:")
            none_fields = []
            for field_name, field_value in required_field_values.items():
                if field_value is None:
                    logger.error(f"   CRITICAL: {field_name} is None!")
                    none_fields.append(field_name)
                else:
                    logger.info(f"   {field_name}: OK")
            
            if none_fields:
                logger.error(f"FOUND {len(none_fields)} NONE VALUES IN REQUIRED FIELDS: {none_fields}")
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

            logger.info(f"SUCCESSFULLY CREATED SAFE RECON RECORD for metric {safe_metric_id}")
            logger.info(f"FINAL RECORD SUMMARY:")
            for key, value in safe_record.items():
                logger.info(f"   {key}: {repr(value)} (type: {type(value)})")
            
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
        logger.info("Starting recon record creation from write results...")

        # Validate required parameters to prevent None values
        logger.info("Validating input parameters...")
        if json_data is None:
            logger.error("json_data cannot be None")
            return []
        if run_date is None:
            logger.error("run_date cannot be None")
            return []
        if dependencies is None:
            logger.error("dependencies cannot be None")
            return []
        if env is None:
            logger.error("env cannot be None")
            return []
        if partition_dt is None:
            logger.error("partition_dt cannot be None")
            return []

        logger.info("All input parameters validated successfully")

        logger.info("Filtering data by dependencies...")
        filtered_data = [record for record in json_data if record['dependency'] in dependencies]
        logger.info(f"Filtered to {len(filtered_data)} records for dependencies: {dependencies}")

        # Create lookup dictionaries for failed metrics and their error messages
        logger.info("Creating error lookup dictionaries...")
        failed_execution_lookup = {}
        failed_execution_categories = {}
        for failed_metric in failed_execution_metrics:
            metric_id = failed_metric['metric_record']['metric_id']
            failed_execution_lookup[metric_id] = failed_metric['error_message']
            failed_execution_categories[metric_id] = failed_metric.get('error_category', 'SQL_EXECUTION_ERROR')

        failed_write_lookup = {}
        for target_table, failed_metrics in failed_write_metrics.items():
            for failed_metric in failed_metrics:
                metric_id = failed_metric['metric_id']
                failed_write_lookup[metric_id] = failed_metric['error_message']

        logger.info(f"Error lookup summary:")
        logger.info(f"   Failed execution metrics: {len(failed_execution_lookup)}")
        logger.info(f"   Failed write metrics: {len(failed_write_lookup)}")

        all_recon_records = []
        success_count = 0
        failed_count = 0
        safe_record_count = 0

        logger.info("Processing individual metrics for recon record creation...")

        for i, record in enumerate(filtered_data, 1):
            metric_id = record.get('metric_id', 'UNKNOWN')
            metric_name = record.get('metric_name', 'UNKNOWN')
            target_table = record.get('target_table', 'UNKNOWN').strip()

            logger.debug(f"   [{i}/{len(filtered_data)}] Processing metric: {metric_id}")

            # Determine status and error message with enhanced error categorization
            is_success = False
            error_message = None
            error_category = None

            # Check if metric was successfully written to target table
            if target_table in successful_writes and metric_id in successful_writes[target_table]:
                is_success = True
                success_count += 1
                logger.debug(f"       Metric {metric_id} was successfully written to {target_table}")
            else:
                failed_count += 1
                # Determine the specific failure reason
                if metric_id in failed_execution_lookup:
                    error_message = failed_execution_lookup[metric_id]
                    error_category = failed_execution_categories.get(metric_id, "SQL_EXECUTION_ERROR")
                    logger.debug(f"       Metric {metric_id} failed during SQL execution: {error_category}")
                elif metric_id in failed_write_lookup:
                    error_message = failed_write_lookup[metric_id]
                    error_category = "BIGQUERY_WRITE_ERROR"
                    logger.debug(f"       Metric {metric_id} failed during BigQuery write: {error_category}")
                else:
                    error_message = "Unknown failure occurred during processing"
                    error_category = "UNKNOWN_ERROR"
                    logger.debug(f"       Metric {metric_id} failed with unknown error")

            execution_status = 'success' if is_success else 'failed'

            # Create recon record with comprehensive error handling
            logger.info(f"🔧 CREATING RECON RECORD FOR METRIC {metric_id} (Status: {execution_status})")
            try:
                if is_success:
                    logger.info(f"   Using standard method for successful metric {metric_id}")
                    # For successful metrics, use the standard method
                    final_sql = self.replace_sql_placeholders(record.get('sql', 'SELECT 1'), run_date,
                                                              partition_info_table)
                    recon_record = self.build_recon_record(record, final_sql, run_date, env, execution_status,
                        partition_dt, error_message, error_category)
                else:
                    logger.info(f"   Using safe method for failed metric {metric_id}")
                    # For failed metrics, use the safe method to avoid cascading errors
                    recon_record = self.create_safe_recon_record(metric_id, metric_name, run_date, env, partition_dt,
                        error_message or "Unknown failure occurred during processing",
                        error_category or "UNKNOWN_ERROR")
                    safe_record_count += 1

                # Validate the created record before adding it
                if recon_record is None:
                    logger.error(f"CRITICAL: Created recon record is None for metric {metric_id}!")
                    raise ValueError(f"Recon record is None for metric {metric_id}")
                
                # Check for None values in the created record
                none_fields = [k for k, v in recon_record.items() if v is None]
                if none_fields:
                    logger.error(f"CRITICAL: Recon record for metric {metric_id} has None values in fields: {none_fields}")
                    logger.error(f"   Full record: {recon_record}")
                    raise ValueError(f"Recon record has None values in fields: {none_fields}")

                all_recon_records.append(recon_record)
                logger.info(f"Successfully created and added recon record for metric {metric_id}: {execution_status}")

            except Exception as recon_error:
                logger.error(f"Failed to create recon record for metric {metric_id}: {str(recon_error)}")
                logger.error(f"   Error type: {type(recon_error).__name__}")
                # Last resort: create absolute minimal record
                try:
                    logger.info(f"   Attempting minimal safe recon record for metric {metric_id}")
                    minimal_record = self.create_safe_recon_record(metric_id, metric_name, run_date, env, partition_dt,
                        f"Recon creation failed: {str(recon_error)}", "RECON_CREATION_ERROR")
                    
                    # Validate minimal record too
                    if minimal_record is None:
                        logger.error(f"CRITICAL: Minimal recon record is None for metric {metric_id}!")
                        raise ValueError(f"Minimal recon record is None for metric {metric_id}")
                    
                    none_fields = [k for k, v in minimal_record.items() if v is None]
                    if none_fields:
                        logger.error(f"CRITICAL: Minimal recon record for metric {metric_id} has None values: {none_fields}")
                        raise ValueError(f"Minimal recon record has None values: {none_fields}")
                    
                    all_recon_records.append(minimal_record)
                    safe_record_count += 1
                    logger.info(f"Created minimal safe recon record for metric {metric_id}")
                except Exception as minimal_error:
                    logger.error(f"CRITICAL: Failed to create minimal recon record for metric {metric_id}: {str(minimal_error)}")
                    logger.error(f"   This metric will have NO recon record!")

        logger.info("RECON RECORD CREATION SUMMARY:")
        logger.info(f"   Total metrics processed: {len(filtered_data)}")
        logger.info(f"    Successful metrics (written to target tables): {success_count}")
        logger.info(f"    Failed metrics (not written to target tables): {failed_count}")
        logger.info(f"   Total recon records created: {len(all_recon_records)}")
        logger.info(f"   Standard recon records: {len(all_recon_records) - safe_record_count}")
        logger.info(f"   Safe/minimal records created: {safe_record_count}")

        if len(all_recon_records) != len(filtered_data):
            missing_count = len(filtered_data) - len(all_recon_records)
            logger.error(f"   WARNING: {missing_count} metrics have no recon records!")

        # Log recon record status distribution
        passed_recon_count = sum(1 for r in all_recon_records if r.get('rcncln_exact_pass_in') == 'Passed')
        failed_recon_count = sum(1 for r in all_recon_records if r.get('rcncln_exact_pass_in') == 'Failed')
        logger.info(f"   Recon records marked as 'Passed': {passed_recon_count}")
        logger.info(f"   Recon records marked as 'Failed': {failed_recon_count}")

        logger.info("Recon record creation completed")
        return all_recon_records

    def write_recon_to_bq(self, recon_records: List[Dict], recon_table: str) -> None:
        """Write reconciliation records to BigQuery recon table"""
        self.bq_operations.write_recon_records(recon_records, recon_table)

    def create_pipeline_failure_recon_records(self, json_data: List[Dict], run_date: str, dependencies: List[str],
                                              env: str, error_message: str, error_category: str = "PIPELINE_ERROR") -> \
    List[Dict]:
        """Create recon records for pipeline-level failures"""
        logger.info("Creating recon records for pipeline failure")

        # Filter data by dependencies
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
