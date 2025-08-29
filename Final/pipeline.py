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
from utils import (
    DateUtils, NumericUtils, SQLUtils, ValidationUtils, 
    StringUtils, ExecutionUtils
)
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
            self.bq_operations.validate_partition_info_table(partition_info_table)  # Check table exists and has required structure
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
                raise GCSError(f"Invalid JSON structure in {validated_path}. Expected a JSON array of objects, but received a malformed object like '{{}}'.")

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
                                f"Could not determine partition_dt for table {dataset}.{table_name}"
                            )
                        
                        logger.info(f"Replacing {{partition_info}} placeholder with partition_dt: {replacement_date} for table {dataset}.{table_name}")
                    else:
                        raise SQLExecutionError(
                            f"Could not find table reference for {{partition_info}} placeholder at position {start_pos}"
                        )
                
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
        
        logger.info("="*80)
        logger.info("FINAL SQL QUERY (ready for BigQuery console):")
        logger.info("="*80)
        logger.info(final_sql)
        logger.info("="*80)
        logger.info("Copy the above query to run directly in BigQuery console for debugging")
        logger.info("="*80)
        
        return self.bq_operations.execute_sql_with_results(final_sql, metric_id)
    
    # Metrics Processing
    def check_dependencies_exist(self, json_data: List[Dict], dependencies: List[str]) -> None:
        """Check if all specified dependencies exist in the JSON data"""
        available_dependencies = set(record['dependency'] for record in json_data)
        missing_dependencies = set(dependencies) - available_dependencies
        
        if missing_dependencies:
            raise ValidationError(
                f"Missing dependencies in JSON data: {missing_dependencies}. "
                f"Available dependencies: {available_dependencies}"
            )
        
        logger.info(f"All dependencies found: {dependencies}")
    
    def process_metrics(self, json_data: List[Dict], run_date: str, 
                       dependencies: List[str], partition_info_table: str) -> Tuple[Dict[str, DataFrame], List[Dict], List[Dict]]:
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
        filtered_data = [
            record for record in json_data 
            if record['dependency'] in dependencies
        ]
        
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
            logger.info("-"*50)
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
                    sql_results = self.execute_sql(
                        record['sql'], 
                        run_date, 
                        partition_info_table,
                        record['metric_id']
                    )
                    logger.debug(f"      SQL execution successful for metric: {metric_id}")
                    
                    final_record = {
                        # Use SQL results if available, otherwise fall back to JSON values
                        'metric_id': sql_results.get('metric_id', record['metric_id']),
                        'metric_name': sql_results.get('metric_name', record['metric_name']),
                        'metric_type': sql_results.get('metric_type', record['metric_type']),
                        'metric_description': sql_results.get('metric_description', record.get('metric_description')),
                        'frequency': sql_results.get('frequency', record.get('frequency')),
                        'numerator_value': NumericUtils.safe_decimal_conversion(sql_results['numerator_value']),
                        'denominator_value': NumericUtils.safe_decimal_conversion(sql_results['denominator_value']),
                        'metric_output': NumericUtils.safe_decimal_conversion(sql_results['metric_output']),
                        'business_data_date': sql_results['business_data_date'],
                        'partition_dt': partition_dt,
                        'pipeline_execution_ts': DateUtils.get_current_timestamp()
                    }
                    
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
                    
                    failed_metrics.append({
                        'metric_record': record,
                        'error_message': error_message,
                        'error_category': error_category
                    })
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
        
        logger.info("-"*50)
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
                    failed_metrics.append({
                        'metric_id': row['metric_id'],
                        'error_message': error_message
                    })
            except:
                pass
            
            return [], failed_metrics
    
    # Reconciliation Operations
    def build_recon_record(self, metric_record: Dict, sql: str, run_date: str, 
                          env: str, execution_status: str, partition_dt: str, 
                          error_message: Optional[str] = None,
                          error_category: Optional[str] = None) -> Dict:
        """Build a reconciliation record for a metric"""
        try:
            # Validate required parameters to prevent None values
            if metric_record is None:
                raise MetricsPipelineError("metric_record cannot be None")
            if run_date is None:
                raise MetricsPipelineError("run_date cannot be None")
            if env is None:
                raise MetricsPipelineError("env cannot be None")
            if partition_dt is None:
                raise MetricsPipelineError("partition_dt cannot be None")
            
            # Validate required fields in metric_record
            metric_id = metric_record.get('metric_id')
            metric_name = metric_record.get('metric_name')
            target_table_name = metric_record.get('target_table')
            
            if metric_id is None:
                raise MetricsPipelineError("metric_record['metric_id'] cannot be None")
            if metric_name is None:
                raise MetricsPipelineError("metric_record['metric_name'] cannot be None")
            if target_table_name is None:
                raise MetricsPipelineError("metric_record['target_table'] cannot be None")
            
            source_dataset, source_table = SQLUtils.get_source_table_info(sql)
            
            target_table_parts = target_table_name.split('.')
            target_dataset = target_table_parts[1] if len(target_table_parts) >= 2 else None
            target_table_name_only = target_table_parts[2] if len(target_table_parts) >= 3 else None
            
            current_timestamp = DateUtils.get_current_timestamp()
            current_year = current_timestamp.year
            
            is_success = execution_status == 'success'
            
            if is_success:
                exclusion_reason = 'Metric data was successfully written.'
            else:
                exclusion_reason = 'Metric data was failed written.'
                if error_message:
                    if error_category:
                        formatted_error = StringUtils.format_error_with_category(error_message, error_category)
                        exclusion_reason += f' {formatted_error}'
                    else:
                        clean_error = StringUtils.clean_error_message(error_message)
                        exclusion_reason += f' Error: {clean_error}'
            
            # Build recon record with default values
            default_values = ValidationConfig.get_default_recon_values()
            
            recon_record = {
                'module_id': PipelineConfig.RECON_MODULE_ID,
                'module_type_nm': PipelineConfig.RECON_MODULE_TYPE,
                'source_server_nm': env,
                'target_server_nm': env,
                'source_vl': '0',
                'target_vl': '0' if is_success else '1',
                'rcncln_exact_pass_in': 'Passed' if is_success else 'Failed',
                'latest_source_parttn_dt': run_date,
                'latest_target_parttn_dt': run_date,
                'load_ts': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'schdld_dt': datetime.strptime(partition_dt, '%Y-%m-%d').date(),
                'source_system_id': metric_id,
                'schdld_yr': current_year,
                'Job_Name': metric_name
            }
            
            # Add optional columns
            recon_record.update({
                'source_databs_nm': source_dataset or 'UNKNOWN',
                'source_table_nm': source_table or 'UNKNOWN',
                'target_databs_nm': target_dataset or 'UNKNOWN',
                'target_table_nm': target_table_name_only or 'UNKNOWN',
                'clcltn_ds': 'Success' if is_success else 'Failed',
                'excldd_vl': '0' if is_success else '1',
                'excldd_reason_tx': exclusion_reason,
                **default_values
            })
            
            logger.debug(f"Built recon record for metric {metric_id}: {execution_status}")
            return recon_record
            
        except Exception as e:
            logger.error(f"Failed to build recon record for metric {metric_record.get('metric_id', 'UNKNOWN') if metric_record else 'UNKNOWN'}: {str(e)}")
            raise MetricsPipelineError(f"Failed to build recon record: {str(e)}")
    
    def build_fallback_recon_record(self, metric_record: Dict, run_date: str, 
                                   env: str, partition_dt: str, error_message: str) -> Dict:
        """Build a fallback reconciliation record when normal recon record creation fails"""
        try:
            # Validate required parameters to prevent None values
            if run_date is None:
                run_date = DateUtils.get_current_partition_dt()
            if env is None:
                env = 'UNKNOWN'
            if partition_dt is None:
                partition_dt = DateUtils.get_current_partition_dt()
            if error_message is None:
                error_message = 'Unknown error occurred'
            
            current_timestamp = DateUtils.get_current_timestamp()
            current_year = current_timestamp.year
            
            formatted_error = StringUtils.format_error_with_category(error_message, 'RECON_CREATION_ERROR')
            exclusion_reason = f'Failed to create recon record: {formatted_error}'
            
            # Build minimal recon record with default values
            default_values = ValidationConfig.get_default_recon_values()
            
            # Safely extract metric information
            metric_id = 'UNKNOWN'
            metric_name = 'UNKNOWN'
            if metric_record and isinstance(metric_record, dict):
                metric_id = metric_record.get('metric_id', 'UNKNOWN') or 'UNKNOWN'
                metric_name = metric_record.get('metric_name', 'UNKNOWN') or 'UNKNOWN'
            
            fallback_record = {
                'module_id': PipelineConfig.RECON_MODULE_ID,
                'module_type_nm': PipelineConfig.RECON_MODULE_TYPE,
                'source_server_nm': env,
                'target_server_nm': env,
                'source_vl': '1',  # Error count
                'target_vl': '1',  # Error count
                'rcncln_exact_pass_in': 'Failed',
                'latest_source_parttn_dt': run_date,
                'latest_target_parttn_dt': run_date,
                'load_ts': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'schdld_dt': datetime.strptime(partition_dt, '%Y-%m-%d').date(),
                'source_system_id': metric_id,
                'schdld_yr': current_year,
                'Job_Name': metric_name,
                'source_databs_nm': 'UNKNOWN',
                'source_table_nm': 'UNKNOWN',
                'target_databs_nm': 'UNKNOWN',
                'target_table_nm': 'UNKNOWN',
                'clcltn_ds': 'Failed',
                'excldd_vl': '1',
                'excldd_reason_tx': exclusion_reason,
                **default_values
            }
            
            logger.debug(f"Built fallback recon record for metric {metric_id}")
            return fallback_record
            
        except Exception as e:
            logger.error(f"Failed to build fallback recon record: {str(e)}")
            # Return absolute minimal record with safe defaults
            safe_run_date = run_date or DateUtils.get_current_partition_dt()
            safe_env = env or 'UNKNOWN'
            safe_partition_dt = partition_dt or DateUtils.get_current_partition_dt()
            safe_timestamp = DateUtils.get_current_timestamp()
            
            return {
                'module_id': PipelineConfig.RECON_MODULE_ID,
                'module_type_nm': PipelineConfig.RECON_MODULE_TYPE,
                'source_server_nm': safe_env,
                'target_server_nm': safe_env,
                'source_vl': '1',
                'target_vl': '1',
                'rcncln_exact_pass_in': 'Failed',
                'latest_source_parttn_dt': safe_run_date,
                'latest_target_parttn_dt': safe_run_date,
                'load_ts': safe_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'schdld_dt': datetime.strptime(safe_partition_dt, '%Y-%m-%d').date(),
                'source_system_id': 'UNKNOWN',
                'schdld_yr': safe_timestamp.year,
                'Job_Name': 'UNKNOWN',
                'source_databs_nm': 'UNKNOWN',
                'source_table_nm': 'UNKNOWN',
                'target_databs_nm': 'UNKNOWN',
                'target_table_nm': 'UNKNOWN',
                'clcltn_ds': 'Failed',
                'excldd_vl': '1',
                'excldd_reason_tx': f'Critical error in recon record creation: {StringUtils.clean_error_message(str(e))}',
                'source_column_nm': 'NA',
                'source_file_nm': 'NA',
                'source_contrl_file_nm': 'NA',
                'target_column_nm': 'NA',
                'target_file_nm': 'NA',
                'target_contrl_file_nm': 'NA',
                'tolrnc_pc': 'NA',
                'rcncln_tolrnc_pass_in': 'NA'
            }
    
    def write_recon_to_bq(self, recon_records: List[Dict], recon_table: str) -> None:
        """Write reconciliation records to BigQuery recon table"""
        self.bq_operations.write_recon_records(recon_records, recon_table)
    
    def create_pipeline_failure_recon_records(self, json_data: List[Dict], run_date: str, 
                                            dependencies: List[str], env: str, 
                                            error_message: str, error_category: str = "PIPELINE_ERROR") -> List[Dict]:
        """Create recon records for pipeline-level failures"""
        logger.info("Creating recon records for pipeline failure")
        
        partition_dt = DateUtils.get_current_partition_dt()
        
        filtered_data = [
            record for record in json_data 
            if record['dependency'] in dependencies
        ]
        
        recon_records = []
        
        for record in filtered_data:
            try:
                fallback_record = self.build_fallback_recon_record(
                    record, run_date, env, partition_dt, 
                    f"[{error_category}] Pipeline failed: {error_message}"
                )
                recon_records.append(fallback_record)
                
            except Exception as recon_error:
                logger.error(f"Failed to create pipeline failure recon record for metric {record.get('metric_id', 'UNKNOWN')}: {str(recon_error)}")
        
        logger.info(f"Created {len(recon_records)} pipeline failure recon records")
        return recon_records
    
    def create_recon_records_from_write_results(self, json_data: List[Dict], run_date: str, 
                                              dependencies: List[str], partition_info_table: str,
                                              env: str, successful_writes: Dict[str, List[str]],
                                              failed_execution_metrics: List[Dict], 
                                              failed_write_metrics: Dict[str, List[Dict]],
                                              partition_dt: str) -> List[Dict]:
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
        filtered_data = [
            record for record in json_data 
            if record['dependency'] in dependencies
        ]
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
        fallback_count = 0
        minimal_count = 0
        
        logger.info("Processing individual metrics for recon record creation...")
        
        for i, record in enumerate(filtered_data, 1):
            metric_id = record['metric_id']
            target_table = record['target_table'].strip()
            
            logger.debug(f"   [{i}/{len(filtered_data)}] Processing metric: {metric_id}")
            
            # Determine status and error message with enhanced error categorization
            is_success = False
            error_message = None
            error_category = None
            
            if target_table in successful_writes and metric_id in successful_writes[target_table]:
                is_success = True
                success_count += 1
                logger.debug(f"      Metric {metric_id} was successfully written")
            else:
                failed_count += 1
                if metric_id in failed_execution_lookup:
                    error_message = failed_execution_lookup[metric_id]
                    error_category = failed_execution_categories.get(metric_id, "SQL_EXECUTION_ERROR")
                    logger.debug(f"      Metric {metric_id} failed during execution: {error_category}")
                elif metric_id in failed_write_lookup:
                    error_message = failed_write_lookup[metric_id]
                    error_category = "BIGQUERY_WRITE_ERROR"
                    logger.debug(f"      Metric {metric_id} failed during write: {error_category}")
                else:
                    error_message = "Unknown failure occurred during processing"
                    error_category = "UNKNOWN_ERROR"
                    logger.debug(f"      Metric {metric_id} failed with unknown error")
            
            execution_status = 'success' if is_success else 'failed'
            
            # Log the error details for debugging
            if not is_success:
                logger.info(f"   Creating recon record for failed metric {metric_id}:")
                logger.info(f"      Error category: {error_category}")
                logger.info(f"      Error message: {error_message[:100]}{'...' if len(error_message) > 100 else ''}")
            
            try:
                final_sql = self.replace_sql_placeholders(record['sql'], run_date, partition_info_table)
                
                recon_record = self.build_recon_record(
                    record, 
                    final_sql, 
                    run_date, 
                    env, 
                    execution_status,
                    partition_dt,
                    error_message,
                    error_category
                )
                all_recon_records.append(recon_record)
                
                logger.debug(f"      Created standard recon record for metric {metric_id}: {execution_status}")
                if not is_success:
                    logger.info(f"      Successfully preserved original error in recon record: {error_category}")
                
            except Exception as recon_error:
                # Even if recon record creation fails, create a fallback record
                logger.error(f"      Failed to create standard recon record for metric {metric_id}: {str(recon_error)}")
                try:
                    # Preserve the original error message (SQL error) instead of the recon creation error
                    original_error_message = error_message or "Unknown failure occurred during processing"
                    original_error_category = error_category or "UNKNOWN_ERROR"
                    
                    logger.info(f"      Creating fallback recon record with original error: {original_error_category}")
                    # Create fallback record with original error, not recon creation error
                    fallback_record = self.build_fallback_recon_record_with_original_error(
                        record, run_date, env, partition_dt, 
                        original_error_message, original_error_category
                    )
                    all_recon_records.append(fallback_record)
                    fallback_count += 1
                    logger.info(f"      Created fallback recon record for metric {metric_id} with original error: {original_error_category}")
                except Exception as fallback_error:
                    logger.error(f"      Failed to create fallback recon record for metric {metric_id}: {str(fallback_error)}")
                    # Last resort: create minimal record with original error
                    try:
                        logger.info(f"      Creating minimal recon record as last resort...")
                        minimal_record = self.create_minimal_recon_record(
                            metric_id, run_date, env, partition_dt, 
                            error_message or "Unknown failure occurred during processing"
                        )
                        all_recon_records.append(minimal_record)
                        minimal_count += 1
                        logger.info(f"      Created minimal recon record for metric {metric_id}")
                    except Exception as minimal_error:
                        logger.error(f"      Failed to create minimal recon record for metric {metric_id}: {str(minimal_error)}")
                        logger.error(f"      CRITICAL: No recon record could be created for metric {metric_id}")
        
        logger.info("RECON RECORD CREATION SUMMARY:")
        logger.info(f"   Total metrics processed: {len(filtered_data)}")
        logger.info(f"   Successful metrics: {success_count}")
        logger.info(f"   Failed metrics: {failed_count}")
        logger.info(f"   Total recon records created: {len(all_recon_records)}")
        logger.info(f"   Fallback records created: {fallback_count}")
        logger.info(f"   Minimal records created: {minimal_count}")
        
        if len(all_recon_records) != len(filtered_data):
            missing_count = len(filtered_data) - len(all_recon_records)
            logger.error(f"   WARNING: {missing_count} metrics have no recon records!")
        
        logger.info("Recon record creation completed")
        return all_recon_records
    
    def build_fallback_recon_record_with_original_error(self, metric_record: Dict, run_date: str, 
                                                       env: str, partition_dt: str, 
                                                       original_error_message: str, 
                                                       original_error_category: str) -> Dict:
        """Build a fallback reconciliation record preserving the original error (e.g., SQL error)"""
        try:
            # Validate required parameters to prevent None values
            if run_date is None:
                run_date = DateUtils.get_current_partition_dt()
            if env is None:
                env = 'UNKNOWN'
            if partition_dt is None:
                partition_dt = DateUtils.get_current_partition_dt()
            if original_error_message is None:
                original_error_message = 'Unknown error occurred'
            
            current_timestamp = DateUtils.get_current_timestamp()
            current_year = current_timestamp.year
            
            # Format the original error with its category (e.g., SQL_EXECUTION_ERROR)
            formatted_error = StringUtils.format_error_with_category(original_error_message, original_error_category)
            exclusion_reason = f'Metric processing failed: {formatted_error}'
            
            # Build minimal recon record with default values
            default_values = ValidationConfig.get_default_recon_values()
            
            # Safely extract metric information
            metric_id = 'UNKNOWN'
            metric_name = 'UNKNOWN'
            if metric_record and isinstance(metric_record, dict):
                metric_id = metric_record.get('metric_id', 'UNKNOWN') or 'UNKNOWN'
                metric_name = metric_record.get('metric_name', 'UNKNOWN') or 'UNKNOWN'
            
            fallback_record = {
                'module_id': PipelineConfig.RECON_MODULE_ID,
                'module_type_nm': PipelineConfig.RECON_MODULE_TYPE,
                'source_server_nm': env,
                'target_server_nm': env,
                'source_vl': '1',  # Error count
                'target_vl': '1',  # Error count
                'rcncln_exact_pass_in': 'Failed',
                'latest_source_parttn_dt': run_date,
                'latest_target_parttn_dt': run_date,
                'load_ts': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'schdld_dt': datetime.strptime(partition_dt, '%Y-%m-%d').date(),
                'source_system_id': metric_id,
                'schdld_yr': current_year,
                'Job_Name': metric_name,
                'source_databs_nm': 'UNKNOWN',
                'source_table_nm': 'UNKNOWN',
                'target_databs_nm': 'UNKNOWN',
                'target_table_nm': 'UNKNOWN',
                'clcltn_ds': 'Failed',
                'excldd_vl': '1',
                'excldd_reason_tx': exclusion_reason,
                **default_values
            }
            
            logger.debug(f"Built fallback recon record for metric {metric_id} with original error: {original_error_category}")
            return fallback_record
            
        except Exception as e:
            logger.error(f"Failed to build fallback recon record with original error: {str(e)}")
            # Fall back to minimal record creation
            raise e
    
    def create_minimal_recon_record(self, metric_id: str, run_date: str, env: str, 
                                   partition_dt: str, error_message: str) -> Dict:
        """Create a minimal recon record as last resort"""
        try:
            # Use safe defaults for all parameters
            safe_metric_id = metric_id or 'UNKNOWN'
            safe_run_date = run_date or DateUtils.get_current_partition_dt()
            safe_env = env or 'UNKNOWN'
            safe_partition_dt = partition_dt or DateUtils.get_current_partition_dt()
            safe_error_message = error_message or 'Unknown error occurred'
            safe_timestamp = DateUtils.get_current_timestamp()
            
            # Create minimal exclusion reason
            clean_error = StringUtils.clean_error_message(safe_error_message)
            exclusion_reason = f'Metric processing failed: {clean_error}'
            
            minimal_record = {
                'module_id': PipelineConfig.RECON_MODULE_ID,
                'module_type_nm': PipelineConfig.RECON_MODULE_TYPE,
                'source_server_nm': safe_env,
                'target_server_nm': safe_env,
                'source_vl': '1',
                'target_vl': '1',
                'rcncln_exact_pass_in': 'Failed',
                'latest_source_parttn_dt': safe_run_date,
                'latest_target_parttn_dt': safe_run_date,
                'load_ts': safe_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'schdld_dt': datetime.strptime(safe_partition_dt, '%Y-%m-%d').date(),
                'source_system_id': safe_metric_id,
                'schdld_yr': safe_timestamp.year,
                'Job_Name': 'UNKNOWN',
                'source_databs_nm': 'UNKNOWN',
                'source_table_nm': 'UNKNOWN',
                'target_databs_nm': 'UNKNOWN',
                'target_table_nm': 'UNKNOWN',
                'clcltn_ds': 'Failed',
                'excldd_vl': '1',
                'excldd_reason_tx': exclusion_reason,
                'source_column_nm': 'NA',
                'source_file_nm': 'NA',
                'source_contrl_file_nm': 'NA',
                'target_column_nm': 'NA',
                'target_file_nm': 'NA',
                'target_contrl_file_nm': 'NA',
                'tolrnc_pc': 'NA',
                'rcncln_tolrnc_pass_in': 'NA'
            }
            
            logger.debug(f"Created minimal recon record for metric {safe_metric_id}")
            return minimal_record
            
        except Exception as e:
            logger.error(f"Failed to create minimal recon record: {str(e)}")
            raise MetricsPipelineError(f"Failed to create minimal recon record: {str(e)}")