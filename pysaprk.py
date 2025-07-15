import argparse
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any, Callable
import sys
from decimal import Decimal
import os
import tempfile
from contextlib import contextmanager
import uuid

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
from google.cloud.exceptions import NotFound, GoogleCloudError

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
    
    # Class constants
    REQUIRED_FIELDS = ['metric_id', 'metric_name', 'metric_type', 'sql', 'dependency', 'target_table']
    NUMERIC_FIELDS = ['metric_output', 'numerator_value', 'denominator_value']
    PLACEHOLDER_PATTERNS = {
        'currently': r'\{currently\}',
        'partition_info': r'\{partition_info\}'
    }
    
    def __init__(self, spark: SparkSession, bq_client: bigquery.Client):
        self.spark = spark
        self.bq_client = bq_client
        self.execution_id = str(uuid.uuid4())
        self.processed_metrics = []
        self.overwritten_metrics = []
        self.target_tables = set()
        
    def _handle_operation(self, operation: Callable, operation_name: str, *args, **kwargs) -> Any:
        """Generic error handling wrapper for operations"""
        try:
            return operation(*args, **kwargs)
        except Exception as e:
            error_msg = f"{operation_name} failed: {str(e)}"
            logger.error(error_msg)
            raise MetricsPipelineError(error_msg)
    
    def _execute_bq_query(self, query: str, operation_name: str = "BigQuery query") -> Any:
        """Execute BigQuery query with error handling"""
        def execute():
            logger.info(f"Executing {operation_name}")
            query_job = self.bq_client.query(query)
            return query_job.result()
        
        return self._handle_operation(execute, operation_name)
    
    def _validate_table_format(self, table_name: str, record_index: Optional[int] = None) -> List[str]:
        """Validate BigQuery table format and return parts"""
        if not table_name or not table_name.strip():
            raise MetricsPipelineError(f"Record {record_index}: target_table cannot be empty")
        
        table_parts = table_name.strip().split('.')
        if len(table_parts) != 3:
            raise MetricsPipelineError(
                f"Record {record_index}: target_table '{table_name}' must be in format 'project.dataset.table'"
            )
        
        part_names = ['project', 'dataset', 'table']
        for i, part in enumerate(table_parts):
            if not part.strip():
                raise MetricsPipelineError(
                    f"Record {record_index}: target_table '{table_name}' has empty {part_names[i]} part"
                )
        
        return table_parts
    
    def _validate_field_content(self, record: Dict, field: str, record_index: int) -> None:
        """Validate that a field exists and has valid content"""
        if field not in record:
            raise MetricsPipelineError(f"Record {record_index}: Missing required field '{field}'")
        
        value = record[field]
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise MetricsPipelineError(
                f"Record {record_index}: Field '{field}' is null, empty, or contains only whitespace"
            )
    
    def _normalize_numeric_value(self, value: Union[int, float, Decimal, str, None]) -> Optional[str]:
        """Normalize numeric values to string representation to preserve precision"""
        if value is None:
            return None
        
        try:
            if isinstance(value, Decimal):
                return str(value)
            elif isinstance(value, (int, float)):
                return str(Decimal(str(value)))
            elif isinstance(value, str):
                return str(Decimal(value))
            else:
                return str(Decimal(str(value)))
        except (ValueError, TypeError, OverflowError, Exception) as e:
            logger.warning(f"Could not normalize numeric value: {value}, error: {e}")
            return None
    
    def _safe_decimal_conversion(self, value: Optional[str]) -> Optional[Decimal]:
        """Safely convert string to Decimal for BigQuery"""
        if value is None:
            return None
        
        try:
            return Decimal(value)
        except (ValueError, TypeError, OverflowError):
            logger.warning(f"Could not convert to Decimal: {value}")
            return None
    
    def _find_table_references(self, sql: str) -> List[Tuple[str, str, str]]:
        """Find all table references in SQL"""
        table_pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
        return re.findall(table_pattern, sql)
    
    def _build_metric_record(self, record: Dict, sql_results: Dict, partition_dt: str) -> Dict:
        """Build final metric record with proper type conversion"""
        return {
            'metric_id': record['metric_id'],
            'metric_name': record['metric_name'],
            'metric_type': record['metric_type'],
            'numerator_value': self._safe_decimal_conversion(sql_results['numerator_value']),
            'denominator_value': self._safe_decimal_conversion(sql_results['denominator_value']),
            'metric_output': self._safe_decimal_conversion(sql_results['metric_output']),
            'business_data_date': sql_results['business_data_date'],
            'partition_dt': partition_dt,
            'pipeline_execution_ts': datetime.utcnow()
        }
    
    def _create_dataframe_schema(self) -> StructType:
        """Create standard DataFrame schema for metrics"""
        return StructType([
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
    
    def validate_gcs_path(self, gcs_path: str) -> str:
        """Validate GCS path format and accessibility"""
        if not gcs_path.startswith('gs://'):
            raise MetricsPipelineError(f"Invalid GCS path format: {gcs_path}. Must start with 'gs://'")
        
        path_parts = gcs_path.replace('gs://', '').split('/')
        if len(path_parts) < 2:
            raise MetricsPipelineError(f"Invalid GCS path structure: {gcs_path}")
        
        def test_access():
            test_df = self.spark.read.option("multiline", "true").json(gcs_path).limit(0)
            test_df.count()
            logger.info(f"GCS path validated successfully: {gcs_path}")
            return gcs_path
        
        return self._handle_operation(test_access, f"GCS path validation for {gcs_path}")
    
    def read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """Read JSON file from GCS and return as list of dictionaries"""
        def read_and_convert():
            validated_path = self.validate_gcs_path(gcs_path)
            logger.info(f"Reading JSON from GCS: {validated_path}")
            
            df = self.spark.read.option("multiline", "true").json(validated_path)
            if df.count() == 0:
                raise MetricsPipelineError(f"No data found in JSON file: {validated_path}")
            
            json_data = [row.asDict() for row in df.collect()]
            logger.info(f"Successfully read {len(json_data)} records from JSON")
            return json_data
        
        return self._handle_operation(read_and_convert, "JSON reading from GCS")
    
    def validate_json(self, json_data: List[Dict]) -> List[Dict]:
        """Validate JSON data for required fields and duplicates"""
        logger.info("Validating JSON data")
        metric_ids = set()
        
        for i, record in enumerate(json_data):
            # Validate required fields
            for field in self.REQUIRED_FIELDS:
                self._validate_field_content(record, field, i)
            
            # Check for duplicate metric IDs
            metric_id = record['metric_id'].strip()
            if metric_id in metric_ids:
                raise MetricsPipelineError(f"Record {i}: Duplicate metric_id '{metric_id}' found")
            metric_ids.add(metric_id)
            
            # Validate target table format
            self._validate_table_format(record['target_table'], i)
            
            # Validate SQL placeholders
            sql_query = record['sql'].strip()
            if sql_query:
                placeholder_counts = {
                    name: len(re.findall(pattern, sql_query))
                    for name, pattern in self.PLACEHOLDER_PATTERNS.items()
                }
                
                if sum(placeholder_counts.values()) == 0:
                    logger.warning(f"Record {i}: SQL query contains no date placeholders")
                else:
                    logger.debug(f"Record {i}: Found placeholders: {placeholder_counts}")
        
        logger.info(f"Successfully validated {len(json_data)} records with {len(metric_ids)} unique metric IDs")
        return json_data
    
    def find_placeholder_positions(self, sql: str) -> List[Tuple[str, int, int]]:
        """Find all placeholders in SQL with their positions"""
        placeholders = []
        
        for placeholder_type, pattern in self.PLACEHOLDER_PATTERNS.items():
            for match in re.finditer(pattern, sql):
                placeholders.append((placeholder_type, match.start(), match.end()))
        
        return sorted(placeholders, key=lambda x: x[1])
    
    def get_table_for_placeholder(self, sql: str, placeholder_pos: int) -> Optional[Tuple[str, str]]:
        """Find the table associated with a placeholder based on its position"""
        table_references = []
        for match in re.finditer(r'`([^.]+)\.([^.]+)\.([^`]+)`', sql):
            table_references.append((match.end(), match.groups()))
        
        # Find closest table before placeholder
        best_table = None
        best_distance = float('inf')
        
        for table_end_pos, (project, dataset, table) in table_references:
            if table_end_pos < placeholder_pos:
                distance = placeholder_pos - table_end_pos
                if distance < best_distance:
                    best_distance = distance
                    best_table = (dataset, table)
        
        return best_table
    
    def get_partition_dt(self, dataset: str, table_name: str, partition_info_table: str) -> Optional[str]:
        """Get latest partition_dt from metadata table"""
        query = f"""
        SELECT partition_dt 
        FROM `{partition_info_table}` 
        WHERE project_dataset = '{dataset}' 
        AND table_name = '{table_name}'
        ORDER BY partition_dt DESC
        LIMIT 1
        """
        
        logger.info(f"Querying partition info for {dataset}.{table_name}")
        
        try:
            results = self._execute_bq_query(query, f"partition info query for {dataset}.{table_name}")
            
            for row in results:
                partition_dt = row.partition_dt
                return partition_dt.strftime('%Y-%m-%d') if isinstance(partition_dt, datetime) else str(partition_dt)
            
            logger.warning(f"No partition info found for {dataset}.{table_name}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get partition_dt for {dataset}.{table_name}: {str(e)}")
            return None
    
    def replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str) -> str:
        """Replace placeholders in SQL with appropriate dates"""
        placeholders = self.find_placeholder_positions(sql)
        
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
                table_info = self.get_table_for_placeholder(sql, start_pos)
                
                if not table_info:
                    raise MetricsPipelineError(
                        f"Could not find table reference for {{partition_info}} placeholder at position {start_pos}"
                    )
                
                dataset, table_name = table_info
                replacement_date = self.get_partition_dt(dataset, table_name, partition_info_table)
                
                if not replacement_date:
                    raise MetricsPipelineError(
                        f"Could not determine partition_dt for table {dataset}.{table_name}"
                    )
                
                logger.info(f"Replacing {{partition_info}} placeholder with partition_dt: {replacement_date} for table {dataset}.{table_name}")
            
            final_sql = final_sql[:start_pos] + f"'{replacement_date}'" + final_sql[end_pos:]
        
        logger.info(f"Successfully replaced {len(placeholders)} placeholders in SQL")
        return final_sql
    
    def _validate_denominator(self, result_dict: Dict, metric_id: Optional[str] = None) -> None:
        """Validate denominator value"""
        if result_dict['denominator_value'] is None:
            return
        
        try:
            denominator_decimal = self._safe_decimal_conversion(result_dict['denominator_value'])
            if denominator_decimal is None:
                return
            
            prefix = f"Metric '{metric_id}': " if metric_id else ""
            
            if denominator_decimal == 0:
                error_msg = f"{prefix}Invalid denominator value: denominator_value is 0. Cannot calculate metrics with zero denominator."
                logger.error(error_msg)
                raise MetricsPipelineError(error_msg)
            elif denominator_decimal < 0:
                error_msg = f"{prefix}Invalid denominator value: denominator_value is negative ({denominator_decimal}). Negative denominators are not allowed."
                logger.error(error_msg)
                raise MetricsPipelineError(error_msg)
            elif abs(denominator_decimal) < Decimal('0.0000001'):
                warning_msg = f"{prefix}Very small denominator value detected: {denominator_decimal}. This may cause precision issues."
                logger.warning(warning_msg)
                
        except (ValueError, TypeError):
            logger.warning(f"Could not validate denominator_value: {result_dict['denominator_value']}")
    
    def execute_sql(self, sql: str, run_date: str, partition_info_table: str, metric_id: Optional[str] = None) -> Dict:
        """Execute SQL query with dynamic placeholder replacement"""
        def execute_and_process():
            final_sql = self.replace_sql_placeholders(sql, run_date, partition_info_table)
            logger.info(f"Executing SQL query with placeholder replacements")
            
            results = self._execute_bq_query(final_sql, "SQL execution")
            
            result_dict = {
                'metric_output': None,
                'numerator_value': None,
                'denominator_value': None,
                'business_data_date': None
            }
            
            for row in results:
                row_dict = dict(row)
                
                for key in result_dict.keys():
                    if key in row_dict:
                        value = row_dict[key]
                        if key in self.NUMERIC_FIELDS:
                            result_dict[key] = self._normalize_numeric_value(value)
                        else:
                            result_dict[key] = value
                break
            
            self._validate_denominator(result_dict, metric_id)
            
            if result_dict['business_data_date'] is not None:
                result_dict['business_data_date'] = result_dict['business_data_date'].strftime('%Y-%m-%d')
            else:
                raise MetricsPipelineError("business_data_date is required but was not returned by the SQL query")
            
            return result_dict
        
        operation_name = f"SQL execution for metric {metric_id}" if metric_id else "SQL execution"
        return self._handle_operation(execute_and_process, operation_name)
    
    def _rollback_single_metric(self, metric_id: str, target_table: str, partition_dt: str) -> None:
        """Rollback a specific metric from the target table"""
        query = f"""
        DELETE FROM `{target_table}` 
        WHERE metric_id = '{metric_id}' 
        AND partition_dt = '{partition_dt}'
        AND pipeline_execution_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """
        
        logger.info(f"Rolling back metric {metric_id} from {target_table}")
        
        try:
            self._execute_bq_query(query, f"rollback metric {metric_id}")
            logger.info(f"Successfully rolled back metric {metric_id}")
        except Exception as e:
            logger.error(f"Failed to rollback metric {metric_id}: {str(e)}")
    
    def check_dependencies_exist(self, json_data: List[Dict], dependencies: List[str]) -> None:
        """Check if all specified dependencies exist in the JSON data"""
        available_dependencies = set(record['dependency'] for record in json_data)
        missing_dependencies = set(dependencies) - available_dependencies
        
        if missing_dependencies:
            raise MetricsPipelineError(
                f"Missing dependencies in JSON data: {missing_dependencies}. "
                f"Available dependencies: {available_dependencies}"
            )
        
        logger.info(f"All dependencies found: {dependencies}")
    
    def process_metrics(self, json_data: List[Dict], run_date: str, 
                       dependencies: List[str], partition_info_table: str) -> Tuple[Dict[str, DataFrame], List[Dict], List[Dict]]:
        """Process metrics and create Spark DataFrames grouped by target_table"""
        logger.info(f"Processing metrics for dependencies: {dependencies}")
        
        self.check_dependencies_exist(json_data, dependencies)
        partition_dt = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Using pipeline run date as partition_dt: {partition_dt}")
        
        # Filter and group records
        filtered_data = [record for record in json_data if record['dependency'] in dependencies]
        
        if not filtered_data:
            raise MetricsPipelineError(f"No records found for dependencies: {dependencies}")
        
        logger.info(f"Found {len(filtered_data)} records to process")
        
        # Group by target table
        records_by_table = {}
        for record in filtered_data:
            target_table = record['target_table'].strip()
            records_by_table.setdefault(target_table, []).append(record)
        
        logger.info(f"Records grouped into {len(records_by_table)} target tables: {list(records_by_table.keys())}")
        
        # Process each group
        result_dfs = {}
        successful_metrics = []
        failed_metrics = []
        
        for target_table, records in records_by_table.items():
            logger.info(f"Processing {len(records)} metrics for target table: {target_table}")
            processed_records = []
            
            for record in records:
                try:
                    sql_results = self.execute_sql(
                        record['sql'], run_date, partition_info_table, record['metric_id']
                    )
                    
                    final_record = self._build_metric_record(record, sql_results, partition_dt)
                    processed_records.append(final_record)
                    successful_metrics.append(record)
                    logger.info(f"Successfully processed metric_id: {record['metric_id']} for table: {target_table}")
                    
                except Exception as e:
                    error_message = str(e)
                    logger.error(f"Failed to process metric_id {record['metric_id']} for table {target_table}: {error_message}")
                    failed_metrics.append({
                        'metric_record': record,
                        'error_message': error_message
                    })
                    continue
            
            # Create DataFrame if we have successful records
            if processed_records:
                schema = self._create_dataframe_schema()
                df = self.spark.createDataFrame(processed_records, schema)
                result_dfs[target_table] = df
                logger.info(f"Created DataFrame for {target_table} with {df.count()} records")
            else:
                logger.warning(f"No records processed successfully for target table: {target_table}")
        
        logger.info(f"Processing complete: {len(successful_metrics)} successful, {len(failed_metrics)} failed")
        
        if failed_metrics:
            logger.warning(f"Failed metrics: {[fm['metric_record']['metric_id'] for fm in failed_metrics]}")
        
        return result_dfs, successful_metrics, failed_metrics
    
    def get_bq_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get BigQuery table schema"""
        def get_schema():
            logger.info(f"Getting schema for table: {table_name}")
            table = self.bq_client.get_table(table_name)
            return table.schema
        
        try:
            return get_schema()
        except NotFound:
            raise MetricsPipelineError(f"Table not found: {table_name}")
        except Exception as e:
            raise MetricsPipelineError(f"Failed to get table schema: {str(e)}")
    
    def align_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame:
        """Align Spark DataFrame with BigQuery table schema"""
        logger.info(f"Aligning DataFrame schema with BigQuery table: {target_table}")
        
        bq_schema = self.get_bq_table_schema(target_table)
        current_columns = df.columns
        bq_columns = [field.name for field in bq_schema]
        
        # Filter columns and reorder
        columns_to_keep = [col for col in current_columns if col in bq_columns]
        columns_to_drop = [col for col in current_columns if col not in bq_columns]
        
        if columns_to_drop:
            logger.info(f"Dropping extra columns: {columns_to_drop}")
            df = df.drop(*columns_to_drop)
        
        df = df.select(*[col(c) for c in bq_columns if c in columns_to_keep])
        
        # Handle type conversions
        type_conversions = {
            'DATE': lambda field: df.withColumn(field.name, to_date(col(field.name))),
            'TIMESTAMP': lambda field: df.withColumn(field.name, col(field.name).cast(TimestampType())),
            'NUMERIC': lambda field: df.withColumn(field.name, col(field.name).cast(DecimalType(38, 9))),
            'FLOAT': lambda field: df.withColumn(field.name, col(field.name).cast(DoubleType()))
        }
        
        for field in bq_schema:
            if field.name in df.columns and field.field_type in type_conversions:
                df = type_conversions[field.field_type](field)
        
        logger.info(f"Schema alignment complete. Final columns: {df.columns}")
        return df
    
    def _manage_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> Tuple[List[str], List[str]]:
        """Check and delete existing metrics, return existing and new metrics"""
        if not metric_ids:
            return [], []
        
        # Check existing metrics
        escaped_ids = [mid.replace("'", "''") for mid in metric_ids]
        ids_str = "', '".join(escaped_ids)
        
        query = f"""
        SELECT DISTINCT metric_id 
        FROM `{target_table}` 
        WHERE metric_id IN ('{ids_str}') 
        AND partition_dt = '{partition_dt}'
        """
        
        logger.info(f"Checking existing metrics for partition_dt: {partition_dt}")
        
        try:
            results = self._execute_bq_query(query, "existing metrics check")
            existing_metrics = [row.metric_id for row in results]
            
            if existing_metrics:
                logger.info(f"Found {len(existing_metrics)} existing metrics: {existing_metrics}")
                
                # Delete existing metrics
                delete_query = f"""
                DELETE FROM `{target_table}` 
                WHERE metric_id IN ('{ids_str}') 
                AND partition_dt = '{partition_dt}'
                """
                
                logger.info(f"Deleting existing metrics: {existing_metrics}")
                delete_results = self._execute_bq_query(delete_query, "existing metrics deletion")
                
                deleted_count = getattr(delete_results, 'num_dml_affected_rows', 0)
                logger.info(f"Successfully deleted {deleted_count} existing records")
                
                self.overwritten_metrics.extend(existing_metrics)
            else:
                logger.info("No existing metrics found")
            
            new_metrics = [mid for mid in metric_ids if mid not in existing_metrics]
            return existing_metrics, new_metrics
            
        except Exception as e:
            logger.error(f"Failed to manage existing metrics: {str(e)}")
            raise MetricsPipelineError(f"Failed to manage existing metrics: {str(e)}")
    
    def write_to_bq_with_overwrite(self, df: DataFrame, target_table: str) -> Tuple[List[str], List[Dict]]:
        """Write DataFrame to BigQuery table with overwrite capability"""
        try:
            logger.info(f"Writing DataFrame to BigQuery table with overwrite: {target_table}")
            
            self.target_tables.add(target_table)
            
            # Get metric IDs and partition date
            metric_records = df.select('metric_id', 'partition_dt').distinct().collect()
            
            if not metric_records:
                logger.warning("No records to process")
                return [], []
            
            partition_dt = metric_records[0]['partition_dt']
            metric_ids = [row['metric_id'] for row in metric_records]
            
            logger.info(f"Processing {len(metric_ids)} metrics for partition_dt: {partition_dt}")
            
            # Manage existing metrics
            existing_metrics, new_metrics = self._manage_existing_metrics(metric_ids, partition_dt, target_table)
            
            if new_metrics:
                logger.info(f"Adding {len(new_metrics)} new metrics: {new_metrics}")
            
            # Track processed metrics
            self.processed_metrics.extend(metric_ids)
            
            # Write to BigQuery
            df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} records to {target_table}")
            logger.info(f"Summary: {len(existing_metrics)} overwritten, {len(new_metrics)} new metrics")
            
            return metric_ids, []
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"Failed to write to BigQuery with overwrite: {error_message}")
            
            # Return failed metrics
            failed_metrics = []
            metric_records = df.select('metric_id').distinct().collect()
            for row in metric_records:
                failed_metrics.append({
                    'metric_id': row['metric_id'],
                    'error_message': error_message
                })
            
            return [], failed_metrics
    
    def rollback_all_processed_metrics(self, partition_dt: str) -> None:
        """Rollback all processed metrics from all target tables"""
        logger.info("Starting rollback of processed metrics from all target tables")
        
        if not self.processed_metrics:
            logger.info("No metrics to rollback")
            return
        
        if not self.target_tables:
            logger.info("No target tables to rollback from")
            return
        
        # Only rollback newly inserted metrics
        new_metrics = [mid for mid in self.processed_metrics if mid not in self.overwritten_metrics]
        
        if new_metrics:
            logger.info(f"Rolling back {len(new_metrics)} newly inserted metrics from {len(self.target_tables)} tables")
            
            for target_table in self.target_tables:
                logger.info(f"Rolling back metrics from table: {target_table}")
                
                for metric_id in new_metrics:
                    try:
                        self._rollback_single_metric(metric_id, target_table, partition_dt)
                    except Exception as e:
                        logger.error(f"Failed to rollback metric {metric_id} from table {target_table}: {str(e)}")
        else:
            logger.info("No newly inserted metrics to rollback")
        
        if self.overwritten_metrics:
            logger.warning(f"Note: {len(self.overwritten_metrics)} overwritten metrics cannot be automatically restored: {self.overwritten_metrics}")
        
        logger.info("Rollback process completed")
    
    def _extract_table_info(self, table_reference: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract dataset and table name from table reference"""
        try:
            parts = table_reference.split('.')
            if len(parts) >= 3:
                return parts[1], parts[2]
            elif len(parts) == 2:
                return parts[0], parts[1]
            else:
                return None, None
        except Exception:
            return None, None
    
    def _build_base_recon_record(self, metric_record: Dict, run_date: str, env: str, 
                                execution_status: str, partition_dt: str) -> Dict:
        """Build base recon record with common fields"""
        current_timestamp = datetime.utcnow()
        is_success = execution_status == 'success'
        
        # Extract table info
        target_dataset, target_table = self._extract_table_info(metric_record['target_table'])
        
        return {
            'module_id': '103',
            'module_type_nm': 'Metrics',
            'source_server_nm': env,
            'target_server_nm': env,
            'target_databs_nm': target_dataset or 'UNKNOWN',
            'target_table_nm': target_table or 'UNKNOWN',
            'source_vl': '0',
            'target_vl': '0' if is_success else '1',
            'excldd_vl': '0' if is_success else '1',
            'rcncln_exact_pass_in': 'Passed' if is_success else 'Failed',
            'latest_source_parttn_dt': run_date,
            'latest_target_parttn_dt': run_date,
            'load_ts': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'schdld_dt': datetime.strptime(partition_dt, '%Y-%m-%d').date(),
            'source_system_id': metric_record['metric_id'],
            'schdld_yr': current_timestamp.year,
            'Job_Name': metric_record['metric_name']
        }
    
    def build_recon_record(self, metric_record: Dict, sql: str, run_date: str, 
                          env: str, execution_status: str, partition_dt: str, 
                          error_message: Optional[str] = None) -> Dict:
        """Build a reconciliation record for a metric"""
        try:
            # Get base record
            recon_record = self._build_base_recon_record(metric_record, run_date, env, execution_status, partition_dt)
            
            # Add source table info
            table_refs = self._find_table_references(sql)
            if table_refs:
                project, dataset, table = table_refs[0]
                recon_record.update({
                    'source_databs_nm': dataset,
                    'source_table_nm': table
                })
            else:
                recon_record.update({
                    'source_databs_nm': 'UNKNOWN',
                    'source_table_nm': 'UNKNOWN'
                })
            
            # Add status-dependent fields
            is_success = execution_status == 'success'
            if is_success:
                exclusion_reason = 'Metric data was successfully written.'
                clcltn_ds = 'Success'
            else:
                exclusion_reason = 'Metric data was failed written.'
                clcltn_ds = 'Failed'
                if error_message:
                    clean_error = error_message.replace('\n', ' ').replace('\r', ' ').strip()
                    if len(clean_error) > 500:
                        clean_error = clean_error[:497] + '...'
                    exclusion_reason += f' Error: {clean_error}'
            
            # Add remaining fields
            recon_record.update({
                'source_column_nm': 'NA',
                'source_file_nm': 'NA',
                'source_contrl_file_nm': 'NA',
                'target_column_nm': 'NA',
                'target_file_nm': 'NA',
                'target_contrl_file_nm': 'NA',
                'clcltn_ds': clcltn_ds,
                'excldd_reason_tx': exclusion_reason,
                'tolrnc_pc': 'NA',
                'rcncln_tolrnc_pass_in': 'NA'
            })
            
            logger.debug(f"Built recon record for metric {metric_record['metric_id']}: {execution_status}")
            return recon_record
            
        except Exception as e:
            logger.error(f"Failed to build recon record for metric {metric_record['metric_id']}: {str(e)}")
            raise MetricsPipelineError(f"Failed to build recon record: {str(e)}")
    
    def _create_recon_schema(self) -> StructType:
        """Create schema for recon table"""
        return StructType([
            StructField("module_id", StringType(), False),
            StructField("module_type_nm", StringType(), False),
            StructField("source_databs_nm", StringType(), True),
            StructField("source_table_nm", StringType(), True),
            StructField("source_column_nm", StringType(), True),
            StructField("source_file_nm", StringType(), True),
            StructField("source_contrl_file_nm", StringType(), True),
            StructField("source_server_nm", StringType(), False),
            StructField("target_databs_nm", StringType(), True),
            StructField("target_table_nm", StringType(), True),
            StructField("target_column_nm", StringType(), True),
            StructField("target_file_nm", StringType(), True),
            StructField("target_contrl_file_nm", StringType(), True),
            StructField("target_server_nm", StringType(), False),
            StructField("source_vl", StringType(), False),
            StructField("target_vl", StringType(), False),
            StructField("clcltn_ds", StringType(), True),
            StructField("excldd_vl", StringType(), True),
            StructField("excldd_reason_tx", StringType(), True),
            StructField("tolrnc_pc", StringType(), True),
            StructField("rcncln_exact_pass_in", StringType(), False),
            StructField("rcncln_tolrnc_pass_in", StringType(), True),
            StructField("latest_source_parttn_dt", StringType(), False),
            StructField("latest_target_parttn_dt", StringType(), False),
            StructField("load_ts", StringType(), False),
            StructField("schdld_dt", DateType(), False),
            StructField("source_system_id", StringType(), False),
            StructField("schdld_yr", IntegerType(), False),
            StructField("Job_Name", StringType(), False)
        ])
    
    def write_recon_to_bq(self, recon_records: List[Dict], recon_table: str) -> None:
        """Write reconciliation records to BigQuery recon table"""
        if not recon_records:
            logger.info("No recon records to write")
            return
        
        def write_recon():
            logger.info(f"Writing {len(recon_records)} recon records to {recon_table}")
            
            recon_schema = self._create_recon_schema()
            recon_df = self.spark.createDataFrame(recon_records, recon_schema)
            
            logger.info(f"Recon Schema for {recon_table}:")
            recon_df.printSchema()
            logger.info(f"Recon Data for {recon_table}:")
            recon_df.show(truncate=False)
            
            recon_df.write \
                .format("bigquery") \
                .option("table", recon_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {len(recon_records)} recon records to {recon_table}")
        
        self._handle_operation(write_recon, "recon records write")
    
    def create_recon_records_from_write_results(self, json_data: List[Dict], run_date: str, 
                                              dependencies: List[str], partition_info_table: str,
                                              env: str, successful_writes: Dict[str, List[str]],
                                              failed_execution_metrics: List[Dict], 
                                              failed_write_metrics: Dict[str, List[Dict]],
                                              partition_dt: str) -> List[Dict]:
        """Create recon records based on execution results and write success/failure"""
        logger.info("Creating recon records based on execution and write results")
        
        # Filter records and create lookups
        filtered_data = [record for record in json_data if record['dependency'] in dependencies]
        
        failed_execution_lookup = {
            fm['metric_record']['metric_id']: fm['error_message'] 
            for fm in failed_execution_metrics
        }
        
        failed_write_lookup = {}
        for target_table, failed_metrics in failed_write_metrics.items():
            for failed_metric in failed_metrics:
                failed_write_lookup[failed_metric['metric_id']] = failed_metric['error_message']
        
        all_recon_records = []
        
        for record in filtered_data:
            metric_id = record['metric_id']
            target_table = record['target_table'].strip()
            
            # Determine status and error message
            is_success = target_table in successful_writes and metric_id in successful_writes[target_table]
            
            error_message = None
            if not is_success:
                error_message = (failed_execution_lookup.get(metric_id) or 
                               failed_write_lookup.get(metric_id) or 
                               "Unknown failure occurred during processing")
            
            execution_status = 'success' if is_success else 'failed'
            
            try:
                final_sql = self.replace_sql_placeholders(record['sql'], run_date, partition_info_table)
                recon_record = self.build_recon_record(
                    record, final_sql, run_date, env, execution_status, partition_dt, error_message
                )
                all_recon_records.append(recon_record)
                logger.debug(f"Created recon record for metric {metric_id}: {execution_status}")
                
            except Exception as recon_error:
                logger.error(f"Failed to create recon record for metric {metric_id}: {str(recon_error)}")
                continue
        
        logger.info(f"Created {len(all_recon_records)} recon records based on execution and write results")
        return all_recon_records

@contextmanager
def managed_spark_session(app_name: str = "MetricsPipeline"):
    """Context manager for Spark session with proper cleanup"""
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        logger.info(f"Spark session created successfully: {app_name}")
        yield spark
        
    except Exception as e:
        logger.error(f"Error in Spark session: {str(e)}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='PySpark BigQuery Metrics Pipeline')
    
    required_args = [
        ('--gcs_path', 'GCS path to JSON input file'),
        ('--run_date', 'Run date in YYYY-MM-DD format'),
        ('--dependencies', 'Comma-separated list of dependencies to process'),
        ('--partition_info_table', 'BigQuery table for partition info (project.dataset.table)'),
        ('--env', 'Environment name (e.g., BLD, INT, PRE, PRD)'),
        ('--recon_table', 'BigQuery table for reconciliation data (project.dataset.table)')
    ]
    
    for arg, help_text in required_args:
        parser.add_argument(arg, required=True, help=help_text)
    
    return parser.parse_args()

def validate_date_format(date_str: str) -> None:
    """Validate date format"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise MetricsPipelineError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")

def log_pipeline_summary(pipeline: MetricsPipeline, successful_writes: Dict[str, List[str]], 
                        failed_execution_metrics: List[Dict], failed_write_metrics: Dict[str, List[Dict]]) -> None:
    """Log comprehensive pipeline summary"""
    logger.info("Pipeline completed successfully!")
    
    # Processing summary
    if pipeline.processed_metrics:
        logger.info(f"Total metrics processed: {len(pipeline.processed_metrics)}")
        if pipeline.overwritten_metrics:
            logger.info(f"Metrics overwritten: {len(pipeline.overwritten_metrics)}")
            logger.info(f"New metrics added: {len(pipeline.processed_metrics) - len(pipeline.overwritten_metrics)}")
        else:
            logger.info("All metrics were new (no existing metrics overwritten)")
    else:
        logger.info("No metrics were processed")
    
    # Execution summary
    logger.info(f"Execution results: {len([m for m in pipeline.processed_metrics if m not in [fm['metric_record']['metric_id'] for fm in failed_execution_metrics]])} successful, {len(failed_execution_metrics)} failed")
    
    if failed_execution_metrics:
        logger.warning(f"Failed to execute metrics: {[fm['metric_record']['metric_id'] for fm in failed_execution_metrics]}")
    
    # Write summary
    total_successful = sum(len(metrics) for metrics in successful_writes.values())
    total_failed_writes = sum(len(metrics) for metrics in failed_write_metrics.values())
    
    logger.info(f"Write results: {total_successful} successful, {total_failed_writes} failed")
    
    if successful_writes:
        logger.info("Successfully written metrics by table:")
        for table, metrics in successful_writes.items():
            logger.info(f"  {table}: {len(metrics)} metrics")
    
    if failed_write_metrics:
        logger.warning("Failed write metrics by table:")
        for table, metrics in failed_write_metrics.items():
            logger.warning(f"  {table}: {len(metrics)} metrics")

def main():
    """Main function with improved error handling and resource management"""
    pipeline = None
    partition_dt = None
    
    try:
        args = parse_arguments()
        validate_date_format(args.run_date)
        
        dependencies = [dep.strip() for dep in args.dependencies.split(',') if dep.strip()]
        
        if not dependencies:
            raise MetricsPipelineError("No valid dependencies provided")
        
        # Log startup info
        startup_info = [
            "Starting Metrics Pipeline",
            f"GCS Path: {args.gcs_path}",
            f"Run Date: {args.run_date}",
            f"Dependencies: {dependencies}",
            f"Partition Info Table: {args.partition_info_table}",
            f"Environment: {args.env}",
            f"Recon Table: {args.recon_table}",
            "Pipeline will check for existing metrics and overwrite them if found",
            "Target tables will be read from JSON configuration",
            "JSON must contain: metric_id, metric_name, metric_type, sql, dependency, target_table",
            "SQL placeholders: {currently} = run_date, {partition_info} = partition_dt from metadata table",
            "Reconciliation records will be written to recon table for each metric with detailed error messages"
        ]
        
        for info in startup_info:
            logger.info(info)
        
        with managed_spark_session("MetricsPipeline") as spark:
            bq_client = bigquery.Client()
            pipeline = MetricsPipeline(spark, bq_client)
            
            # Execute pipeline steps
            logger.info("Step 1: Reading JSON from GCS")
            json_data = pipeline.read_json_from_gcs(args.gcs_path)
            
            logger.info("Step 2: Validating JSON data")
            validated_data = pipeline.validate_json(json_data)
            
            logger.info("Step 3: Processing metrics")
            metrics_dfs, successful_execution_metrics, failed_execution_metrics = pipeline.process_metrics(
                validated_data, args.run_date, dependencies, args.partition_info_table
            )
            
            partition_dt = datetime.now().strftime('%Y-%m-%d')
            
            logger.info("Step 4: Writing metrics to target tables")
            successful_writes = {}
            failed_write_metrics = {}
            
            if metrics_dfs:
                logger.info(f"Found {len(metrics_dfs)} target tables with successful metrics to write")
                for target_table, df in metrics_dfs.items():
                    logger.info(f"Processing target table: {target_table}")
                    
                    aligned_df = pipeline.align_schema_with_bq(df, target_table)
                    
                    logger.info(f"Schema for {target_table}:")
                    aligned_df.printSchema()
                    aligned_df.show(truncate=False)
                    
                    logger.info(f"Writing to BigQuery table: {target_table}")
                    written_metric_ids, failed_metrics_for_table = pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                    
                    if written_metric_ids:
                        successful_writes[target_table] = written_metric_ids
                        logger.info(f"Successfully wrote {len(written_metric_ids)} metrics to {target_table}")
                    
                    if failed_metrics_for_table:
                        failed_write_metrics[target_table] = failed_metrics_for_table
                        logger.error(f"Failed to write {len(failed_metrics_for_table)} metrics to {target_table}")
            else:
                logger.warning("No metrics were successfully executed, skipping target table writes")
            
            logger.info("Step 5: Creating and writing reconciliation records")
            recon_records = pipeline.create_recon_records_from_write_results(
                validated_data, args.run_date, dependencies, args.partition_info_table,
                args.env, successful_writes, failed_execution_metrics, failed_write_metrics, partition_dt
            )
            
            pipeline.write_recon_to_bq(recon_records, args.recon_table)
            
            # Log comprehensive summary
            log_pipeline_summary(pipeline, successful_writes, failed_execution_metrics, failed_write_metrics)
            
            # Log recon summary
            if recon_records:
                logger.info(f"Total recon records created: {len(recon_records)}")
                success_count = sum(1 for r in recon_records if r.get('rcncln_exact_pass_in') == 'Passed')
                failed_count = len(recon_records) - success_count
                logger.info(f"Successful metric reconciliations: {success_count}")
                if failed_count > 0:
                    logger.info(f"Failed metric reconciliations: {failed_count}")
            else:
                logger.info("No recon records were created")
        
    except MetricsPipelineError as e:
        logger.error(f"Pipeline failed: {str(e)}")
        
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("Attempting to rollback processed metrics")
                pipeline.rollback_all_processed_metrics(partition_dt)
            except Exception as rollback_error:
                logger.error(f"Rollback failed: {str(rollback_error)}")
        
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("Attempting to rollback processed metrics")
                pipeline.rollback_all_processed_metrics(partition_dt)
            except Exception as rollback_error:
                logger.error(f"Rollback failed: {str(rollback_error)}")
        
        sys.exit(1)

if __name__ == "__main__":
    main()
