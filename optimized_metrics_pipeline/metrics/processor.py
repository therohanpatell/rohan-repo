"""Metrics processing utilities for the optimized metrics pipeline"""

from typing import Dict, List, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from ..config.constants import SCHEMA_DEFINITIONS
from ..config.settings import PipelineConfig
from ..bigquery.client import BigQueryClient
from ..sql.placeholder_manager import PlaceholderManager
from ..utils.logging import LoggerMixin
from ..utils.numeric_utils import normalize_numeric_value, safe_decimal_conversion
from ..utils.date_utils import format_date_for_sql, get_current_timestamp
from ..utils.exceptions import MetricsPipelineError, SQLExecutionError


class MetricsProcessor(LoggerMixin):
    """Processor for metrics execution and DataFrame creation"""
    
    def __init__(self, spark: SparkSession, bq_client: BigQueryClient, 
                 placeholder_manager: PlaceholderManager, config: PipelineConfig):
        super().__init__()
        self.spark = spark
        self.bq_client = bq_client
        self.placeholder_manager = placeholder_manager
        self.config = config
    
    def process_metrics(self, json_data: List[Dict], run_date: str, 
                       dependencies: List[str], partition_info_table: str,
                       partition_dt: str) -> Dict:
        """
        Process metrics and create Spark DataFrames grouped by target_table
        
        Args:
            json_data: List of metric definitions
            run_date: CLI provided run date
            dependencies: List of dependencies to process
            partition_info_table: Metadata table name
            partition_dt: Partition date for records
            
        Returns:
            Dictionary with processing results
        """
        self.log_info(f"Processing metrics for dependencies: {dependencies}")
        
        # Filter records by dependency
        filtered_data = self._filter_by_dependencies(json_data, dependencies)
        
        # Group records by target_table
        records_by_table = self._group_by_target_table(filtered_data)
        
        # Process each group and create DataFrames
        dataframes = {}
        successful_metrics = []
        failed_metrics = []
        
        for target_table, records in records_by_table.items():
            self.log_info(f"Processing {len(records)} metrics for target table: {target_table}")
            
            table_results = self._process_table_metrics(
                records, run_date, partition_info_table, partition_dt
            )
            
            if table_results['successful_records']:
                df = self._create_dataframe(table_results['successful_records'])
                dataframes[target_table] = df
                self.log_info(f"Created DataFrame for {target_table} with {df.count()} records")
            
            successful_metrics.extend(table_results['successful_metrics'])
            failed_metrics.extend(table_results['failed_metrics'])
        
        # Log summary
        self.log_info(f"Processing complete: {len(successful_metrics)} successful, {len(failed_metrics)} failed")
        
        return {
            'dataframes': dataframes,
            'successful_metrics': successful_metrics,
            'failed_metrics': failed_metrics,
            'successful_count': len(successful_metrics),
            'failed_count': len(failed_metrics)
        }
    
    def _filter_by_dependencies(self, json_data: List[Dict], dependencies: List[str]) -> List[Dict]:
        """Filter records by dependencies"""
        filtered_data = [
            record for record in json_data 
            if record['dependency'] in dependencies
        ]
        
        if not filtered_data:
            raise MetricsPipelineError(f"No records found for dependencies: {dependencies}")
        
        self.log_info(f"Found {len(filtered_data)} records to process")
        return filtered_data
    
    def _group_by_target_table(self, filtered_data: List[Dict]) -> Dict[str, List[Dict]]:
        """Group records by target_table"""
        records_by_table = {}
        for record in filtered_data:
            target_table = record['target_table'].strip()
            if target_table not in records_by_table:
                records_by_table[target_table] = []
            records_by_table[target_table].append(record)
        
        self.log_info(f"Records grouped into {len(records_by_table)} target tables: {list(records_by_table.keys())}")
        return records_by_table
    
    def _process_table_metrics(self, records: List[Dict], run_date: str, 
                              partition_info_table: str, partition_dt: str) -> Dict:
        """Process all metrics for a specific table"""
        processed_records = []
        successful_metrics = []
        failed_metrics = []
        
        for record in records:
            try:
                # Execute SQL and get results
                sql_results = self._execute_metric_sql(
                    record['sql'], 
                    run_date, 
                    partition_info_table,
                    record['metric_id']
                )
                
                # Build final record
                final_record = self._build_metric_record(record, sql_results, partition_dt)
                
                processed_records.append(final_record)
                successful_metrics.append(record)
                self.log_info(f"Successfully processed metric_id: {record['metric_id']}")
                
            except Exception as e:
                self.log_error(f"Failed to process metric_id {record['metric_id']}: {str(e)}")
                failed_metrics.append(record)
                continue
        
        return {
            'successful_records': processed_records,
            'successful_metrics': successful_metrics,
            'failed_metrics': failed_metrics
        }
    
    def _execute_metric_sql(self, sql: str, run_date: str, 
                           partition_info_table: str, metric_id: str) -> Dict:
        """Execute SQL query for a metric"""
        try:
            # Replace placeholders with appropriate dates
            final_sql = self.placeholder_manager.replace_sql_placeholders(
                sql, run_date, partition_info_table
            )
            
            self.log_info(f"Executing SQL query for metric {metric_id}")
            self.log_debug(f"SQL query: {final_sql}")
            
            # Execute query
            results = self.bq_client.execute_query(final_sql)
            
            if not results:
                raise SQLExecutionError(f"No results returned for metric {metric_id}")
            
            # Process first result row
            result_dict = self._process_sql_result(results[0])
            
            return result_dict
            
        except Exception as e:
            error_msg = f"Failed to execute SQL for metric {metric_id}: {str(e)}"
            self.log_error(error_msg)
            raise SQLExecutionError(error_msg)
    
    def _process_sql_result(self, row: Dict) -> Dict:
        """Process a single SQL result row"""
        result_dict = {
            'metric_output': None,
            'numerator_value': None,
            'denominator_value': None,
            'business_data_date': None
        }
        
        # Map and normalize values
        for key in result_dict.keys():
            if key in row:
                value = row[key]
                if key in ['metric_output', 'numerator_value', 'denominator_value']:
                    result_dict[key] = normalize_numeric_value(value)
                else:
                    result_dict[key] = value
        
        # Format business_data_date
        if result_dict['business_data_date'] is not None:
            if hasattr(result_dict['business_data_date'], 'strftime'):
                result_dict['business_data_date'] = result_dict['business_data_date'].strftime('%Y-%m-%d')
            else:
                result_dict['business_data_date'] = str(result_dict['business_data_date'])
        else:
            raise SQLExecutionError("business_data_date is required but was not returned by the SQL query")
        
        return result_dict
    
    def _build_metric_record(self, record: Dict, sql_results: Dict, partition_dt: str) -> Dict:
        """Build final metric record for DataFrame"""
        return {
            'metric_id': record['metric_id'],
            'metric_name': record['metric_name'],
            'metric_type': record['metric_type'],
            'numerator_value': safe_decimal_conversion(sql_results['numerator_value']),
            'denominator_value': safe_decimal_conversion(sql_results['denominator_value']),
            'metric_output': safe_decimal_conversion(sql_results['metric_output']),
            'business_data_date': sql_results['business_data_date'],
            'partition_dt': partition_dt,
            'pipeline_execution_ts': get_current_timestamp()
        }
    
    def _create_dataframe(self, records: List[Dict]) -> DataFrame:
        """Create Spark DataFrame from processed records"""
        schema = SCHEMA_DEFINITIONS['metrics']
        df = self.spark.createDataFrame(records, schema)
        
        self.log_info(f"Created DataFrame with {len(records)} records")
        return df