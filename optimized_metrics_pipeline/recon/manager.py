"""Reconciliation management utilities for the optimized metrics pipeline"""

from typing import Dict, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from ..config.constants import SCHEMA_DEFINITIONS
from ..config.settings import PipelineConfig
from ..bigquery.client import BigQueryClient
from ..utils.logging import LoggerMixin
from ..utils.date_utils import parse_date_string, get_current_timestamp, get_year_from_date
from ..utils.exceptions import MetricsPipelineError
import re


class ReconManager(LoggerMixin):
    """Manager for reconciliation operations"""
    
    def __init__(self, spark: SparkSession, bq_client: BigQueryClient, config: PipelineConfig):
        super().__init__()
        self.spark = spark
        self.bq_client = bq_client
        self.config = config
    
    def create_and_write_recon_records(self, json_data: List[Dict], run_date: str, 
                                      dependencies: List[str], partition_info_table: str,
                                      env: str, recon_table: str,
                                      successful_writes: Dict[str, List[str]],
                                      failed_writes: Dict[str, List[str]],
                                      partition_dt: str) -> Dict:
        """
        Create and write reconciliation records based on write results
        
        Args:
            json_data: List of metric definitions
            run_date: CLI provided run date
            dependencies: List of dependencies processed
            partition_info_table: Metadata table name
            env: Environment name
            recon_table: Recon table name
            successful_writes: Dict mapping target_table to list of successfully written metric IDs
            failed_writes: Dict mapping target_table to list of failed metric IDs
            partition_dt: Partition date used
            
        Returns:
            Dictionary with recon results
        """
        self.log_info("Creating reconciliation records based on write results")
        
        # Filter records by dependency
        filtered_data = [
            record for record in json_data 
            if record['dependency'] in dependencies
        ]
        
        recon_records = []
        
        for record in filtered_data:
            metric_id = record['metric_id']
            target_table = record['target_table'].strip()
            
            # Determine if this metric was successfully written
            is_success = False
            if target_table in successful_writes and metric_id in successful_writes[target_table]:
                is_success = True
            
            execution_status = 'success' if is_success else 'failed'
            
            try:
                recon_record = self._build_recon_record(
                    record, 
                    run_date, 
                    env, 
                    execution_status,
                    partition_dt
                )
                recon_records.append(recon_record)
                
                self.log_debug(f"Created recon record for metric {metric_id}: {execution_status}")
                
            except Exception as recon_error:
                self.log_error(f"Failed to create recon record for metric {metric_id}: {str(recon_error)}")
                continue
        
        # Write recon records to BigQuery
        if recon_records:
            self._write_recon_records(recon_records, recon_table)
        
        self.log_info(f"Created {len(recon_records)} recon records based on write results")
        
        return {
            'recon_records_count': len(recon_records),
            'recon_records': recon_records
        }
    
    def _build_recon_record(self, metric_record: Dict, run_date: str, 
                           env: str, execution_status: str, partition_dt: str) -> Dict:
        """
        Build a reconciliation record for a metric
        
        Args:
            metric_record: Original metric record from JSON
            run_date: Run date from CLI
            env: Environment from CLI
            execution_status: 'success' or 'failed'
            partition_dt: Partition date used in SQL
            
        Returns:
            Dictionary containing recon record
        """
        try:
            # Extract source table info from SQL
            source_dataset, source_table = self._get_source_table_info(metric_record['sql'])
            
            # Extract target table info
            target_table_parts = metric_record['target_table'].split('.')
            target_dataset = target_table_parts[1] if len(target_table_parts) >= 2 else None
            target_table = target_table_parts[2] if len(target_table_parts) >= 3 else None
            
            # Current timestamp and year
            current_timestamp = get_current_timestamp()
            current_year = get_year_from_date(run_date) or current_timestamp.year
            
            # Status-dependent values
            is_success = execution_status == 'success'
            
            # Build recon record with required columns
            recon_record = {
                'module_id': self.config.recon_module_id,
                'module_type_nm': self.config.recon_module_type,
                'source_server_nm': env,
                'target_server_nm': env,
                'source_vl': self.config.recon_source_vl,
                'target_vl': '0' if is_success else '1',
                'rcncln_exact_pass_in': 'Passed' if is_success else 'Failed',
                'latest_source_parttn_dt': run_date,
                'latest_target_parttn_dt': run_date,
                'load_ts': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'schdld_dt': parse_date_string(partition_dt),
                'source_system_id': metric_record['metric_id'],
                'schdld_yr': current_year,
                'Job_Name': metric_record['metric_name']
            }
            
            # Add optional columns for context
            recon_record.update({
                'source_databs_nm': source_dataset or 'UNKNOWN',
                'source_table_nm': source_table or 'UNKNOWN',
                'source_column_nm': self.config.recon_na_value,
                'source_file_nm': self.config.recon_na_value,
                'source_contrl_file_nm': self.config.recon_na_value,
                'target_databs_nm': target_dataset or 'UNKNOWN',
                'target_table_nm': target_table or 'UNKNOWN',
                'target_column_nm': self.config.recon_na_value,
                'target_file_nm': self.config.recon_na_value,
                'target_contrl_file_nm': self.config.recon_na_value,
                'clcltn_ds': 'Success' if is_success else 'Failed',
                'excldd_vl': '0' if is_success else '1',
                'excldd_reason_tx': 'Metric data was successfully written.' if is_success else 'Metric data failed to be written.',
                'tolrnc_pc': self.config.recon_na_value,
                'rcncln_tolrnc_pass_in': self.config.recon_na_value
            })
            
            self.log_debug(f"Built recon record for metric {metric_record['metric_id']}: {execution_status}")
            return recon_record
            
        except Exception as e:
            error_msg = f"Failed to build recon record for metric {metric_record['metric_id']}: {str(e)}"
            self.log_error(error_msg)
            raise MetricsPipelineError(error_msg)
    
    def _get_source_table_info(self, sql: str) -> tuple:
        """
        Extract source table dataset and table name from SQL query
        
        Args:
            sql: SQL query string
            
        Returns:
            Tuple of (dataset_name, table_name) or (None, None) if not found
        """
        try:
            # Pattern to match BigQuery table references like `project.dataset.table`
            table_pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
            
            # Find all table references in the SQL
            matches = re.findall(table_pattern, sql)
            
            if matches:
                # Take the first table reference as the source table
                project, dataset, table = matches[0]
                self.log_debug(f"Extracted source table info: dataset={dataset}, table={table}")
                return dataset, table
            else:
                self.log_warning("No source table found in SQL query")
                return None, None
                
        except Exception as e:
            self.log_error(f"Failed to extract source table info: {str(e)}")
            return None, None
    
    def _write_recon_records(self, recon_records: List[Dict], recon_table: str) -> None:
        """
        Write reconciliation records to BigQuery recon table
        
        Args:
            recon_records: List of recon records to write
            recon_table: Target recon table name
        """
        try:
            if not recon_records:
                self.log_info("No recon records to write")
                return
            
            self.log_info(f"Writing {len(recon_records)} recon records to {recon_table}")
            
            # Create DataFrame
            schema = SCHEMA_DEFINITIONS['recon']
            recon_df = self.spark.createDataFrame(recon_records, schema)
            
            # Write to BigQuery
            write_config = self.config.bigquery_config.get_write_options()
            
            recon_df.write \
                .format(write_config['format']) \
                .option("table", recon_table) \
                .option("writeMethod", write_config['writeMethod']) \
                .mode(write_config['mode']) \
                .save()
            
            self.log_info(f"Successfully wrote {len(recon_records)} recon records to {recon_table}")
            
        except Exception as e:
            error_msg = f"Failed to write recon records to BigQuery: {str(e)}"
            self.log_error(error_msg)
            raise MetricsPipelineError(error_msg)
    
    def get_recon_statistics(self, recon_records: List[Dict]) -> Dict:
        """
        Get statistics about reconciliation records
        
        Args:
            recon_records: List of recon records
            
        Returns:
            Dictionary with recon statistics
        """
        if not recon_records:
            return {}
        
        total_records = len(recon_records)
        passed_records = sum(1 for r in recon_records if r.get('rcncln_exact_pass_in') == 'Passed')
        failed_records = total_records - passed_records
        
        return {
            'total_recon_records': total_records,
            'passed_recon_records': passed_records,
            'failed_recon_records': failed_records,
            'success_rate': (passed_records / total_records * 100) if total_records > 0 else 0
        }