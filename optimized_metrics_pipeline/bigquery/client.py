"""BigQuery client utilities for the metrics pipeline"""

from typing import List, Dict, Optional, Tuple
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
from ..config.constants import ERROR_MESSAGES
from ..utils.exceptions import BigQueryError
from ..utils.logging import LoggerMixin


class BigQueryClient(LoggerMixin):
    """Enhanced BigQuery client with common operations"""
    
    def __init__(self, client: bigquery.Client = None):
        super().__init__()
        self.client = client or bigquery.Client()
    
    def execute_query(self, query: str) -> List[Dict]:
        """
        Execute a BigQuery query and return results
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of result dictionaries
            
        Raises:
            BigQueryError: If query execution fails
        """
        try:
            self.log_debug(f"Executing BigQuery query: {query}")
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Convert results to list of dictionaries
            result_list = [dict(row) for row in results]
            
            self.log_debug(f"Query returned {len(result_list)} rows")
            return result_list
            
        except Exception as e:
            error_msg = f"Failed to execute BigQuery query: {str(e)}"
            self.log_error(error_msg)
            raise BigQueryError(error_msg)
    
    def get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """
        Get BigQuery table schema
        
        Args:
            table_name: Full table name (project.dataset.table)
            
        Returns:
            List of schema fields
            
        Raises:
            BigQueryError: If table not found or other error
        """
        try:
            self.log_info(f"Getting schema for table: {table_name}")
            table = self.client.get_table(table_name)
            return table.schema
            
        except NotFound:
            raise BigQueryError(ERROR_MESSAGES['table_not_found'].format(table=table_name))
        except Exception as e:
            error_msg = f"Failed to get table schema: {str(e)}"
            self.log_error(error_msg)
            raise BigQueryError(error_msg)
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if BigQuery table exists
        
        Args:
            table_name: Full table name (project.dataset.table)
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            self.client.get_table(table_name)
            return True
        except NotFound:
            return False
        except Exception as e:
            self.log_warning(f"Error checking table existence: {str(e)}")
            return False
    
    def get_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]:
        """
        Check which metric IDs already exist in BigQuery table for the given partition date
        
        Args:
            metric_ids: List of metric IDs to check
            partition_dt: Partition date to check
            target_table: Target BigQuery table
            
        Returns:
            List of existing metric IDs
        """
        try:
            if not metric_ids:
                return []
            
            # Escape single quotes in metric IDs for safety
            escaped_metric_ids = [mid.replace("'", "''") for mid in metric_ids]
            metric_ids_str = "', '".join(escaped_metric_ids)
            
            query = f"""
            SELECT DISTINCT metric_id 
            FROM `{target_table}` 
            WHERE metric_id IN ('{metric_ids_str}') 
            AND partition_dt = '{partition_dt}'
            """
            
            self.log_info(f"Checking existing metrics for partition_dt: {partition_dt}")
            self.log_debug(f"Query: {query}")
            
            results = self.execute_query(query)
            existing_metrics = [row['metric_id'] for row in results]
            
            if existing_metrics:
                self.log_info(f"Found {len(existing_metrics)} existing metrics: {existing_metrics}")
            else:
                self.log_info("No existing metrics found")
            
            return existing_metrics
            
        except Exception as e:
            error_msg = f"Failed to check existing metrics: {str(e)}"
            self.log_error(error_msg)
            raise BigQueryError(error_msg)
    
    def delete_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> int:
        """
        Delete existing metrics from BigQuery table for the given partition date
        
        Args:
            metric_ids: List of metric IDs to delete
            partition_dt: Partition date for deletion
            target_table: Target BigQuery table
            
        Returns:
            Number of rows deleted
        """
        try:
            if not metric_ids:
                self.log_info("No metrics to delete")
                return 0
            
            # Escape single quotes in metric IDs for safety
            escaped_metric_ids = [mid.replace("'", "''") for mid in metric_ids]
            metric_ids_str = "', '".join(escaped_metric_ids)
            
            delete_query = f"""
            DELETE FROM `{target_table}` 
            WHERE metric_id IN ('{metric_ids_str}') 
            AND partition_dt = '{partition_dt}'
            """
            
            self.log_info(f"Deleting existing metrics: {metric_ids} for partition_dt: {partition_dt}")
            self.log_debug(f"Delete query: {delete_query}")
            
            query_job = self.client.query(delete_query)
            results = query_job.result()
            
            # Get the number of deleted rows
            deleted_count = results.num_dml_affected_rows if hasattr(results, 'num_dml_affected_rows') else 0
            
            self.log_info(f"Successfully deleted {deleted_count} existing records for metrics: {metric_ids}")
            return deleted_count
            
        except Exception as e:
            error_msg = f"Failed to delete existing metrics: {str(e)}"
            self.log_error(error_msg)
            raise BigQueryError(error_msg)
    
    def get_partition_info(self, project_dataset: str, table_name: str, partition_info_table: str) -> Optional[str]:
        """
        Get latest partition_dt from metadata table
        
        Args:
            project_dataset: Dataset name
            table_name: Table name
            partition_info_table: Metadata table name
            
        Returns:
            Latest partition date as string or None
        """
        try:
            query = f"""
            SELECT partition_dt 
            FROM `{partition_info_table}` 
            WHERE project_dataset = '{project_dataset}' 
            AND table_name = '{table_name}'
            ORDER BY partition_dt DESC
            LIMIT 1
            """
            
            self.log_info(f"Querying partition info for {project_dataset}.{table_name}")
            
            results = self.execute_query(query)
            
            if results:
                partition_dt = results[0]['partition_dt']
                if hasattr(partition_dt, 'strftime'):
                    return partition_dt.strftime('%Y-%m-%d')
                return str(partition_dt)
            
            self.log_warning(f"No partition info found for {project_dataset}.{table_name}")
            return None
            
        except Exception as e:
            self.log_error(f"Failed to get partition_dt for {project_dataset}.{table_name}: {str(e)}")
            return None
    
    def rollback_metric(self, metric_id: str, target_table: str, partition_dt: str, timeout_hours: int = 1) -> None:
        """
        Rollback a specific metric from the target table
        
        Args:
            metric_id: Metric ID to rollback
            target_table: Target BigQuery table
            partition_dt: Partition date for the metric
            timeout_hours: Rollback timeout in hours
        """
        try:
            delete_query = f"""
            DELETE FROM `{target_table}` 
            WHERE metric_id = '{metric_id.replace("'", "''")}' 
            AND partition_dt = '{partition_dt}'
            AND pipeline_execution_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {timeout_hours} HOUR)
            """
            
            self.log_info(f"Rolling back metric {metric_id} from {target_table}")
            
            query_job = self.client.query(delete_query)
            query_job.result()
            
            self.log_info(f"Successfully rolled back metric {metric_id}")
            
        except Exception as e:
            error_msg = ERROR_MESSAGES['rollback_failed'].format(metric_id=metric_id, error=str(e))
            self.log_error(error_msg)
            raise BigQueryError(error_msg)