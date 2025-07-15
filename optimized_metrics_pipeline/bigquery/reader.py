"""BigQuery reader utilities for the metrics pipeline"""

from typing import List, Dict, Optional, Tuple
from .client import BigQueryClient
from ..utils.logging import LoggerMixin


class BigQueryReader(LoggerMixin):
    """Reader for BigQuery operations"""
    
    def __init__(self, bq_client: BigQueryClient):
        super().__init__()
        self.bq_client = bq_client
    
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
        return self.bq_client.get_partition_info(project_dataset, table_name, partition_info_table)
    
    def get_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]:
        """
        Get existing metric IDs from target table
        
        Args:
            metric_ids: List of metric IDs to check
            partition_dt: Partition date
            target_table: Target table name
            
        Returns:
            List of existing metric IDs
        """
        return self.bq_client.get_existing_metrics(metric_ids, partition_dt, target_table)
    
    def get_metric_history(self, metric_id: str, target_table: str, days_back: int = 30) -> List[Dict]:
        """
        Get historical data for a specific metric
        
        Args:
            metric_id: Metric ID to query
            target_table: Target table name
            days_back: Number of days to look back
            
        Returns:
            List of historical metric records
        """
        try:
            query = f"""
            SELECT *
            FROM `{target_table}`
            WHERE metric_id = '{metric_id.replace("'", "''")}'
            AND partition_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
            ORDER BY partition_dt DESC
            """
            
            self.log_info(f"Getting {days_back} days of history for metric: {metric_id}")
            results = self.bq_client.execute_query(query)
            
            self.log_info(f"Found {len(results)} historical records for metric {metric_id}")
            return results
            
        except Exception as e:
            self.log_error(f"Failed to get metric history for {metric_id}: {str(e)}")
            return []
    
    def get_metrics_by_dependency(self, dependency: str, target_table: str, partition_dt: str) -> List[Dict]:
        """
        Get all metrics for a specific dependency and partition date
        
        Args:
            dependency: Dependency name
            target_table: Target table name
            partition_dt: Partition date
            
        Returns:
            List of metric records
        """
        try:
            # Note: This assumes there's a dependency column in the target table
            # If not, you might need to join with the original JSON data
            query = f"""
            SELECT *
            FROM `{target_table}`
            WHERE partition_dt = '{partition_dt}'
            ORDER BY metric_id
            """
            
            self.log_info(f"Getting metrics for dependency: {dependency}, partition: {partition_dt}")
            results = self.bq_client.execute_query(query)
            
            self.log_info(f"Found {len(results)} metrics for dependency {dependency}")
            return results
            
        except Exception as e:
            self.log_error(f"Failed to get metrics for dependency {dependency}: {str(e)}")
            return []
    
    def get_table_statistics(self, target_table: str) -> Dict:
        """
        Get basic statistics about a target table
        
        Args:
            target_table: Target table name
            
        Returns:
            Dictionary with table statistics
        """
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT metric_id) as unique_metrics,
                COUNT(DISTINCT partition_dt) as unique_partitions,
                MIN(partition_dt) as earliest_partition,
                MAX(partition_dt) as latest_partition,
                MIN(pipeline_execution_ts) as earliest_execution,
                MAX(pipeline_execution_ts) as latest_execution
            FROM `{target_table}`
            """
            
            self.log_info(f"Getting statistics for table: {target_table}")
            results = self.bq_client.execute_query(query)
            
            if results:
                stats = results[0]
                self.log_info(f"Table statistics: {stats}")
                return stats
            else:
                return {}
                
        except Exception as e:
            self.log_error(f"Failed to get table statistics: {str(e)}")
            return {}
    
    def validate_partition_info_table(self, partition_info_table: str) -> bool:
        """
        Validate that the partition info table exists and has expected structure
        
        Args:
            partition_info_table: Partition info table name
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check if table exists
            if not self.bq_client.check_table_exists(partition_info_table):
                self.log_error(f"Partition info table does not exist: {partition_info_table}")
                return False
            
            # Check expected columns
            query = f"""
            SELECT column_name
            FROM `{partition_info_table.split('.')[0]}.{partition_info_table.split('.')[1]}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{partition_info_table.split('.')[2]}'
            """
            
            try:
                results = self.bq_client.execute_query(query)
                columns = [row['column_name'] for row in results]
                
                expected_columns = ['project_dataset', 'table_name', 'partition_dt']
                missing_columns = [col for col in expected_columns if col not in columns]
                
                if missing_columns:
                    self.log_error(f"Missing columns in partition info table: {missing_columns}")
                    return False
                
                self.log_info("Partition info table validation passed")
                return True
                
            except Exception:
                # If we can't check the schema, assume it's valid
                self.log_warning("Could not validate partition info table schema, assuming valid")
                return True
                
        except Exception as e:
            self.log_error(f"Failed to validate partition info table: {str(e)}")
            return False
    
    def get_failed_metrics(self, target_table: str, partition_dt: str) -> List[str]:
        """
        Get metrics that failed to process (if there's a status column)
        
        Args:
            target_table: Target table name
            partition_dt: Partition date
            
        Returns:
            List of failed metric IDs
        """
        try:
            # This is a placeholder implementation
            # In a real scenario, you might have a status column or separate error table
            query = f"""
            SELECT DISTINCT metric_id
            FROM `{target_table}`
            WHERE partition_dt = '{partition_dt}'
            AND (metric_output IS NULL OR denominator_value = 0)
            """
            
            self.log_info(f"Getting failed metrics for partition: {partition_dt}")
            results = self.bq_client.execute_query(query)
            
            failed_metrics = [row['metric_id'] for row in results]
            
            if failed_metrics:
                self.log_warning(f"Found {len(failed_metrics)} potentially failed metrics: {failed_metrics}")
            
            return failed_metrics
            
        except Exception as e:
            self.log_error(f"Failed to get failed metrics: {str(e)}")
            return []