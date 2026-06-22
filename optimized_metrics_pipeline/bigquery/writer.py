"""BigQuery writer utilities for the metrics pipeline"""

from typing import List, Dict, Set
from pyspark.sql import DataFrame
from .client import BigQueryClient
from ..config.settings import BigQueryConfig
from ..utils.exceptions import BigQueryError
from ..utils.logging import LoggerMixin


class BigQueryWriter(LoggerMixin):
    """Writer for BigQuery operations with transaction safety"""
    
    def __init__(self, bq_client: BigQueryClient, config: BigQueryConfig):
        super().__init__()
        self.bq_client = bq_client
        self.config = config
        self.processed_metrics: List[str] = []
        self.overwritten_metrics: List[str] = []
        self.target_tables: Set[str] = set()
    
    def write_dataframe(self, df: DataFrame, target_table: str) -> List[str]:
        """
        Write DataFrame to BigQuery table with basic append
        
        Args:
            df: Spark DataFrame to write
            target_table: Target BigQuery table
            
        Returns:
            List of metric IDs that were written
        """
        try:
            self.log_info(f"Writing DataFrame to BigQuery table: {target_table}")
            
            # Get metric IDs from DataFrame
            metric_ids = [row['metric_id'] for row in df.select('metric_id').collect()]
            
            # Write to BigQuery using Spark BigQuery connector
            write_options = self.config.get_write_options()
            
            df.write \
                .format(write_options['format']) \
                .option("table", target_table) \
                .option("writeMethod", write_options['writeMethod']) \
                .mode(write_options['mode']) \
                .save()
            
            self.log_info(f"Successfully wrote {df.count()} records to {target_table}")
            
            # Track processed metrics
            self.processed_metrics.extend(metric_ids)
            self.target_tables.add(target_table)
            
            return metric_ids
            
        except Exception as e:
            error_msg = f"Failed to write to BigQuery: {str(e)}"
            self.log_error(error_msg)
            raise BigQueryError(error_msg)
    
    def write_with_overwrite(self, df: DataFrame, target_table: str) -> List[str]:
        """
        Write DataFrame to BigQuery table with overwrite capability for existing metrics
        
        Args:
            df: Spark DataFrame to write
            target_table: Target BigQuery table
            
        Returns:
            List of metric IDs that were successfully written
        """
        try:
            self.log_info(f"Writing DataFrame to BigQuery table with overwrite: {target_table}")
            
            # Track target table for rollback
            self.target_tables.add(target_table)
            
            # Collect metric IDs and partition date from the DataFrame
            metric_records = df.select('metric_id', 'partition_dt').distinct().collect()
            
            if not metric_records:
                self.log_warning("No records to process")
                return []
            
            # Get partition date (assuming all records have the same partition_dt)
            partition_dt = metric_records[0]['partition_dt']
            metric_ids = [row['metric_id'] for row in metric_records]
            
            self.log_info(f"Processing {len(metric_ids)} metrics for partition_dt: {partition_dt}")
            
            # Check which metrics already exist
            existing_metrics = self.bq_client.get_existing_metrics(metric_ids, partition_dt, target_table)
            
            # Track overwritten vs new metrics
            new_metrics = [mid for mid in metric_ids if mid not in existing_metrics]
            
            # Delete existing metrics if any
            if existing_metrics:
                self.log_info(f"Overwriting {len(existing_metrics)} existing metrics: {existing_metrics}")
                self.bq_client.delete_existing_metrics(existing_metrics, partition_dt, target_table)
                # Track overwritten metrics separately
                self.overwritten_metrics.extend(existing_metrics)
            
            if new_metrics:
                self.log_info(f"Adding {len(new_metrics)} new metrics: {new_metrics}")
            
            # Add all metric IDs to processed metrics for rollback tracking
            self.processed_metrics.extend(metric_ids)
            
            # Write the DataFrame to BigQuery
            write_options = self.config.get_write_options()
            
            df.write \
                .format(write_options['format']) \
                .option("table", target_table) \
                .option("writeMethod", write_options['writeMethod']) \
                .mode(write_options['mode']) \
                .save()
            
            self.log_info(f"Successfully wrote {df.count()} records to {target_table}")
            self.log_info(f"Summary: {len(existing_metrics)} overwritten, {len(new_metrics)} new metrics")
            
            return metric_ids
            
        except Exception as e:
            error_msg = f"Failed to write to BigQuery with overwrite: {str(e)}"
            self.log_error(error_msg)
            raise BigQueryError(error_msg)
    
    def rollback_processed_metrics(self, partition_dt: str, timeout_hours: int = 1) -> None:
        """
        Rollback all processed metrics from all target tables in case of failure
        
        Args:
            partition_dt: Partition date for rollback
            timeout_hours: Rollback timeout in hours
        """
        self.log_info("Starting rollback of processed metrics from all target tables")
        
        if not self.processed_metrics:
            self.log_info("No metrics to rollback")
            return
        
        if not self.target_tables:
            self.log_info("No target tables to rollback from")
            return
        
        # Only rollback newly inserted metrics (not overwritten ones)
        new_metrics = [mid for mid in self.processed_metrics if mid not in self.overwritten_metrics]
        
        if new_metrics:
            self.log_info(f"Rolling back {len(new_metrics)} newly inserted metrics from {len(self.target_tables)} tables")
            
            for target_table in self.target_tables:
                self.log_info(f"Rolling back metrics from table: {target_table}")
                
                # Find metrics that were inserted into this specific table
                for metric_id in new_metrics:
                    try:
                        self.bq_client.rollback_metric(metric_id, target_table, partition_dt, timeout_hours)
                    except Exception as e:
                        self.log_error(f"Failed to rollback metric {metric_id} from table {target_table}: {str(e)}")
        else:
            self.log_info("No newly inserted metrics to rollback")
        
        if self.overwritten_metrics:
            self.log_warning(f"Note: {len(self.overwritten_metrics)} overwritten metrics cannot be automatically restored: {self.overwritten_metrics}")
        
        self.log_info("Rollback process completed")
    
    def get_write_statistics(self) -> Dict[str, any]:
        """
        Get statistics about the write operations
        
        Returns:
            Dictionary with write statistics
        """
        return {
            'total_processed_metrics': len(self.processed_metrics),
            'total_overwritten_metrics': len(self.overwritten_metrics),
            'total_new_metrics': len(self.processed_metrics) - len(self.overwritten_metrics),
            'target_tables': list(self.target_tables),
            'processed_metrics': self.processed_metrics,
            'overwritten_metrics': self.overwritten_metrics
        }
    
    def clear_tracking(self) -> None:
        """Clear all tracking information"""
        self.processed_metrics.clear()
        self.overwritten_metrics.clear()
        self.target_tables.clear()
        self.log_info("Cleared all tracking information")
    
    def validate_before_write(self, df: DataFrame, target_table: str) -> bool:
        """
        Validate DataFrame before writing to BigQuery
        
        Args:
            df: Spark DataFrame
            target_table: Target BigQuery table
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check if table exists
            if not self.bq_client.check_table_exists(target_table):
                self.log_error(f"Target table does not exist: {target_table}")
                return False
            
            # Check if DataFrame has data
            if df.count() == 0:
                self.log_warning("DataFrame is empty")
                return False
            
            # Check required columns
            required_columns = ['metric_id', 'partition_dt']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                self.log_error(f"Missing required columns: {missing_columns}")
                return False
            
            self.log_info("Pre-write validation passed")
            return True
            
        except Exception as e:
            self.log_error(f"Pre-write validation failed: {str(e)}")
            return False