"""
Main application entry point for the Metrics Pipeline
Handles argument parsing, pipeline orchestration, and error handling
"""

import argparse
import sys
from datetime import datetime

from config import setup_logging
from exceptions import MetricsPipelineError
from utils import DateUtils, managed_spark_session
from pipeline import MetricsPipeline
from bigquery import create_bigquery_operations

logger = setup_logging()


class PipelineOrchestrator:
    """Orchestrates the entire pipeline execution"""
    
    def __init__(self):
        self.pipeline = None  # Will be initialized with Spark session
    
    def parse_arguments(self) -> argparse.Namespace:
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(  # Set up CLI argument parser
            description='PySpark BigQuery Metrics Pipeline'
        )
        
        parser.add_argument(
            '--gcs_path', 
            required=True, 
            help='GCS path to JSON input file'
        )
        parser.add_argument(
            '--run_date', 
            required=True, 
            help='Run date in YYYY-MM-DD format'
        )
        parser.add_argument(
            '--dependencies', 
            required=True, 
            help='Comma-separated list of dependencies to process'
        )
        parser.add_argument(
            '--partition_info_table', 
            required=True, 
            help='BigQuery table for partition info (project.dataset.table)'
        )
        parser.add_argument(
            '--env', 
            required=True, 
            help='Environment name (e.g., BLD, PRD, DEV)'
        )
        parser.add_argument(
            '--recon_table', 
            required=True, 
            help='BigQuery table for reconciliation data (project.dataset.table)'
        )
        
        return parser.parse_args()  # Parse and return all arguments
    
    def validate_and_parse_dependencies(self, dependencies_str: str) -> list:
        """Validate and parse dependencies string"""
        dependencies = [dep.strip() for dep in dependencies_str.split(',') if dep.strip()]  # Split and clean dependencies
        
        if not dependencies:
            raise MetricsPipelineError("No valid dependencies provided")  # Ensure at least one dependency exists
        
        return dependencies
    
    def log_pipeline_info(self, args: argparse.Namespace, dependencies: list) -> None:
        """Log pipeline configuration information"""
        logger.info(f"Pipeline: {args.gcs_path} | Run: {args.run_date} | Deps: {len(dependencies)} | Env: {args.env}")
    
    def execute_pipeline_steps(self, args: argparse.Namespace, dependencies: list) -> None:
        """Execute the main pipeline steps with comprehensive error handling"""
        json_data = None
        validated_data = None
        
        logger.info("Starting pipeline execution")
        
        with managed_spark_session("MetricsPipeline") as spark:
            bq_operations = create_bigquery_operations(spark)
            self.pipeline = MetricsPipeline(spark, bq_operations)
            
            try:
                self.pipeline.validate_partition_info_table(args.partition_info_table)
                json_data = self.pipeline.read_json_from_gcs(args.gcs_path)
                validated_data = self.pipeline.validate_json(json_data)
                logger.info(f"Validated {len(validated_data)} records")
                
                metrics_dfs, successful_execution_metrics, failed_execution_metrics = self.pipeline.process_metrics(
                    validated_data, args.run_date, dependencies, args.partition_info_table
                )
                logger.info(f"Processed: {len(successful_execution_metrics)} success, {len(failed_execution_metrics)} failed")
                
                successful_writes, failed_write_metrics = self.write_metrics_to_tables(metrics_dfs)
                logger.info(f"Written: {sum(len(m) for m in successful_writes.values())} success, {sum(len(m) for m in failed_write_metrics.values())} failed")
                
                self.create_and_write_recon_records(
                    validated_data, args, dependencies, successful_writes, 
                    failed_execution_metrics, failed_write_metrics
                )
                
                self.log_pipeline_statistics(
                    successful_execution_metrics, failed_execution_metrics, 
                    successful_writes, failed_write_metrics
                )
                
            except Exception as pipeline_error:
                logger.error(f"Pipeline failed: {str(pipeline_error)}")
                
                # Try to write failure recon records
                if validated_data is not None:
                    try:
                        failure_recon_records = self.pipeline.create_pipeline_failure_recon_records(
                            validated_data, args.run_date, dependencies, args.env, 
                            str(pipeline_error), "PIPELINE_EXECUTION_ERROR"
                        )
                        self.pipeline.write_recon_to_bq(failure_recon_records, args.recon_table)
                    except Exception as recon_error:
                        logger.error(f"Failed to write failure recon records: {str(recon_error)}")
                elif json_data is not None:
                    try:
                        failure_recon_records = self.pipeline.create_pipeline_failure_recon_records(
                            json_data, args.run_date, dependencies, args.env, 
                            str(pipeline_error), "PIPELINE_VALIDATION_ERROR"
                        )
                        self.pipeline.write_recon_to_bq(failure_recon_records, args.recon_table)
                    except Exception as recon_error:
                        logger.error(f"Failed to write failure recon records: {str(recon_error)}")
                
                raise pipeline_error
    
    def write_metrics_to_tables(self, metrics_dfs: dict) -> tuple:
        """Write metrics DataFrames to BigQuery tables"""
        successful_writes = {}
        failed_write_metrics = {}
        
        if metrics_dfs:
            for target_table, df in metrics_dfs.items():
                try:
                    aligned_df = self.pipeline.align_schema_with_bq(df, target_table)
                    written_metric_ids, failed_metrics_for_table = self.pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                    
                    if written_metric_ids:
                        successful_writes[target_table] = written_metric_ids
                    if failed_metrics_for_table:
                        failed_write_metrics[target_table] = failed_metrics_for_table
                        logger.error(f"Failed writes to {target_table}: {len(failed_metrics_for_table)} metrics")
                    
                except Exception as table_error:
                    logger.error(f"Failed to process table {target_table}: {str(table_error)}")
                    try:
                        metric_records = df.select('metric_id').distinct().collect()
                        failed_write_metrics[target_table] = [{'metric_id': row['metric_id'], 'error_message': str(table_error)} for row in metric_records]
                    except:
                        pass
        
        return successful_writes, failed_write_metrics
    
    def create_and_write_recon_records(self, validated_data: list, args: argparse.Namespace, 
                                     dependencies: list, successful_writes: dict, 
                                     failed_execution_metrics: list, failed_write_metrics: dict) -> None:
        """Create and write reconciliation records"""
        from utils import DateUtils
        partition_dt = DateUtils.get_current_partition_dt()
        
        if not all([validated_data, args.run_date, args.env, partition_dt]):
            logger.error("Missing required parameters for recon record creation")
            return
        
        recon_records = self.pipeline.create_recon_records_from_write_results(
            validated_data, args.run_date, dependencies, args.partition_info_table,
            args.env, successful_writes, failed_execution_metrics, failed_write_metrics, partition_dt
        )
        
        self.pipeline.write_recon_to_bq(recon_records, args.recon_table)
        logger.info(f"Recon: {len(recon_records)} records written")
    
    def log_pipeline_statistics(self, successful_execution_metrics: list, failed_execution_metrics: list,
                               successful_writes: dict, failed_write_metrics: dict) -> None:
        """Log comprehensive pipeline statistics"""
        total_successful = sum(len(metrics) for metrics in successful_writes.values())
        total_failed_writes = sum(len(metrics) for metrics in failed_write_metrics.values())
        total_metrics = len(successful_execution_metrics) + len(failed_execution_metrics)
        
        logger.info(f"Pipeline complete: {total_successful} written, {total_failed_writes} failed writes, {len(failed_execution_metrics)} failed executions")
        
        if total_metrics > 0:
            success_rate = (len(successful_execution_metrics) / total_metrics) * 100
            logger.info(f"Success rate: {success_rate:.1f}% ({len(successful_execution_metrics)}/{total_metrics})")


def main():
    """Main application entry point"""
    orchestrator = PipelineOrchestrator()
    
    try:
        args = orchestrator.parse_arguments()
        dependencies = orchestrator.validate_and_parse_dependencies(args.dependencies)
        orchestrator.log_pipeline_info(args, dependencies)
        orchestrator.execute_pipeline_steps(args, dependencies)
        print("Pipeline completed successfully")
        
    except MetricsPipelineError as e:
        logger.error(f"Pipeline error: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()