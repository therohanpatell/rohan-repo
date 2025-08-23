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
        logger.info("Starting Metrics Pipeline")
        logger.info(f"GCS Path: {args.gcs_path}")
        logger.info(f"Run Date: {args.run_date}")
        logger.info(f"Dependencies: {dependencies}")
        logger.info(f"Partition Info Table: {args.partition_info_table}")
        logger.info(f"Environment: {args.env}")
        logger.info(f"Recon Table: {args.recon_table}")
        logger.info("Pipeline uses overwrite functionality - existing metrics for the same partition will be replaced")
        logger.info("Target tables will be read from JSON configuration")
        logger.info("JSON must contain: metric_id, metric_name, metric_type, sql, dependency, target_table")
        logger.info("SQL placeholders: {currently} = run_date, {partition_info} = partition_dt from metadata table")
        logger.info("Reconciliation records will be written to recon table for each metric with detailed error messages")
    
    def execute_pipeline_steps(self, args: argparse.Namespace, dependencies: list) -> None:
        """Execute the main pipeline steps with comprehensive error handling"""
        json_data = None
        validated_data = None
        
        with managed_spark_session("MetricsPipeline") as spark:  # Create managed Spark session
            # Initialize BigQuery operations and pipeline
            bq_operations = create_bigquery_operations(spark)  # Set up BigQuery client
            self.pipeline = MetricsPipeline(spark, bq_operations)  # Initialize main pipeline
            
            try:
                # Step 0: Validate partition info table before processing
                logger.info("Step 0: Validating partition info table")
                self.pipeline.validate_partition_info_table(args.partition_info_table)
                
                # Step 1: Read and validate JSON
                logger.info("Step 1: Reading JSON from GCS")
                json_data = self.pipeline.read_json_from_gcs(args.gcs_path)
                
                logger.info("Step 2: Validating JSON data")
                validated_data = self.pipeline.validate_json(json_data)
                
                # Step 3: Process metrics
                logger.info("Step 3: Processing metrics")
                metrics_dfs, successful_execution_metrics, failed_execution_metrics = self.pipeline.process_metrics(
                    validated_data, 
                    args.run_date, 
                    dependencies, 
                    args.partition_info_table
                )
                
                # Step 4: Write metrics to target tables
                logger.info("Step 4: Writing metrics to target tables")
                successful_writes, failed_write_metrics = self.write_metrics_to_tables(metrics_dfs)
                
                # Step 5: Create and write reconciliation records
                logger.info("Step 5: Creating and writing reconciliation records")
                self.create_and_write_recon_records(
                    validated_data, args, dependencies, successful_writes, 
                    failed_execution_metrics, failed_write_metrics
                )
                
                # Log final statistics
                self.log_pipeline_statistics(
                    successful_execution_metrics, failed_execution_metrics, 
                    successful_writes, failed_write_metrics
                )
                
            except Exception as pipeline_error:
                logger.error(f"Pipeline execution failed: {str(pipeline_error)}")
                
                # Try to write failure recon records if we have the necessary data
                if validated_data is not None:
                    try:
                        logger.info("Creating recon records for pipeline failure")
                        failure_recon_records = self.pipeline.create_pipeline_failure_recon_records(
                            validated_data, args.run_date, dependencies, args.env, 
                            str(pipeline_error), "PIPELINE_EXECUTION_ERROR"
                        )
                        self.pipeline.write_recon_to_bq(failure_recon_records, args.recon_table)
                        logger.info(f"Successfully wrote {len(failure_recon_records)} failure recon records")
                    except Exception as recon_error:
                        logger.error(f"Failed to write failure recon records: {str(recon_error)}")
                elif json_data is not None:
                    try:
                        logger.info("Creating recon records for pipeline failure (using unvalidated JSON data)")
                        failure_recon_records = self.pipeline.create_pipeline_failure_recon_records(
                            json_data, args.run_date, dependencies, args.env, 
                            str(pipeline_error), "PIPELINE_VALIDATION_ERROR"
                        )
                        self.pipeline.write_recon_to_bq(failure_recon_records, args.recon_table)
                        logger.info(f"Successfully wrote {len(failure_recon_records)} failure recon records")
                    except Exception as recon_error:
                        logger.error(f"Failed to write failure recon records: {str(recon_error)}")
                
                # Re-raise the original error
                raise pipeline_error
    
    def write_metrics_to_tables(self, metrics_dfs: dict) -> tuple:
        """Write metrics DataFrames to BigQuery tables"""
        successful_writes = {}
        failed_write_metrics = {}
        
        if metrics_dfs:
            logger.info(f"Found {len(metrics_dfs)} target tables with successful metrics to write")
            for target_table, df in metrics_dfs.items():
                logger.info(f"Processing target table: {target_table}")
                
                # Align schema with BigQuery
                aligned_df = self.pipeline.align_schema_with_bq(df, target_table)
                
                # Show schema and data for debugging
                logger.info(f"Schema for {target_table}:")
                aligned_df.printSchema()
                aligned_df.show(truncate=False)
                
                # Write to BigQuery with overwrite capability
                logger.info(f"Writing to BigQuery table: {target_table}")
                written_metric_ids, failed_metrics_for_table = self.pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                
                if written_metric_ids:
                    successful_writes[target_table] = written_metric_ids
                    logger.info(f"Successfully wrote {len(written_metric_ids)} metrics to {target_table}")
                
                if failed_metrics_for_table:
                    failed_write_metrics[target_table] = failed_metrics_for_table
                    logger.error(f"Failed to write {len(failed_metrics_for_table)} metrics to {target_table}")
        else:
            logger.warning("No metrics were successfully executed, skipping target table writes")
        
        return successful_writes, failed_write_metrics
    
    def create_and_write_recon_records(self, validated_data: list, args: argparse.Namespace, 
                                     dependencies: list, successful_writes: dict, 
                                     failed_execution_metrics: list, failed_write_metrics: dict) -> None:
        """Create and write reconciliation records"""
        # Get partition_dt from DateUtils (same as used in pipeline)
        from utils import DateUtils
        partition_dt = DateUtils.get_current_partition_dt()  # Get current date for partitioning
        
        recon_records = self.pipeline.create_recon_records_from_write_results(
            validated_data,
            args.run_date,
            dependencies,
            args.partition_info_table,
            args.env,
            successful_writes,
            failed_execution_metrics,
            failed_write_metrics,
            partition_dt
        )
        
        # Write recon records to recon table
        self.pipeline.write_recon_to_bq(recon_records, args.recon_table)  # Store tracking records
        
        # Log recon statistics
        if recon_records:  # Calculate and log success/failure counts
            logger.info(f"Total recon records created: {len(recon_records)}")
            success_count = sum(1 for r in recon_records if r.get('rcncln_exact_pass_in') == 'Passed')
            failed_count = len(recon_records) - success_count
            logger.info(f"Successful metric reconciliations: {success_count}")
            if failed_count > 0:
                logger.info(f"Failed metric reconciliations: {failed_count}")
        else:
            logger.info("No recon records were created")
    
    def log_pipeline_statistics(self, successful_execution_metrics: list, failed_execution_metrics: list,
                               successful_writes: dict, failed_write_metrics: dict) -> None:
        """Log comprehensive pipeline statistics"""
        logger.info("Pipeline completed successfully!")
        
        # Log processing statistics
        if self.pipeline.processed_metrics:
            logger.info(f"Total metrics processed: {len(self.pipeline.processed_metrics)}")
            logger.info("Metrics successfully written with overwrite capability")
        else:
            logger.info("No metrics were processed")
        
        # Log execution statistics
        logger.info(f"Execution results: {len(successful_execution_metrics)} successful, {len(failed_execution_metrics)} failed")
        
        if failed_execution_metrics:
            logger.warning(f"Failed to execute metrics: {[fm['metric_record']['metric_id'] for fm in failed_execution_metrics]}")
        
        # Write summary
        total_successful = sum(len(metrics) for metrics in successful_writes.values())
        total_failed_writes = sum(len(metrics) for metrics in failed_write_metrics.values())
        
        logger.info(f"Write results: {total_successful} successful, {total_failed_writes} failed")


def main():
    """Main application entry point"""
    orchestrator = PipelineOrchestrator()  # Create pipeline orchestrator
    
    try:
        # Parse and validate arguments
        args = orchestrator.parse_arguments()  # Get CLI arguments
        dependencies = orchestrator.validate_and_parse_dependencies(args.dependencies)  # Parse dependency list
        
        # Log pipeline information
        orchestrator.log_pipeline_info(args, dependencies)  # Log configuration details
        
        # Execute pipeline
        orchestrator.execute_pipeline_steps(args, dependencies)  # Run main pipeline logic
        
    except MetricsPipelineError as e:
        logger.error(f"Pipeline error: {str(e)}")  # Log known pipeline errors
        logger.error("Pipeline failed - any partial writes will be handled by overwrite functionality on next run")
        sys.exit(1)  # Exit with error code
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")  # Log unexpected errors
        logger.error("Pipeline failed with unexpected error - check logs for details")
        sys.exit(1)  # Exit with error code


if __name__ == "__main__":
    main()