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
        logger.info("PIPELINE CONFIGURATION:")
        logger.info("-"*60)
        logger.info(f"GCS Path: {args.gcs_path}")
        logger.info(f"Run Date: {args.run_date}")
        logger.info(f"Dependencies: {dependencies} ({len(dependencies)} total)")
        logger.info(f"Partition Info Table: {args.partition_info_table}")
        logger.info(f"Environment: {args.env}")
        logger.info(f"Recon Table: {args.recon_table}")
        logger.info("-"*60)
        logger.info("PIPELINE BEHAVIOR:")
        logger.info("   Pipeline uses overwrite functionality - existing metrics for the same partition will be replaced")
        logger.info("   Target tables will be read from JSON configuration")
        logger.info("   JSON must contain: metric_id, metric_name, metric_type, sql, dependency, target_table")
        logger.info("   SQL placeholders: {currently} = run_date, {partition_info} = partition_dt from metadata table")
        logger.info("   Reconciliation records will be written to recon table for each metric with detailed error messages")
        logger.info("-"*60)
    
    def execute_pipeline_steps(self, args: argparse.Namespace, dependencies: list) -> None:
        """Execute the main pipeline steps with comprehensive error handling"""
        json_data = None
        validated_data = None
        
        logger.info("="*80)
        logger.info("STARTING METRICS PIPELINE EXECUTION")
        logger.info("="*80)
        
        with managed_spark_session("MetricsPipeline") as spark:  # Create managed Spark session
            # Initialize BigQuery operations and pipeline
            logger.info("Initializing BigQuery operations and pipeline...")
            bq_operations = create_bigquery_operations(spark)  # Set up BigQuery client
            self.pipeline = MetricsPipeline(spark, bq_operations)  # Initialize main pipeline
            logger.info("Pipeline initialization completed")
            
            try:
                # Step 0: Validate partition info table before processing
                logger.info("-"*60)
                logger.info("STEP 0: VALIDATING PARTITION INFO TABLE")
                logger.info("-"*60)
                logger.info(f"Validating partition info table: {args.partition_info_table}")
                self.pipeline.validate_partition_info_table(args.partition_info_table)
                logger.info("Partition info table validation completed successfully")
                
                # Step 1: Read and validate JSON
                logger.info("-"*60)
                logger.info("STEP 1: READING JSON FROM GCS")
                logger.info("-"*60)
                logger.info(f"Reading JSON from GCS path: {args.gcs_path}")
                json_data = self.pipeline.read_json_from_gcs(args.gcs_path)
                logger.info(f"Successfully read {len(json_data)} records from JSON")
                
                logger.info("-"*60)
                logger.info("STEP 2: VALIDATING JSON DATA")
                logger.info("-"*60)
                logger.info(f"Validating {len(json_data)} JSON records...")
                validated_data = self.pipeline.validate_json(json_data)
                logger.info(f"Successfully validated {len(validated_data)} JSON records")
                
                # Step 3: Process metrics
                logger.info("-"*60)
                logger.info("STEP 3: PROCESSING METRICS")
                logger.info("-"*60)
                logger.info(f"Processing metrics for dependencies: {dependencies}")
                logger.info(f"Run date: {args.run_date}")
                metrics_dfs, successful_execution_metrics, failed_execution_metrics = self.pipeline.process_metrics(
                    validated_data, 
                    args.run_date, 
                    dependencies, 
                    args.partition_info_table
                )
                logger.info(f"Metrics processing completed:")
                logger.info(f"   Successful executions: {len(successful_execution_metrics)}")
                logger.info(f"   Failed executions: {len(failed_execution_metrics)}")
                logger.info(f"   Target tables with data: {len(metrics_dfs)}")
                
                # Step 4: Write metrics to target tables
                logger.info("-"*60)
                logger.info("STEP 4: WRITING METRICS TO TARGET TABLES")
                logger.info("-"*60)
                successful_writes, failed_write_metrics = self.write_metrics_to_tables(metrics_dfs)
                logger.info(f"Metrics writing completed:")
                logger.info(f"   Successful writes: {sum(len(metrics) for metrics in successful_writes.values())}")
                logger.info(f"   Failed writes: {sum(len(metrics) for metrics in failed_write_metrics.values())}")
                
                # Step 5: Create and write reconciliation records
                logger.info("-"*60)
                logger.info("STEP 5: CREATING AND WRITING RECONCILIATION RECORDS")
                logger.info("-"*60)
                logger.info("RECON RECORD CREATION LOGIC VERIFICATION:")
                logger.info("    Successful metrics (written to target tables) → Recon status: 'Passed'")
                logger.info("    Failed metrics (not written to target tables) → Recon status: 'Failed'")
                logger.info("-"*40)
                self.create_and_write_recon_records(
                    validated_data, args, dependencies, successful_writes, 
                    failed_execution_metrics, failed_write_metrics
                )
                
                # Log final statistics
                logger.info("-"*60)
                logger.info("FINAL PIPELINE STATISTICS")
                logger.info("-"*60)
                self.log_pipeline_statistics(
                    successful_execution_metrics, failed_execution_metrics, 
                    successful_writes, failed_write_metrics
                )
                
            except Exception as pipeline_error:
                logger.error("="*80)
                logger.error("PIPELINE EXECUTION FAILED")
                logger.error("="*80)
                logger.error(f"Error: {str(pipeline_error)}")
                logger.error(f"Error type: {type(pipeline_error).__name__}")
                
                # Try to write failure recon records if we have the necessary data
                if validated_data is not None:
                    try:
                        logger.info("Attempting to create recon records for pipeline failure...")
                        logger.info(f"Using validated data with {len(validated_data)} records")
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
                        logger.info("Attempting to create recon records for pipeline failure (using unvalidated JSON data)...")
                        logger.info(f"Using unvalidated JSON data with {len(json_data)} records")
                        failure_recon_records = self.pipeline.create_pipeline_failure_recon_records(
                            json_data, args.run_date, dependencies, args.env, 
                            str(pipeline_error), "PIPELINE_VALIDATION_ERROR"
                        )
                        self.pipeline.write_recon_to_bq(failure_recon_records, args.recon_table)
                        logger.info(f"Successfully wrote {len(failure_recon_records)} failure recon records")
                    except Exception as recon_error:
                        logger.error(f"Failed to write failure recon records: {str(recon_error)}")
                else:
                    logger.error("No data available to create failure recon records")
                
                logger.error("="*80)
                logger.error("Re-raising original pipeline error...")
                logger.error("="*80)
                # Re-raise the original error
                raise pipeline_error
    
    def write_metrics_to_tables(self, metrics_dfs: dict) -> tuple:
        """Write metrics DataFrames to BigQuery tables"""
        logger.info("Starting metrics write to BigQuery tables...")
        successful_writes = {}
        failed_write_metrics = {}
        
        if metrics_dfs:
            logger.info(f"Found {len(metrics_dfs)} target tables with successful metrics to write")
            
            for i, (target_table, df) in enumerate(metrics_dfs.items(), 1):
                logger.info("-"*50)
                logger.info(f"[{i}/{len(metrics_dfs)}] Processing target table: {target_table}")
                
                try:
                    record_count = df.count()
                    logger.info(f"   Records to write: {record_count}")
                    
                    # Align schema with BigQuery
                    logger.info("   Aligning schema with BigQuery table...")
                    aligned_df = self.pipeline.align_schema_with_bq(df, target_table)
                    logger.info("   Schema alignment completed")
                    
                    # Show schema and data for debugging
                    logger.info(f"   Schema for {target_table}:")
                    aligned_df.printSchema()
                    logger.info(f"   Sample data for {target_table} (first 3 records):")
                    aligned_df.show(3, truncate=False)
                    
                    # Write to BigQuery with overwrite capability
                    logger.info(f"   Writing to BigQuery table: {target_table}")
                    written_metric_ids, failed_metrics_for_table = self.pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                    
                    if written_metric_ids:
                        successful_writes[target_table] = written_metric_ids
                        logger.info(f"   Successfully wrote {len(written_metric_ids)} metrics to {target_table}")
                        logger.info(f"      Written metrics: {written_metric_ids[:5]}{'...' if len(written_metric_ids) > 5 else ''}")
                    
                    if failed_metrics_for_table:
                        failed_write_metrics[target_table] = failed_metrics_for_table
                        logger.error(f"   Failed to write {len(failed_metrics_for_table)} metrics to {target_table}")
                        for j, failed_metric in enumerate(failed_metrics_for_table[:3], 1):
                            logger.error(f"      {j}. {failed_metric.get('metric_id', 'UNKNOWN')}: {failed_metric.get('error_message', 'No error message')[:100]}...")
                        if len(failed_metrics_for_table) > 3:
                            logger.error(f"      ... and {len(failed_metrics_for_table) - 3} more failed metrics")
                    
                except Exception as table_error:
                    logger.error(f"   Failed to process table {target_table}: {str(table_error)}")
                    # Add all metrics from this table to failed writes
                    try:
                        metric_records = df.select('metric_id').distinct().collect()
                        table_failed_metrics = []
                        for row in metric_records:
                            table_failed_metrics.append({
                                'metric_id': row['metric_id'],
                                'error_message': str(table_error)
                            })
                        failed_write_metrics[target_table] = table_failed_metrics
                    except:
                        logger.error(f"   Could not extract metric IDs from failed table {target_table}")
        else:
            logger.warning("No metrics were successfully executed, skipping target table writes")
        
        logger.info("-"*50)
        logger.info("WRITE SUMMARY:")
        total_successful = sum(len(metrics) for metrics in successful_writes.values())
        total_failed = sum(len(metrics) for metrics in failed_write_metrics.values())
        logger.info(f"   Total successful writes: {total_successful}")
        logger.info(f"   Total failed writes: {total_failed}")
        logger.info(f"   Tables with successful writes: {len(successful_writes)}")
        logger.info(f"   Tables with failed writes: {len(failed_write_metrics)}")
        
        return successful_writes, failed_write_metrics
    
    def create_and_write_recon_records(self, validated_data: list, args: argparse.Namespace, 
                                     dependencies: list, successful_writes: dict, 
                                     failed_execution_metrics: list, failed_write_metrics: dict) -> None:
        """Create and write reconciliation records"""
        logger.info("Starting reconciliation record creation...")
        
        # Get partition_dt from DateUtils (same as used in pipeline)
        from utils import DateUtils
        partition_dt = DateUtils.get_current_partition_dt()  # Get current date for partitioning
        logger.info(f"Using partition_dt: {partition_dt}")
        
        # Validate required parameters before creating recon records
        logger.info("Validating required parameters for recon record creation...")
        if validated_data is None:
            logger.error("validated_data is None, cannot create recon records")
            return
        if args.run_date is None:
            logger.error("args.run_date is None, cannot create recon records")
            return
        if args.env is None:
            logger.error("args.env is None, cannot create recon records")
            return
        if partition_dt is None:
            logger.error("partition_dt is None, cannot create recon records")
            return
        
        logger.info("All required parameters validated successfully")
        logger.info(f"Input data summary:")
        logger.info(f"   Validated data records: {len(validated_data)}")
        logger.info(f"   Run date: {args.run_date}")
        logger.info(f"   Environment: {args.env}")
        logger.info(f"   Dependencies: {dependencies}")
        logger.info(f"   Successful writes: {sum(len(metrics) for metrics in successful_writes.values())} metrics across {len(successful_writes)} tables")
        logger.info(f"   Failed execution metrics: {len(failed_execution_metrics)}")
        logger.info(f"   Failed write metrics: {sum(len(metrics) for metrics in failed_write_metrics.values())} metrics across {len(failed_write_metrics)} tables")
        
        logger.info("Creating recon records from write results...")
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
        logger.info(f"Writing {len(recon_records)} recon records to BigQuery table: {args.recon_table}")
        self.pipeline.write_recon_to_bq(recon_records, args.recon_table)  # Store tracking records
        logger.info("Recon records successfully written to BigQuery")
        
        # Log recon statistics
        if recon_records:  # Calculate and log success/failure counts
            logger.info("RECONCILIATION STATISTICS:")
            logger.info(f"   Total recon records created: {len(recon_records)}")
            success_count = sum(1 for r in recon_records if r.get('rcncln_exact_pass_in') == 'Passed')
            failed_count = len(recon_records) - success_count
            logger.info(f"   Successful metric reconciliations: {success_count}")
            if failed_count > 0:
                logger.info(f"   Failed metric reconciliations: {failed_count}")
                # Log details of failed metrics
                failed_metrics = [r for r in recon_records if r.get('rcncln_exact_pass_in') == 'Failed']
                for i, failed_record in enumerate(failed_metrics[:5]):  # Show first 5 failures
                    logger.info(f"      {i+1}. Metric {failed_record.get('source_system_id', 'UNKNOWN')}: {failed_record.get('excldd_reason_tx', 'No reason provided')[:100]}...")
                if len(failed_metrics) > 5:
                    logger.info(f"      ... and {len(failed_metrics) - 5} more failed metrics")
        else:
            logger.warning("No recon records were created")
    
    def log_pipeline_statistics(self, successful_execution_metrics: list, failed_execution_metrics: list,
                               successful_writes: dict, failed_write_metrics: dict) -> None:
        """Log comprehensive pipeline statistics"""
        logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        
        # Log processing statistics
        if self.pipeline.processed_metrics:
            logger.info("PROCESSING STATISTICS:")
            logger.info(f"   Total metrics processed: {len(self.pipeline.processed_metrics)}")
            logger.info(f"   Target tables used: {len(self.pipeline.target_tables)}")
            logger.info(f"   Target tables: {list(self.pipeline.target_tables)}")
            logger.info("   Metrics successfully written with overwrite capability")
        else:
            logger.warning("No metrics were processed")
        
        # Log execution statistics
        logger.info("EXECUTION STATISTICS:")
        logger.info(f"   Successful executions: {len(successful_execution_metrics)}")
        logger.info(f"   Failed executions: {len(failed_execution_metrics)}")
        
        if failed_execution_metrics:
            logger.warning("FAILED EXECUTION DETAILS:")
            failed_by_category = {}
            for fm in failed_execution_metrics:
                category = fm.get('error_category', 'UNKNOWN')
                if category not in failed_by_category:
                    failed_by_category[category] = []
                failed_by_category[category].append(fm['metric_record']['metric_id'])
            
            for category, metrics in failed_by_category.items():
                logger.warning(f"   {category}: {len(metrics)} metrics")
                for metric_id in metrics[:3]:  # Show first 3 metrics per category
                    logger.warning(f"      - {metric_id}")
                if len(metrics) > 3:
                    logger.warning(f"      ... and {len(metrics) - 3} more")
        
        # Write summary
        total_successful = sum(len(metrics) for metrics in successful_writes.values())
        total_failed_writes = sum(len(metrics) for metrics in failed_write_metrics.values())
        
        logger.info("WRITE STATISTICS:")
        logger.info(f"   Successful writes: {total_successful}")
        logger.info(f"   Failed writes: {total_failed_writes}")
        
        if successful_writes:
            logger.info("   Successful writes by table:")
            for table, metrics in successful_writes.items():
                logger.info(f"      {table}: {len(metrics)} metrics")
        
        if failed_write_metrics:
            logger.warning("   Failed writes by table:")
            for table, metrics in failed_write_metrics.items():
                logger.warning(f"      {table}: {len(metrics)} metrics")
        
        # Overall success rate
        total_metrics = len(successful_execution_metrics) + len(failed_execution_metrics)
        if total_metrics > 0:
            success_rate = (len(successful_execution_metrics) / total_metrics) * 100
            logger.info(f"OVERALL SUCCESS RATE: {success_rate:.1f}% ({len(successful_execution_metrics)}/{total_metrics})")
        
        logger.info("RECON RECORD VERIFICATION:")
        logger.info("    All successful metric executions are written to target tables")
        logger.info("    All successful writes are marked as 'Passed' in recon table")
        logger.info("    All failed metric executions are marked as 'Failed' in recon table")
        logger.info("    Failed metrics are NOT written to target tables (as expected)")
        logger.info("="*80)
        logger.info("PIPELINE EXECUTION SUMMARY COMPLETED")


def main():
    """Main application entry point"""
    print("="*80)
    print("METRICS PIPELINE STARTING")
    print("="*80)
    
    orchestrator = PipelineOrchestrator()  # Create pipeline orchestrator
    
    try:
        logger.info("Initializing pipeline orchestrator...")
        
        # Parse and validate arguments
        logger.info("Parsing command line arguments...")
        args = orchestrator.parse_arguments()  # Get CLI arguments
        logger.info("Command line arguments parsed successfully")
        
        logger.info("Validating and parsing dependencies...")
        dependencies = orchestrator.validate_and_parse_dependencies(args.dependencies)  # Parse dependency list
        logger.info(f"Dependencies validated: {dependencies}")
        
        # Log pipeline information
        logger.info("Logging pipeline configuration...")
        orchestrator.log_pipeline_info(args, dependencies)  # Log configuration details
        
        # Execute pipeline
        logger.info("Starting pipeline execution...")
        orchestrator.execute_pipeline_steps(args, dependencies)  # Run main pipeline logic
        
        print("="*80)
        print("METRICS PIPELINE COMPLETED SUCCESSFULLY")
        print("="*80)
        
    except MetricsPipelineError as e:
        print("="*80)
        print("METRICS PIPELINE FAILED - KNOWN ERROR")
        print("="*80)
        logger.error(f"Pipeline error: {str(e)}")  # Log known pipeline errors
        logger.error("Pipeline failed - any partial writes will be handled by overwrite functionality on next run")
        print(f"Error: {str(e)}")
        sys.exit(1)  # Exit with error code
        
    except Exception as e:
        print("="*80)
        print("METRICS PIPELINE FAILED - UNEXPECTED ERROR")
        print("="*80)
        logger.error(f"Unexpected error: {str(e)}")  # Log unexpected errors
        logger.error(f"Error type: {type(e).__name__}")
        logger.error("Pipeline failed with unexpected error - check logs for details")
        print(f"Unexpected error: {str(e)}")
        print("Check logs for detailed error information")
        sys.exit(1)  # Exit with error code


if __name__ == "__main__":
    main()