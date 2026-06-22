"""
Main pipeline orchestrator for the optimized metrics pipeline

This module provides the main entry point and orchestrates all the components
to create an optimized, modular, and maintainable metrics processing pipeline.
"""

import argparse
import sys
from typing import Dict, List, Tuple
from datetime import datetime

# Configuration and utilities
from .config import PipelineConfig, get_default_config
from .utils import (
    setup_logging, get_logger, managed_spark_session, 
    validate_date_format, get_current_partition_dt, MetricsPipelineError
)

# Core components
from .validation import JsonValidator, GcsValidator, DataValidator
from .bigquery import BigQueryClient, SchemaManager, BigQueryWriter
from .sql import PlaceholderManager
from .recon import ReconManager
from .metrics import MetricsProcessor

# Google Cloud imports
from google.cloud import bigquery


class OptimizedMetricsPipeline:
    """Main pipeline orchestrator that coordinates all components"""
    
    def __init__(self, config: PipelineConfig = None):
        """
        Initialize the pipeline with configuration
        
        Args:
            config: Pipeline configuration (uses default if None)
        """
        self.config = config or get_default_config()
        
        # Setup logging
        setup_logging(self.config)
        self.logger = get_logger(self.__class__.__name__)
        
        # Initialize components (will be set up in run method)
        self.spark = None
        self.bq_client = None
        self.components = {}
        
        self.logger.info("OptimizedMetricsPipeline initialized")
    
    def _initialize_components(self):
        """Initialize all pipeline components"""
        self.logger.info("Initializing pipeline components")
        
        # Initialize BigQuery client
        self.bq_client = BigQueryClient(bigquery.Client())
        
        # Initialize validators
        self.components['json_validator'] = JsonValidator()
        self.components['gcs_validator'] = GcsValidator(self.spark)
        self.components['data_validator'] = DataValidator(self.config)
        
        # Initialize BigQuery components
        self.components['schema_manager'] = SchemaManager(self.bq_client)
        self.components['bq_writer'] = BigQueryWriter(self.bq_client, self.config.bigquery_config)
        
        # Initialize SQL components
        self.components['placeholder_manager'] = PlaceholderManager(self.bq_client)
        
        # Initialize metrics processor
        self.components['metrics_processor'] = MetricsProcessor(
            self.spark, 
            self.bq_client, 
            self.components['placeholder_manager'],
            self.config
        )
        
        # Initialize recon manager
        self.components['recon_manager'] = ReconManager(
            self.spark,
            self.bq_client,
            self.config
        )
        
        self.logger.info("All components initialized successfully")
    
    def run(self, args: argparse.Namespace) -> None:
        """
        Main pipeline execution method
        
        Args:
            args: Parsed command line arguments
        """
        pipeline_start_time = datetime.now()
        partition_dt = get_current_partition_dt()
        
        try:
            self.logger.info("="*60)
            self.logger.info("STARTING OPTIMIZED METRICS PIPELINE")
            self.logger.info("="*60)
            
            # Log pipeline parameters
            self._log_pipeline_parameters(args, partition_dt)
            
            # Parse and validate dependencies
            dependencies = self._parse_dependencies(args.dependencies)
            
            # Initialize Spark session
            with managed_spark_session(self.config.spark_config) as spark:
                self.spark = spark
                self._initialize_components()
                
                # Execute pipeline steps
                results = self._execute_pipeline_steps(args, dependencies, partition_dt)
                
                # Log final results
                self._log_final_results(results, pipeline_start_time)
                
                self.logger.info("="*60)
                self.logger.info("PIPELINE COMPLETED SUCCESSFULLY")
                self.logger.info("="*60)
        
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            
            # Attempt rollback if enabled
            if self.config.enable_rollback and self.components.get('bq_writer'):
                self._attempt_rollback(partition_dt)
            
            raise
    
    def _execute_pipeline_steps(self, args: argparse.Namespace, dependencies: List[str], 
                               partition_dt: str) -> Dict:
        """Execute all pipeline steps in sequence"""
        results = {}
        
        # Step 1: Read and validate JSON data
        self.logger.info("STEP 1: Reading and validating JSON data")
        json_data = self.components['gcs_validator'].read_json_from_gcs(args.gcs_path)
        validated_data = self.components['json_validator'].validate_json_data(json_data)
        results['total_records'] = len(validated_data)
        
        # Step 2: Process metrics
        self.logger.info("STEP 2: Processing metrics")
        metrics_results = self.components['metrics_processor'].process_metrics(
            validated_data, 
            args.run_date, 
            dependencies, 
            args.partition_info_table,
            partition_dt
        )
        results.update(metrics_results)
        
        # Step 3: Write metrics to BigQuery
        self.logger.info("STEP 3: Writing metrics to BigQuery")
        write_results = self._write_metrics_to_bigquery(metrics_results['dataframes'])
        results.update(write_results)
        
        # Step 4: Generate and write reconciliation records
        self.logger.info("STEP 4: Generating reconciliation records")
        recon_results = self.components['recon_manager'].create_and_write_recon_records(
            validated_data,
            args.run_date,
            dependencies,
            args.partition_info_table,
            args.env,
            args.recon_table,
            write_results['successful_writes'],
            write_results['failed_writes'],
            partition_dt
        )
        results.update(recon_results)
        
        return results
    
    def _write_metrics_to_bigquery(self, dataframes: Dict) -> Dict:
        """Write all metrics DataFrames to their respective BigQuery tables"""
        successful_writes = {}
        failed_writes = {}
        
        for target_table, df in dataframes.items():
            try:
                self.logger.info(f"Processing target table: {target_table}")
                
                # Align schema with BigQuery
                aligned_df = self.components['schema_manager'].align_dataframe_with_bq_schema(df, target_table)
                
                # Log schema and preview data
                self.logger.info(f"Schema for {target_table}:")
                aligned_df.printSchema()
                aligned_df.show(5, truncate=False)
                
                # Write to BigQuery with overwrite capability
                written_metrics = self.components['bq_writer'].write_with_overwrite(aligned_df, target_table)
                successful_writes[target_table] = written_metrics
                
                self.logger.info(f"Successfully wrote {len(written_metrics)} metrics to {target_table}")
                
            except Exception as e:
                # Get metric IDs that failed to write
                failed_metric_ids = [row['metric_id'] for row in df.select('metric_id').collect()]
                failed_writes[target_table] = failed_metric_ids
                self.logger.error(f"Failed to write metrics to {target_table}: {str(e)}")
        
        return {
            'successful_writes': successful_writes,
            'failed_writes': failed_writes
        }
    
    def _parse_dependencies(self, dependencies_str: str) -> List[str]:
        """Parse and validate dependencies from command line"""
        dependencies = [dep.strip() for dep in dependencies_str.split(',') if dep.strip()]
        
        if not dependencies:
            raise MetricsPipelineError("No valid dependencies provided")
        
        self.logger.info(f"Parsed dependencies: {dependencies}")
        return dependencies
    
    def _log_pipeline_parameters(self, args: argparse.Namespace, partition_dt: str) -> None:
        """Log all pipeline parameters"""
        self.logger.info(f"GCS Path: {args.gcs_path}")
        self.logger.info(f"Run Date: {args.run_date}")
        self.logger.info(f"Dependencies: {args.dependencies}")
        self.logger.info(f"Partition Info Table: {args.partition_info_table}")
        self.logger.info(f"Environment: {args.env}")
        self.logger.info(f"Recon Table: {args.recon_table}")
        self.logger.info(f"Partition DT: {partition_dt}")
    
    def _log_final_results(self, results: Dict, start_time: datetime) -> None:
        """Log final pipeline results"""
        execution_time = datetime.now() - start_time
        
        self.logger.info("="*40)
        self.logger.info("PIPELINE EXECUTION SUMMARY")
        self.logger.info("="*40)
        
        self.logger.info(f"Total execution time: {execution_time}")
        self.logger.info(f"Total records processed: {results.get('total_records', 0)}")
        self.logger.info(f"Successful metrics: {results.get('successful_count', 0)}")
        self.logger.info(f"Failed metrics: {results.get('failed_count', 0)}")
        
        # Log write statistics
        successful_writes = results.get('successful_writes', {})
        failed_writes = results.get('failed_writes', {})
        
        total_successful = sum(len(metrics) for metrics in successful_writes.values())
        total_failed = sum(len(metrics) for metrics in failed_writes.values())
        
        self.logger.info(f"Successfully written metrics: {total_successful}")
        self.logger.info(f"Failed to write metrics: {total_failed}")
        
        # Log recon statistics
        recon_count = results.get('recon_records_count', 0)
        self.logger.info(f"Reconciliation records created: {recon_count}")
        
        # Log BigQuery writer statistics
        if self.components.get('bq_writer'):
            writer_stats = self.components['bq_writer'].get_write_statistics()
            self.logger.info(f"Writer statistics: {writer_stats}")
    
    def _attempt_rollback(self, partition_dt: str) -> None:
        """Attempt to rollback processed metrics"""
        try:
            self.logger.info("Attempting rollback of processed metrics")
            self.components['bq_writer'].rollback_processed_metrics(
                partition_dt, 
                self.config.rollback_timeout_hours
            )
        except Exception as rollback_error:
            self.logger.error(f"Rollback failed: {str(rollback_error)}")


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Optimized PySpark BigQuery Metrics Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --gcs_path gs://bucket/metrics.json --run_date 2024-01-01 
           --dependencies dep1,dep2 --partition_info_table project.dataset.partition_info
           --env PRD --recon_table project.dataset.recon
        """
    )
    
    parser.add_argument('--gcs_path', required=True, help='GCS path to JSON input file')
    parser.add_argument('--run_date', required=True, help='Run date in YYYY-MM-DD format')
    parser.add_argument('--dependencies', required=True, help='Comma-separated list of dependencies')
    parser.add_argument('--partition_info_table', required=True, help='BigQuery partition info table')
    parser.add_argument('--env', required=True, help='Environment name (e.g., BLD, PRD, DEV)')
    parser.add_argument('--recon_table', required=True, help='BigQuery reconciliation table')
    parser.add_argument('--config', help='Path to custom configuration file (optional)')
    
    return parser.parse_args()


def main():
    """Main entry point for the optimized pipeline"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Validate date format
        validate_date_format(args.run_date)
        
        # Load configuration
        config = get_default_config()
        if args.config:
            # Load custom configuration if provided
            # This would be implemented based on your config file format
            pass
        
        # Create and run pipeline
        pipeline = OptimizedMetricsPipeline(config)
        pipeline.run(args)
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()