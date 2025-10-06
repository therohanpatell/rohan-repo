# dq_main.py
"""
Main application entry point for the Data Quality (DQ) Validation Framework
"""

import argparse
import sys
from datetime import datetime

from config import setup_logging
from exceptions import MetricsPipelineError, ValidationError, GCSError, BigQueryError
from utils import managed_spark_session, DateUtils
from bigquery import create_bigquery_operations
from dq_pipeline import create_dq_validation_pipeline

logger = setup_logging()


class DQOrchestrator:
    """Orchestrates the DQ validation pipeline execution with comprehensive error handling"""
    
    def __init__(self):
        self.pipeline = None
    
    def parse_arguments(self) -> argparse.Namespace:
        """Parse command line arguments for DQ pipeline with validation"""
        parser = argparse.ArgumentParser(
            description='PySpark BigQuery Data Quality Validation Framework'
        )
        
        parser.add_argument(
            '--gcs_path', 
            required=True, 
            help='GCS path to DQ JSON configuration file'
        )
        parser.add_argument(
            '--run_date', 
            required=True, 
            help='Run date in YYYY-MM-DD format'
        )
        parser.add_argument(
            '--results_table', 
            required=True, 
            help='BigQuery table for DQ results (project.dataset.table)'
        )
        parser.add_argument(
            '--env', 
            required=True, 
            help='Environment name (e.g., BLD, PRD, DEV)'
        )
        
        return parser.parse_args()
    
    def validate_arguments(self, args: argparse.Namespace) -> None:
        """Validate command line arguments"""
        logger.info("Validating command line arguments...")
        
        # Validate run_date format
        try:
            DateUtils.validate_date_format(args.run_date)
        except ValidationError as e:
            raise ValidationError(f"Invalid run_date format: {str(e)}")
        
        # Validate results_table format
        if not args.results_table or len(args.results_table.split('.')) != 3:
            raise ValidationError(f"Invalid results_table format: {args.results_table}. Must be in format 'project.dataset.table'")
        
        # Validate environment
        valid_envs = ['BLD', 'PRD', 'DEV', 'TEST', 'STG']
        if args.env not in valid_envs:
            raise ValidationError(f"Invalid environment: {args.env}. Must be one of {valid_envs}")
        
        logger.info("All command line arguments validated successfully")

    def log_dq_configuration(self, args: argparse.Namespace) -> None:
        """Log DQ pipeline configuration"""
        logger.info("DATA QUALITY VALIDATION FRAMEWORK CONFIGURATION:")
        logger.info("-"*60)
        logger.info(f"GCS Path: {args.gcs_path}")
        logger.info(f"Run Date: {args.run_date}")
        logger.info(f"Results Table: {args.results_table}")
        logger.info(f"Environment: {args.env}")
        logger.info(f"Execution ID: {getattr(self.pipeline, 'execution_id', 'N/A')}")
        logger.info("-"*60)
        logger.info("DQ FRAMEWORK BEHAVIOR:")
        logger.info("   Executes all active DQ checks defined in JSON configuration")
        logger.info("   Supports comparison types: numeric_condition, set_match, not_in_result, row_match")
        logger.info("   Continues execution even if individual checks fail")
        logger.info("   Logs detailed results to specified BigQuery table")
        logger.info("   Comprehensive error handling and validation")
        logger.info("-"*60)

    def execute_dq_pipeline(self, args: argparse.Namespace) -> None:
        """Execute the main DQ pipeline steps with comprehensive error handling"""
        logger.info("="*80)
        logger.info("STARTING DATA QUALITY VALIDATION FRAMEWORK EXECUTION")
        logger.info("="*80)
        
        json_data = None
        validated_data = None
        
        with managed_spark_session("DQValidationFramework") as spark:
            try:
                # Initialize BigQuery operations and DQ pipeline
                logger.info("Initializing BigQuery operations and DQ pipeline...")
                bq_operations = create_bigquery_operations(spark)
                self.pipeline = create_dq_validation_pipeline(spark, bq_operations)
                logger.info("DQ pipeline initialization completed")
                
                # Step 1: Read JSON from GCS
                logger.info("-"*60)
                logger.info("STEP 1: READING DQ JSON FROM GCS")
                logger.info("-"*60)
                logger.info(f"Reading DQ JSON from GCS path: {args.gcs_path}")
                json_data = self.pipeline.bq_operations.read_json_from_gcs(args.gcs_path)
                logger.info(f"Successfully read {len(json_data)} DQ rules from JSON")
                
                # Step 2: Validate DQ JSON structure
                logger.info("-"*60)
                logger.info("STEP 2: VALIDATING DQ JSON STRUCTURE")
                logger.info("-"*60)
                logger.info(f"Validating {len(json_data)} DQ rules...")
                validated_data = self.pipeline.validate_dq_json_structure(json_data)
                logger.info(f"Successfully validated {len(validated_data)} DQ rules")
                
                # Step 3: Process DQ checks
                logger.info("-"*60)
                logger.info("STEP 3: PROCESSING DQ CHECKS")
                logger.info("-"*60)
                logger.info(f"Processing DQ checks for run date: {args.run_date}")
                dq_results = self.pipeline.process_dq_checks(validated_data, args.run_date)
                logger.info(f"DQ checks processing completed: {len(dq_results)} results generated")
                
                # Step 4: Write DQ results to BigQuery WITH OVERWRITE
                logger.info("-"*60)
                logger.info("STEP 4: WRITING DQ RESULTS TO BIGQUERY WITH OVERWRITE")
                logger.info("-"*60)
                logger.info(f"Writing {len(dq_results)} DQ results to table: {args.results_table}")
                logger.info("OVERWRITE BEHAVIOR: Existing results for the same run_date will be replaced")
                self.pipeline.write_dq_results_to_bq(dq_results, args.results_table, args.run_date)
                logger.info("DQ results successfully written to BigQuery with overwrite")
                
                # Log final statistics
                self.log_dq_statistics(dq_results)
                
            except Exception as pipeline_error:
                logger.error("="*80)
                logger.error("DQ PIPELINE EXECUTION FAILED")
                logger.error("="*80)
                logger.error(f"Error: {str(pipeline_error)}")
                logger.error(f"Error type: {type(pipeline_error).__name__}")
                
                # Attempt to write partial results if we have any
                if hasattr(self.pipeline, 'processed_checks') and self.pipeline.processed_checks:
                    try:
                        logger.info("Attempting to write partial results...")
                        # This would need to be implemented based on what data we have
                        logger.info(f"Processed {len(self.pipeline.processed_checks)} checks before failure")
                    except Exception as partial_error:
                        logger.error(f"Failed to write partial results: {str(partial_error)}")
                
                raise pipeline_error
    
    def log_dq_statistics(self, dq_results: List[Dict]) -> None:
        """Log comprehensive DQ pipeline statistics"""
        logger.info("="*80)
        logger.info("DATA QUALITY VALIDATION FRAMEWORK EXECUTION COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        
        if not dq_results:
            logger.warning("No DQ results to analyze")
            return
        
        # Calculate comprehensive statistics
        total_checks = len(dq_results)
        passed_checks = sum(1 for r in dq_results if r.get('validation_status') == 'PASS')
        failed_checks = sum(1 for r in dq_results if r.get('validation_status') == 'FAIL')
        error_checks = sum(1 for r in dq_results if r.get('execution_status') == 'ERROR')
        skipped_checks = sum(1 for r in dq_results if r.get('execution_status') == 'SKIPPED')
        
        # Calculate execution times
        execution_times = []
        for result in dq_results:
            duration_str = result.get('execution_duration', '0s')
            try:
                # Parse duration string like "1m 30s"
                total_seconds = 0
                for part in duration_str.split():
                    if part.endswith('h'):
                        total_seconds += int(part[:-1]) * 3600
                    elif part.endswith('m'):
                        total_seconds += int(part[:-1]) * 60
                    elif part.endswith('s'):
                        total_seconds += int(part[:-1])
                execution_times.append(total_seconds)
            except:
                execution_times.append(0)
        
        avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
        max_execution_time = max(execution_times) if execution_times else 0
        
        # Categorize by severity
        high_severity = [r for r in dq_results if r.get('severity') == 'High']
        medium_severity = [r for r in dq_results if r.get('severity') == 'Medium']
        low_severity = [r for r in dq_results if r.get('severity') == 'Low']
        
        # Categorize by comparison type
        by_comparison = {}
        for result in dq_results:
            comp_type = result.get('comparison_type', 'unknown')
            if comp_type not in by_comparison:
                by_comparison[comp_type] = []
            by_comparison[comp_type].append(result)
        
        # Categorize by execution status
        by_status = {}
        for result in dq_results:
            status = result.get('execution_status', 'unknown')
            if status not in by_status:
                by_status[status] = []
            by_status[status].append(result)
        
        logger.info("DQ VALIDATION STATISTICS:")
        logger.info("-"*50)
        logger.info(f"Total DQ Checks Executed: {total_checks}")
        logger.info(f"Passed Validations: {passed_checks}")
        logger.info(f"Failed Validations: {failed_checks}")
        logger.info(f"Execution Errors: {error_checks}")
        logger.info(f"Skipped Checks: {skipped_checks}")
        
        if total_checks > 0:
            success_rate = (passed_checks / total_checks) * 100
            logger.info(f"Overall Success Rate: {success_rate:.1f}%")
        
        logger.info(f"Average Execution Time: {avg_execution_time:.2f}s")
        logger.info(f"Maximum Execution Time: {max_execution_time:.2f}s")
        
        logger.info("-"*50)
        logger.info("SEVERITY BREAKDOWN:")
        logger.info(f"  High Severity: {len(high_severity)} checks")
        logger.info(f"  Medium Severity: {len(medium_severity)} checks")
        logger.info(f"  Low Severity: {len(low_severity)} checks")
        
        logger.info("-"*50)
        logger.info("COMPARISON TYPE BREAKDOWN:")
        for comp_type, results in by_comparison.items():
            passed = sum(1 for r in results if r.get('validation_status') == 'PASS')
            total = len(results)
            pass_rate = (passed / total * 100) if total > 0 else 0
            logger.info(f"  {comp_type}: {total} checks ({pass_rate:.1f}% pass rate)")
        
        logger.info("-"*50)
        logger.info("EXECUTION STATUS BREAKDOWN:")
        for status, results in by_status.items():
            logger.info(f"  {status}: {len(results)} checks")
        
        # Log failed high severity checks for attention
        failed_high_severity = [r for r in dq_results 
                              if r.get('validation_status') == 'FAIL' 
                              and r.get('severity') == 'High'
                              and r.get('execution_status') == 'SUCCESS']
        
        if failed_high_severity:
            logger.warning("-"*50)
            logger.warning("HIGH SEVERITY FAILURES (Requires Immediate Attention):")
            for i, failed in enumerate(failed_high_severity[:5], 1):
                check_id = failed.get('check_id', 'UNKNOWN')
                description = failed.get('description', 'No description')
                logger.warning(f"  {i}. {check_id}: {description}")
            if len(failed_high_severity) > 5:
                logger.warning(f"  ... and {len(failed_high_severity) - 5} more high severity failures")
        
        # Log execution errors for investigation
        execution_errors = [r for r in dq_results if r.get('execution_status') == 'ERROR']
        if execution_errors:
            logger.error("-"*50)
            logger.error("EXECUTION ERRORS (Requires Investigation):")
            for i, error in enumerate(execution_errors[:3], 1):
                check_id = error.get('check_id', 'UNKNOWN')
                error_msg = error.get('error_message', 'No error message')
                logger.error(f"  {i}. {check_id}: {error_msg[:100]}...")
            if len(execution_errors) > 3:
                logger.error(f"  ... and {len(execution_errors) - 3} more execution errors")
        
        logger.info("="*80)
        logger.info("DQ FRAMEWORK EXECUTION SUMMARY COMPLETED")


def main():
    """Main DQ application entry point with comprehensive error handling"""
    print("="*80)
    print("DATA QUALITY VALIDATION FRAMEWORK STARTING")
    print("="*80)
    
    orchestrator = DQOrchestrator()
    exit_code = 0
    
    try:
        logger.info("Initializing DQ orchestrator...")
        
        # Parse and validate arguments
        logger.info("Parsing command line arguments...")
        args = orchestrator.parse_arguments()
        logger.info("Command line arguments parsed successfully")
        
        logger.info("Validating command line arguments...")
        orchestrator.validate_arguments(args)
        logger.info("Command line arguments validated successfully")
        
        # Log configuration
        logger.info("Logging DQ configuration...")
        orchestrator.log_dq_configuration(args)
        
        # Execute pipeline
        logger.info("Starting DQ pipeline execution...")
        orchestrator.execute_dq_pipeline(args)
        
        print("="*80)
        print("DATA QUALITY VALIDATION FRAMEWORK COMPLETED SUCCESSFULLY")
        print("="*80)
        
    except ValidationError as e:
        exit_code = 2
        print("="*80)
        print("DQ FRAMEWORK FAILED - VALIDATION ERROR")
        print("="*80)
        logger.error(f"Validation error: {str(e)}")
        print(f"Validation Error: {str(e)}")
        
    except (GCSError, BigQueryError) as e:
        exit_code = 3
        print("="*80)
        print("DQ FRAMEWORK FAILED - INFRASTRUCTURE ERROR")
        print("="*80)
        logger.error(f"Infrastructure error: {str(e)}")
        print(f"Infrastructure Error: {str(e)}")
        
    except MetricsPipelineError as e:
        exit_code = 1
        print("="*80)
        print("DQ FRAMEWORK FAILED - PIPELINE ERROR")
        print("="*80)
        logger.error(f"DQ framework error: {str(e)}")
        print(f"Pipeline Error: {str(e)}")
        
    except Exception as e:
        exit_code = 99
        print("="*80)
        print("DQ FRAMEWORK FAILED - UNEXPECTED ERROR")
        print("="*80)
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        print(f"Unexpected Error: {str(e)}")
        print("Check logs for detailed error information")
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()