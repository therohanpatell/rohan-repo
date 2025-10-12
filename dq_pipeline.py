"""
DQ Pipeline module for data quality check execution and validation
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame

from config import PipelineConfig, setup_logging
from exceptions import ValidationError, SQLExecutionError, BigQueryError
from validation import ValidationEngine
from comparison import ComparisonEngine
from utils import DateUtils, ResultSerializer
from bigquery import BigQueryOperations

logger = setup_logging()


class DQPipeline:
    """Main DQ pipeline for executing and validating checks"""
    
    def __init__(self, spark: SparkSession, bq_operations: BigQueryOperations):
        """
        Initialize DQ pipeline
        
        Args:
            spark: SparkSession instance
            bq_operations: BigQueryOperations instance
        """
        self.spark = spark
        self.bq_operations = bq_operations
        logger.info("DQ Pipeline initialized")
    
    def read_and_validate_dq_config(self, gcs_path: str) -> List[Dict]:
        """
        Read and validate DQ configuration from GCS
        
        Args:
            gcs_path: GCS path to JSON config
            
        Returns:
            Validated DQ check configurations
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            logger.info(f"Reading DQ configuration from GCS: {gcs_path}")
            
            # Read JSON from GCS using existing method
            json_data = self._read_json_from_gcs(gcs_path)
            
            logger.info(f"Successfully read {len(json_data)} DQ check configurations")
            
            # Validate DQ configuration using ValidationEngine
            logger.info("Validating DQ configuration...")
            validated_data = ValidationEngine.validate_dq_json(json_data)
            
            logger.info("DQ configuration validation completed successfully")
            return validated_data
            
        except ValidationError as e:
            logger.error(f"DQ configuration validation failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to read and validate DQ config: {str(e)}")
            raise ValidationError(f"Failed to read and validate DQ config: {str(e)}")
    
    def execute_dq_checks(self, dq_config: List[Dict], run_date: str) -> List[Dict]:
        """
        Execute DQ checks and perform validation
        
        Args:
            dq_config: DQ check configurations
            run_date: Business date for the run
            
        Returns:
            List of DQ result records
        """
        logger.info("=" * 80)
        logger.info("STARTING DQ CHECKS EXECUTION")
        logger.info("=" * 80)
        logger.info(f"Total checks in configuration: {len(dq_config)}")
        logger.info(f"Run date: {run_date}")
        
        # Filter active checks
        active_checks = [check for check in dq_config if check.get('active', False)]
        inactive_checks = [check for check in dq_config if not check.get('active', False)]
        
        logger.info(f"Active checks to execute: {len(active_checks)}")
        logger.info(f"Inactive checks (skipped): {len(inactive_checks)}")
        
        if inactive_checks:
            logger.info("\nSkipped checks (active=false):")
            for check in inactive_checks:
                logger.info(f"  - {check['check_id']}: {check.get('description', 'No description')}")
        
        # Execute each active check
        dq_results = []
        passed_count = 0
        failed_count = 0
        passed_checks = []
        failed_checks = []
        total_execution_time = 0.0
        
        logger.info("\n" + "=" * 80)
        logger.info("EXECUTING ACTIVE CHECKS")
        logger.info("=" * 80)
        
        for i, check_config in enumerate(active_checks, 1):
            check_id = check_config['check_id']
            logger.info(f"\n[{i}/{len(active_checks)}] Processing check: {check_id}")
            logger.info(f"  Category: {check_config.get('category', 'N/A')}")
            logger.info(f"  Description: {check_config.get('description', 'N/A')}")
            logger.info(f"  Severity: {check_config.get('severity', 'N/A')}")
            
            try:
                # Execute single check
                result = self.execute_single_check(check_config, run_date)
                dq_results.append(result)
                
                # Track execution time
                total_execution_time += result.get('execution_duration', 0.0)
                
                # Track pass/fail counts and details
                if result['validation_status'] == 'PASS':
                    passed_count += 1
                    passed_checks.append({
                        'check_id': check_id,
                        'description': check_config.get('description', 'N/A'),
                        'severity': check_config.get('severity', 'N/A'),
                        'duration': result.get('execution_duration', 0.0)
                    })
                else:
                    failed_count += 1
                    failed_checks.append({
                        'check_id': check_id,
                        'description': check_config.get('description', 'N/A'),
                        'severity': check_config.get('severity', 'N/A'),
                        'error': result.get('error_message', 'Unknown reason'),
                        'duration': result.get('execution_duration', 0.0)
                    })
                
            except Exception as e:
                logger.error(f"  ✗ UNEXPECTED ERROR: {str(e)}")
                # Continue with other checks even if one fails
                failed_count += 1
                failed_checks.append({
                    'check_id': check_id,
                    'description': check_config.get('description', 'N/A'),
                    'severity': check_config.get('severity', 'N/A'),
                    'error': str(e),
                    'duration': 0.0
                })
        
        # Log comprehensive summary
        logger.info("\n" + "=" * 80)
        logger.info("DQ CHECKS EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total checks in configuration: {len(dq_config)}")
        logger.info(f"Active checks processed: {len(active_checks)}")
        logger.info(f"Passed: {passed_count}")
        logger.info(f"Failed: {failed_count}")
        logger.info(f"Skipped (inactive): {len(inactive_checks)}")
        logger.info(f"Success rate: {(passed_count / len(active_checks) * 100) if active_checks else 0:.1f}%")
        logger.info(f"Total execution time: {total_execution_time:.2f} seconds")
        logger.info(f"Average execution time per check: {(total_execution_time / len(active_checks)) if active_checks else 0:.2f} seconds")
        
        # Log passed checks summary
        if passed_checks:
            logger.info("\n" + "-" * 80)
            logger.info(f"PASSED CHECKS ({passed_count}):")
            logger.info("-" * 80)
            for check in passed_checks:
                logger.info(f"  ✓ {check['check_id']}")
                logger.info(f"    Description: {check['description']}")
                logger.info(f"    Severity: {check['severity']}")
                logger.info(f"    Duration: {check['duration']:.2f}s")
        
        # Log failed checks summary
        if failed_checks:
            logger.warning("\n" + "-" * 80)
            logger.warning(f"FAILED CHECKS ({failed_count}):")
            logger.warning("-" * 80)
            for check in failed_checks:
                logger.warning(f"  ✗ {check['check_id']}")
                logger.warning(f"    Description: {check['description']}")
                logger.warning(f"    Severity: {check['severity']}")
                logger.warning(f"    Error: {check['error']}")
                logger.warning(f"    Duration: {check['duration']:.2f}s")
        
        logger.info("\n" + "=" * 80)
        
        return dq_results
    
    def execute_single_check(self, check_config: Dict, run_date: str) -> Dict:
        """
        Execute a single DQ check
        
        Args:
            check_config: DQ check configuration
            run_date: Business date
            
        Returns:
            DQ result record
        """
        check_id = check_config['check_id']
        description = check_config.get('description', 'No description')
        sql_query = check_config['sql_query']
        expected_output = check_config['expected_output']
        comparison_type = check_config['comparison_type']
        severity = check_config.get('severity', 'N/A')
        
        # Start timing
        start_time = time.time()
        
        actual_result = None
        validation_status = "FAIL"
        failure_reason = ""
        error_message = None
        
        try:
            # Execute SQL query
            logger.info(f"  Executing SQL query...")
            logger.debug(f"  SQL: {sql_query}")
            
            actual_result = self._execute_dq_sql(sql_query, check_id)
            
            logger.info(f"  SQL execution successful")
            logger.debug(f"  Actual result: {actual_result}")
            
            # Perform validation comparison
            logger.info(f"  Performing validation (comparison_type: {comparison_type})...")
            logger.debug(f"  Expected output: {expected_output}")
            
            validation_status, failure_reason = ComparisonEngine.compare(
                actual_result, 
                expected_output, 
                comparison_type
            )
            
            # Stop timing
            execution_duration = time.time() - start_time
            
            # Log success or failure with detailed information
            if validation_status == "PASS":
                # Success logging for passing check
                logger.info(f"  ✓ CHECK PASSED")
                logger.info(f"  Check ID: {check_id}")
                logger.info(f"  Description: {description}")
                logger.info(f"  Severity: {severity}")
                logger.info(f"  Execution duration: {execution_duration:.2f} seconds")
            else:
                # Failure logging with expected vs actual values
                logger.warning(f"  ✗ CHECK FAILED")
                logger.warning(f"  Check ID: {check_id}")
                logger.warning(f"  Description: {description}")
                logger.warning(f"  Severity: {severity}")
                logger.warning(f"  Comparison Type: {comparison_type}")
                logger.warning(f"  Expected Output: {expected_output}")
                logger.warning(f"  Actual Result: {actual_result}")
                logger.warning(f"  Failure Reason: {failure_reason}")
                logger.warning(f"  Execution duration: {execution_duration:.2f} seconds")
                error_message = failure_reason
            
        except SQLExecutionError as e:
            # Detailed error logging for SQL execution failures
            execution_duration = time.time() - start_time
            error_message = f"SQL execution failed: {str(e)}"
            logger.error(f"  ✗ SQL EXECUTION ERROR")
            logger.error(f"  Check ID: {check_id}")
            logger.error(f"  Description: {description}")
            logger.error(f"  Severity: {severity}")
            logger.error(f"  Error: {error_message}")
            logger.error(f"  SQL Query: {sql_query}")
            logger.error(f"  Execution duration: {execution_duration:.2f} seconds")
            validation_status = "FAIL"
            failure_reason = error_message
            
        except ValidationError as e:
            # Detailed error logging for validation failures
            execution_duration = time.time() - start_time
            error_message = f"Validation failed: {str(e)}"
            logger.error(f"  ✗ VALIDATION ERROR")
            logger.error(f"  Check ID: {check_id}")
            logger.error(f"  Description: {description}")
            logger.error(f"  Severity: {severity}")
            logger.error(f"  Comparison Type: {comparison_type}")
            logger.error(f"  Expected Output: {expected_output}")
            logger.error(f"  Actual Result: {actual_result}")
            logger.error(f"  Error: {error_message}")
            logger.error(f"  Execution duration: {execution_duration:.2f} seconds")
            validation_status = "FAIL"
            failure_reason = error_message
            
        except Exception as e:
            # Detailed error logging for comparison failures and other errors
            execution_duration = time.time() - start_time
            error_message = f"Check execution failed: {str(e)}"
            logger.error(f"  ✗ CHECK EXECUTION ERROR")
            logger.error(f"  Check ID: {check_id}")
            logger.error(f"  Description: {description}")
            logger.error(f"  Severity: {severity}")
            logger.error(f"  Comparison Type: {comparison_type}")
            logger.error(f"  Expected Output: {expected_output}")
            logger.error(f"  Actual Result: {actual_result}")
            logger.error(f"  Error: {error_message}")
            logger.error(f"  Execution duration: {execution_duration:.2f} seconds")
            validation_status = "FAIL"
            failure_reason = error_message
        
        # Build result record
        result_record = self.build_dq_result_record(
            check_config=check_config,
            actual_result=actual_result,
            validation_status=validation_status,
            failure_reason=failure_reason,
            execution_duration=execution_duration,
            run_date=run_date
        )
        
        return result_record
    
    def build_dq_result_record(
        self, 
        check_config: Dict, 
        actual_result: any,
        validation_status: str, 
        failure_reason: str,
        execution_duration: float, 
        run_date: str
    ) -> Dict:
        """
        Build DQ result record for storage
        
        Args:
            check_config: Original check configuration
            actual_result: Query result
            validation_status: PASS or FAIL
            failure_reason: Explanation if FAIL
            execution_duration: Execution time in seconds
            run_date: Business date
            
        Returns:
            DQ result record
        """
        # Serialize expected_output and actual_result for storage
        expected_output_serialized = ResultSerializer.serialize_result(
            check_config['expected_output']
        )
        actual_result_serialized = ResultSerializer.serialize_result(actual_result)
        
        # Get current timestamp and partition date
        execution_timestamp = DateUtils.get_current_timestamp()
        partition_dt = DateUtils.get_current_partition_dt()
        
        # Build result record
        result_record = {
            'check_id': check_config['check_id'],
            'category': check_config['category'],
            'description': check_config.get('description'),
            'severity': check_config['severity'],
            'sql_query': check_config['sql_query'],
            'expected_output': expected_output_serialized,
            'actual_result': actual_result_serialized,
            'comparison_type': check_config['comparison_type'],
            'validation_status': validation_status,
            'impacted_downstream': check_config.get('impacted_downstream', []),
            'tags': check_config.get('tags', []),
            'execution_timestamp': execution_timestamp,
            'error_message': failure_reason if validation_status == 'FAIL' else None,
            'execution_duration': execution_duration,
            'run_dt': run_date,
            'partition_dt': partition_dt
        }
        
        return result_record
    
    def write_dq_results(self, dq_results: List[Dict], dq_target_table: str) -> None:
        """
        Write DQ results to BigQuery table
        
        Args:
            dq_results: List of DQ result records
            dq_target_table: Target BigQuery table (from command-line args)
            
        Raises:
            BigQueryError: If write operation fails
        """
        try:
            if not dq_results:
                logger.warning("No DQ results to write")
                return
            
            logger.info("=" * 80)
            logger.info("WRITING DQ RESULTS TO BIGQUERY")
            logger.info("=" * 80)
            logger.info(f"Target table: {dq_target_table}")
            logger.info(f"Number of results: {len(dq_results)}")
            
            # Create Spark DataFrame from results
            logger.info("Creating Spark DataFrame from DQ results...")
            dq_df = self.spark.createDataFrame(dq_results, PipelineConfig.DQ_RESULTS_SCHEMA)
            
            logger.info("DQ Results DataFrame Schema:")
            dq_df.printSchema()
            
            logger.info("Sample DQ Results (first 5 records):")
            dq_df.show(5, truncate=False)
            
            # Write to BigQuery using existing write method
            logger.info(f"Writing DataFrame to BigQuery table: {dq_target_table}")
            self.bq_operations.write_dataframe_to_table(
                df=dq_df,
                target_table=dq_target_table,
                write_mode="append"
            )
            
            logger.info("=" * 80)
            logger.info("DQ RESULTS WRITE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Failed to write DQ results: {str(e)}")
            raise BigQueryError(f"Failed to write DQ results: {str(e)}")
    
    # Private helper methods
    
    def _read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """
        Read JSON file from GCS (reuses existing pattern)
        
        Args:
            gcs_path: GCS path to JSON file
            
        Returns:
            List of dictionaries from JSON
        """
        try:
            # Validate GCS path format
            if not gcs_path.startswith('gs://'):
                raise ValidationError(
                    f"Invalid GCS path format: {gcs_path}. Must start with 'gs://'"
                )
            
            logger.info(f"Reading JSON from GCS: {gcs_path}")
            
            # Read as text first to avoid Spark schema inference issues
            text_df = self.spark.read.text(gcs_path)
            json_text = '\n'.join([row.value for row in text_df.collect()])
            
            # Parse JSON using Python's json module for accurate type preservation
            import json
            json_data = json.loads(json_text)
            
            # Ensure it's a list
            if not isinstance(json_data, list):
                raise ValidationError(
                    f"JSON file must contain an array at the root level: {gcs_path}"
                )
            
            if len(json_data) == 0:
                raise ValidationError(
                    f"No data found in JSON file: {gcs_path}"
                )
            
            logger.info(f"Successfully read {len(json_data)} records from JSON")
            return json_data
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {gcs_path}: {str(e)}")
            raise ValidationError(f"Invalid JSON format in {gcs_path}: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to read JSON from GCS: {str(e)}")
            raise ValidationError(f"Failed to read JSON from GCS: {str(e)}")
    
    def _execute_dq_sql(self, sql: str, check_id: str) -> any:
        """
        Execute DQ SQL query and return results
        
        Args:
            sql: SQL query to execute
            check_id: Check ID for error tracking
            
        Returns:
            Query results (format depends on query)
            
        Raises:
            SQLExecutionError: If SQL execution fails
        """
        try:
            logger.debug(f"Executing SQL for check {check_id}")
            
            # Execute query using BigQuery operations
            query_job = self.bq_operations.bq_client.query(sql)
            results = query_job.result(timeout=PipelineConfig.QUERY_TIMEOUT)
            
            # Convert results to list of dictionaries
            result_rows = []
            for row in results:
                result_rows.append(dict(row))
            
            logger.debug(f"Query returned {len(result_rows)} rows")
            
            # Return results in appropriate format
            if len(result_rows) == 0:
                return []
            elif len(result_rows) == 1 and len(result_rows[0]) == 1:
                # Single value result - return the value directly
                return list(result_rows[0].values())[0]
            else:
                # Multiple rows or columns - return as list of dicts
                return result_rows
            
        except Exception as e:
            error_msg = f"SQL execution failed for check {check_id}: {str(e)}"
            logger.error(error_msg)
            raise SQLExecutionError(error_msg, check_id)
