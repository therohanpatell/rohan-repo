# dq_pipeline.py
"""
Data Quality (DQ) Validation Pipeline
Extends the existing metrics framework for comprehensive data quality validation
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Any
from decimal import Decimal
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, ArrayType
import json
import time
import re

from config import PipelineConfig, setup_logging
from exceptions import MetricsPipelineError, ValidationError, SQLExecutionError, BigQueryError, GCSError
from utils import DateUtils, NumericUtils, StringUtils, ExecutionUtils

logger = setup_logging()


class DQValidationPipeline:
    """Data Quality Validation Pipeline extending the metrics framework"""

    def __init__(self, spark: SparkSession, bq_operations):
        self.spark = spark
        self.bq_operations = bq_operations
        self.execution_id = ExecutionUtils.generate_execution_id()
        self.processed_checks = []
        
        # DQ-specific configuration
        self.DQ_RESULTS_SCHEMA = self._get_dq_results_schema()

    def _get_dq_results_schema(self):
        """Define BigQuery schema for DQ results with proper array types"""
        return StructType([
            StructField("check_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("sql_query", StringType(), True),
            StructField("description", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("execution_status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("actual_output", ArrayType(StringType()), True),  # REPEATED field in BQ
            StructField("expected_output", ArrayType(StringType()), True),  # REPEATED field in BQ
            StructField("comparison_type", StringType(), True),
            StructField("validation_status", StringType(), True),
            StructField("impacted_downstream", ArrayType(StringType()), True),  # REPEATED field in BQ
            StructField("tags", ArrayType(StringType()), True),  # REPEATED field in BQ
            StructField("execution_duration", StringType(), True),
            StructField("execution_timestamp", TimestampType(), True),
            StructField("run_date", DateType(), True)
        ])

    def validate_dq_json_structure(self, json_data: List[Dict]) -> List[Dict]:
        """
        Validate DQ JSON structure against required fields with comprehensive checks
        
        Args:
            json_data: List of DQ rule definitions
            
        Returns:
            Validated JSON data
            
        Raises:
            ValidationError: If validation fails
        """
        logger.info("Validating DQ JSON structure")
        
        if not json_data:
            raise ValidationError("DQ JSON data is empty or None")
        
        if not isinstance(json_data, list):
            raise ValidationError("DQ JSON data must be a list of rules")
        
        required_fields = [
            'check_id', 'category', 'sql_query', 'description', 
            'severity', 'expected_output', 'comparison_type',
            'impacted_downstream', 'tags', 'active'
        ]
        
        check_ids = set()
        
        for i, rule in enumerate(json_data):
            if not isinstance(rule, dict):
                raise ValidationError(f"Rule {i} is not a valid dictionary")
            
            # Check required fields
            for field in required_fields:
                if field not in rule:
                    raise ValidationError(f"Rule {i}: Missing required field '{field}'")
                
                value = rule[field]
                if value is None:
                    raise ValidationError(f"Rule {i}: Field '{field}' cannot be null")
                
                if isinstance(value, str) and value.strip() == "":
                    raise ValidationError(f"Rule {i}: Field '{field}' cannot be empty string")
            
            # Validate check_id uniqueness and format
            check_id = str(rule['check_id']).strip()
            if not check_id:
                raise ValidationError(f"Rule {i}: check_id cannot be empty")
            
            if check_id in check_ids:
                raise ValidationError(f"Rule {i}: Duplicate check_id '{check_id}' found")
            check_ids.add(check_id)
            
            # Validate SQL query
            sql_query = rule['sql_query']
            if not sql_query or not isinstance(sql_query, str) or sql_query.strip() == "":
                raise ValidationError(f"Rule {i}: sql_query cannot be empty")
            
            # Validate comparison_type
            valid_comparisons = ['numeric_condition', 'set_match', 'not_in_result', 'row_match']
            comparison_type = rule['comparison_type']
            if comparison_type not in valid_comparisons:
                raise ValidationError(f"Rule {i}: Invalid comparison_type '{comparison_type}'. Must be one of {valid_comparisons}")
            
            # Validate severity
            valid_severities = ['High', 'Medium', 'Low']
            severity = rule['severity']
            if severity not in valid_severities:
                raise ValidationError(f"Rule {i}: Invalid severity '{severity}'. Must be one of {valid_severities}")
            
            # Validate active flag
            active = rule['active']
            if not isinstance(active, bool):
                raise ValidationError(f"Rule {i}: 'active' must be a boolean")
            
            # Validate arrays
            if not isinstance(rule['impacted_downstream'], list):
                raise ValidationError(f"Rule {i}: 'impacted_downstream' must be a list")
            
            if not isinstance(rule['tags'], list):
                raise ValidationError(f"Rule {i}: 'tags' must be a list")
            
            # Validate expected_output based on comparison_type
            self._validate_expected_output(rule['expected_output'], comparison_type, i)
        
        logger.info(f"Successfully validated {len(json_data)} DQ rules with {len(check_ids)} unique check IDs")
        return json_data

    def _validate_expected_output(self, expected_output: Any, comparison_type: str, rule_index: int) -> None:
        """
        Validate expected_output based on comparison type
        
        Args:
            expected_output: The expected output value to validate
            comparison_type: Type of comparison
            rule_index: Rule index for error reporting
            
        Raises:
            ValidationError: If validation fails
        """
        if comparison_type == 'numeric_condition':
            if not isinstance(expected_output, (str, int, float)):
                raise ValidationError(f"Rule {rule_index}: For numeric_condition, expected_output must be string, int, or float")
            
            # Validate numeric condition format
            if isinstance(expected_output, str):
                self._validate_numeric_condition_format(expected_output, rule_index)
                
        elif comparison_type in ['set_match', 'not_in_result']:
            if not isinstance(expected_output, list):
                raise ValidationError(f"Rule {rule_index}: For {comparison_type}, expected_output must be a list")
            
            if not expected_output:
                raise ValidationError(f"Rule {rule_index}: For {comparison_type}, expected_output list cannot be empty")
                
        elif comparison_type == 'row_match':
            if not isinstance(expected_output, list):
                raise ValidationError(f"Rule {rule_index}: For row_match, expected_output must be a list of rows")
            
            if not expected_output:
                raise ValidationError(f"Rule {rule_index}: For row_match, expected_output list cannot be empty")
            
            # Validate each row is a dictionary
            for i, row in enumerate(expected_output):
                if not isinstance(row, dict):
                    raise ValidationError(f"Rule {rule_index}: Row {i} in expected_output must be a dictionary")

    def _validate_numeric_condition_format(self, condition: str, rule_index: int) -> None:
        """
        Validate numeric condition format
        
        Args:
            condition: Numeric condition string
            rule_index: Rule index for error reporting
            
        Raises:
            ValidationError: If condition format is invalid
        """
        pattern = r'^(>=|<=|>|<|==|!=)?\s*(\d*\.?\d+)$'
        if not re.match(pattern, condition.strip()):
            raise ValidationError(f"Rule {rule_index}: Invalid numeric condition format '{condition}'. Use formats like '>=10', '5', '<100'")

    def execute_dq_check(self, rule: Dict, run_date: str) -> Dict:
        """
        Execute a single DQ check and return results with comprehensive error handling
        
        Args:
            rule: DQ rule definition
            run_date: Execution run date
            
        Returns:
            Dictionary with DQ check results
        """
        check_id = rule['check_id']
        logger.info(f"Executing DQ check: {check_id}")
        
        # Initialize result structure with safe defaults
        result = {
            'check_id': check_id,
            'category': rule.get('category', 'Unknown'),
            'sql_query': rule.get('sql_query', ''),
            'description': rule.get('description', ''),
            'severity': rule.get('severity', 'Medium'),
            'execution_status': 'SUCCESS',
            'error_message': None,
            'actual_output': [],
            'expected_output': self._convert_expected_output_to_array(rule['expected_output'], rule['comparison_type']),
            'comparison_type': rule.get('comparison_type', 'numeric_condition'),
            'validation_status': 'FAIL',  # Default to FAIL for safety
            'impacted_downstream': rule.get('impacted_downstream', []),
            'tags': rule.get('tags', []),
            'execution_duration': '0s',
            'execution_timestamp': DateUtils.get_current_timestamp(),
            'run_date': self._safe_parse_date(run_date)
        }
        
        start_time = time.time()
        
        try:
            # Validate rule before execution
            if not rule.get('active', True):
                result['execution_status'] = 'SKIPPED'
                result['error_message'] = 'Check is not active'
                result['validation_status'] = 'SKIPPED'
                logger.info(f"DQ check {check_id} skipped (not active)")
                return result
            
            # Execute SQL query
            logger.info(f"Executing SQL for DQ check: {check_id}")
            query_results = self.bq_operations.execute_query(rule['sql_query'])
            
            # Transform results to array of strings
            actual_output = self._transform_query_results(query_results)
            result['actual_output'] = actual_output
            
            # Perform validation
            validation_result = self._perform_validation(
                actual_output, 
                rule['expected_output'], 
                rule['comparison_type']
            )
            result['validation_status'] = validation_result
            
            logger.info(f"DQ check {check_id} completed: {validation_result}")
            
        except Exception as e:
            error_msg = str(e)
            result['execution_status'] = 'ERROR'
            result['error_message'] = StringUtils.clean_error_message(error_msg)
            result['validation_status'] = 'FAIL'
            logger.error(f"DQ check {check_id} failed: {error_msg}")
        
        # Calculate and format execution duration
        end_time = time.time()
        duration_seconds = end_time - start_time
        result['execution_duration'] = self._format_duration(duration_seconds)
        
        return result

    def _convert_expected_output_to_array(self, expected_output: Any, comparison_type: str) -> List[str]:
        """
        Convert expected_output to array of strings for storage
        
        Args:
            expected_output: Expected output from rule
            comparison_type: Type of comparison
            
        Returns:
            Array of string representations
        """
        try:
            if comparison_type == 'numeric_condition':
                return [str(expected_output)]
            elif comparison_type in ['set_match', 'not_in_result']:
                return [str(item) for item in expected_output]
            elif comparison_type == 'row_match':
                return [json.dumps(row, default=str, sort_keys=True) for row in expected_output]
            else:
                logger.warning(f"Unknown comparison type: {comparison_type}")
                return [str(expected_output)]
        except Exception as e:
            logger.error(f"Error converting expected_output to array: {str(e)}")
            return [str(expected_output)]

    def _safe_parse_date(self, date_str: str) -> datetime.date:
        """
        Safely parse date string to date object
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            
        Returns:
            Date object or current date as fallback
        """
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing date '{date_str}': {str(e)}. Using current date.")
            return datetime.now().date()

    def _transform_query_results(self, query_results) -> List[str]:
        """
        Transform BigQuery query results to array of strings with robust handling
        
        Args:
            query_results: BigQuery query results
            
        Returns:
            Array of string representations of results
        """
        output = []
        
        try:
            for row in query_results:
                try:
                    # Convert row to dictionary
                    row_dict = dict(row)
                    
                    if len(row_dict) == 1:
                        # Single value result - take the first value
                        value = list(row_dict.values())[0]
                        output.append(str(value) if value is not None else "NULL")
                    else:
                        # Multi-column result - convert to JSON string with sorted keys for consistency
                        # Handle None values in the row
                        clean_row = {k: (v if v is not None else "NULL") for k, v in row_dict.items()}
                        output.append(json.dumps(clean_row, default=str, sort_keys=True))
                        
                except Exception as row_error:
                    logger.warning(f"Error processing row in query results: {str(row_error)}")
                    output.append("ERROR_PROCESSING_ROW")
            
            # If no results, return empty array
            return output
            
        except Exception as e:
            logger.error(f"Error transforming query results: {str(e)}")
            return ["ERROR_TRANSFORMING_RESULTS"]

    def _perform_validation(self, actual_output: List[str], expected_output: Any, 
                          comparison_type: str) -> str:
        """
        Perform validation based on comparison type with comprehensive error handling
        
        Args:
            actual_output: Transformed query results as array of strings
            expected_output: Expected output from rule definition
            comparison_type: Type of comparison to perform
            
        Returns:
            'PASS' or 'FAIL'
        """
        try:
            if comparison_type == 'numeric_condition':
                return self._validate_numeric_condition(actual_output, expected_output)
            elif comparison_type == 'set_match':
                return self._validate_set_match(actual_output, expected_output)
            elif comparison_type == 'not_in_result':
                return self._validate_not_in_result(actual_output, expected_output)
            elif comparison_type == 'row_match':
                return self._validate_row_match(actual_output, expected_output)
            else:
                logger.error(f"Unknown comparison type: {comparison_type}")
                return 'FAIL'
                
        except Exception as e:
            logger.error(f"Validation error for {comparison_type}: {str(e)}")
            return 'FAIL'

    def _validate_numeric_condition(self, actual_output: List[str], expected_condition: Any) -> str:
        """
        Validate numeric condition (e.g., ">=10") with robust parsing
        
        Args:
            actual_output: Query results as array of strings
            expected_condition: String condition to evaluate
            
        Returns:
            'PASS' or 'FAIL'
        """
        if not actual_output:
            logger.warning("No actual output for numeric condition validation")
            return 'FAIL'
        
        if len(actual_output) != 1:
            logger.warning(f"Expected single numeric value, got {len(actual_output)} values")
            return 'FAIL'
        
        try:
            actual_value_str = actual_output[0]
            if actual_value_str == "NULL" or actual_value_str is None:
                return 'FAIL'
                
            actual_value = float(actual_value_str)
            
            # Parse condition
            condition = str(expected_condition).strip()
            
            # Handle direct equality
            if condition.isdigit() or self._is_float(condition):
                expected_value = float(condition)
                return 'PASS' if actual_value == expected_value else 'FAIL'
            
            # Handle operators
            operators = ['>=', '<=', '>', '<', '==', '!=']
            for op in operators:
                if condition.startswith(op):
                    threshold_str = condition[len(op):].strip()
                    if self._is_float(threshold_str):
                        threshold = float(threshold_str)
                        if op == '>=':
                            return 'PASS' if actual_value >= threshold else 'FAIL'
                        elif op == '<=':
                            return 'PASS' if actual_value <= threshold else 'FAIL'
                        elif op == '>':
                            return 'PASS' if actual_value > threshold else 'FAIL'
                        elif op == '<':
                            return 'PASS' if actual_value < threshold else 'FAIL'
                        elif op == '==':
                            return 'PASS' if actual_value == threshold else 'FAIL'
                        elif op == '!=':
                            return 'PASS' if actual_value != threshold else 'FAIL'
            
            logger.error(f"Invalid numeric condition format: {condition}")
            return 'FAIL'
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing numeric condition '{expected_condition}': {str(e)}")
            return 'FAIL'

    def _is_float(self, value: str) -> bool:
        """Check if string can be converted to float"""
        try:
            float(value)
            return True
        except ValueError:
            return False

    def _validate_set_match(self, actual_output: List[str], expected_set: List[Any]) -> str:
        """
        Validate set match (order doesn't matter) with robust type handling
        
        Args:
            actual_output: Query results as array of strings
            expected_set: Expected set of values
            
        Returns:
            'PASS' or 'FAIL'
        """
        try:
            if not isinstance(expected_set, list):
                logger.error("Expected set must be a list")
                return 'FAIL'
            
            actual_set = set(actual_output)
            expected_set_converted = set(str(x) for x in expected_set)
            
            return 'PASS' if actual_set == expected_set_converted else 'FAIL'
            
        except Exception as e:
            logger.error(f"Error in set match validation: {str(e)}")
            return 'FAIL'

    def _validate_not_in_result(self, actual_output: List[str], disallowed_set: List[Any]) -> str:
        """
        Validate that disallowed values are not in results
        
        Args:
            actual_output: Query results as array of strings
            disallowed_set: Values that should not be present
            
        Returns:
            'PASS' or 'FAIL'
        """
        try:
            if not isinstance(disallowed_set, list):
                logger.error("Disallowed set must be a list")
                return 'FAIL'
            
            actual_set = set(actual_output)
            disallowed_set_converted = set(str(x) for x in disallowed_set)
            
            intersection = actual_set.intersection(disallowed_set_converted)
            return 'PASS' if len(intersection) == 0 else 'FAIL'
            
        except Exception as e:
            logger.error(f"Error in not_in_result validation: {str(e)}")
            return 'FAIL'

    def _validate_row_match(self, actual_output: List[str], expected_rows: List[Any]) -> str:
        """
        Validate exact row match (number of rows, values, and order)
        
        Args:
            actual_output: Query results as array of strings
            expected_rows: Expected rows as array of objects
            
        Returns:
            'PASS' or 'FAIL'
        """
        try:
            if not isinstance(expected_rows, list):
                logger.error("Expected rows must be a list")
                return 'FAIL'
            
            # Convert expected rows to string representation for comparison
            expected_output = []
            for row in expected_rows:
                if isinstance(row, dict):
                    # Sort keys and handle None values for consistent comparison
                    clean_row = {k: (v if v is not None else "NULL") for k, v in row.items()}
                    expected_output.append(json.dumps(clean_row, sort_keys=True))
                else:
                    expected_output.append(str(row))
            
            # For row_match, we expect exact match including order and count
            if len(actual_output) != len(expected_output):
                logger.info(f"Row count mismatch: actual {len(actual_output)}, expected {len(expected_output)}")
                return 'FAIL'
            
            # Compare each row in order
            for i, (actual, expected) in enumerate(zip(actual_output, expected_output)):
                if actual != expected:
                    logger.info(f"Row {i} mismatch: actual '{actual}', expected '{expected}'")
                    return 'FAIL'
            
            return 'PASS'
            
        except Exception as e:
            logger.error(f"Error in row match validation: {str(e)}")
            return 'FAIL'

    def _format_duration(self, seconds: float) -> str:
        """
        Format duration in seconds to human readable string
        
        Args:
            seconds: Duration in seconds
            
        Returns:
            Formatted duration string (e.g., "1h 5m 10s", "1m 15s", "30s")
        """
        try:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = int(seconds % 60)
            
            parts = []
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0:
                parts.append(f"{minutes}m")
            if secs > 0 or not parts:  # Always show at least seconds
                parts.append(f"{secs}s")
                
            return " ".join(parts)
        except Exception as e:
            logger.error(f"Error formatting duration: {str(e)}")
            return "0s"

    def process_dq_checks(self, json_data: List[Dict], run_date: str) -> List[Dict]:
        """
        Process all active DQ checks with comprehensive error handling
        
        Args:
            json_data: List of DQ rule definitions
            run_date: Execution run date
            
        Returns:
            List of DQ check results
        """
        logger.info("Starting DQ checks processing")
        
        if not json_data:
            logger.warning("No DQ rules to process")
            return []
        
        # Filter active checks
        active_checks = [rule for rule in json_data if rule.get('active', True)]
        logger.info(f"Found {len(active_checks)} active DQ checks to process")
        
        if not active_checks:
            logger.warning("No active DQ checks found")
            return []
        
        results = []
        successful_checks = 0
        failed_checks = 0
        error_checks = 0
        
        for i, rule in enumerate(active_checks, 1):
            check_id = rule.get('check_id', f'UNKNOWN_{i}')
            logger.info(f"[{i}/{len(active_checks)}] Processing DQ check: {check_id}")
            
            try:
                result = self.execute_dq_check(rule, run_date)
                results.append(result)
                self.processed_checks.append(check_id)
                
                # Track statistics
                if result['execution_status'] == 'SUCCESS':
                    if result['validation_status'] == 'PASS':
                        successful_checks += 1
                    else:
                        failed_checks += 1
                else:
                    error_checks += 1
                
            except Exception as e:
                logger.error(f"Failed to process DQ check {check_id}: {str(e)}")
                # Create comprehensive error result
                error_result = self._create_error_result(rule, run_date, str(e))
                results.append(error_result)
                error_checks += 1
        
        # Log summary
        logger.info("DQ CHECKS PROCESSING SUMMARY:")
        logger.info(f"  Total checks processed: {len(results)}")
        logger.info(f"  Successful executions: {successful_checks}")
        logger.info(f"  Failed validations: {failed_checks}")
        logger.info(f"  Execution errors: {error_checks}")
        
        return results

    def _create_error_result(self, rule: Dict, run_date: str, error_message: str) -> Dict:
        """
        Create comprehensive error result for failed DQ checks
        
        Args:
            rule: DQ rule definition
            run_date: Execution run date
            error_message: Error message
            
        Returns:
            Error result dictionary
        """
        return {
            'check_id': rule.get('check_id', 'UNKNOWN'),
            'category': rule.get('category', 'Unknown'),
            'sql_query': rule.get('sql_query', ''),
            'description': rule.get('description', ''),
            'severity': rule.get('severity', 'Medium'),
            'execution_status': 'ERROR',
            'error_message': StringUtils.clean_error_message(error_message),
            'actual_output': [],
            'expected_output': self._convert_expected_output_to_array(
                rule.get('expected_output', ''), 
                rule.get('comparison_type', 'numeric_condition')
            ),
            'comparison_type': rule.get('comparison_type', 'numeric_condition'),
            'validation_status': 'FAIL',
            'impacted_downstream': rule.get('impacted_downstream', []),
            'tags': rule.get('tags', []),
            'execution_duration': '0s',
            'execution_timestamp': DateUtils.get_current_timestamp(),
            'run_date': self._safe_parse_date(run_date)
        }

    def write_dq_results_to_bq(self, dq_results: List[Dict], results_table: str, run_date: str) -> None:
        """
        Write DQ results to BigQuery table with overwrite capability
        
        Args:
            dq_results: List of DQ check results
            results_table: Target BigQuery table
            run_date: Run date for overwrite partitioning
        """
        if not dq_results:
            logger.info("No DQ results to write")
            return
            
        logger.info(f"Writing {len(dq_results)} DQ results to BigQuery table: {results_table}")
        
        try:
            # Validate results before creating DataFrame
            validated_results = []
            for i, result in enumerate(dq_results):
                try:
                    # Ensure all array fields are properly formatted
                    validated_result = self._validate_dq_result_structure(result)
                    validated_results.append(validated_result)
                except Exception as e:
                    logger.error(f"Error validating result {i}: {str(e)}")
                    # Create safe fallback result
                    safe_result = self._create_safe_result(result)
                    validated_results.append(safe_result)
            
            # Convert results to DataFrame
            logger.info("Creating Spark DataFrame from DQ results...")
            dq_df = self.spark.createDataFrame(validated_results, self.DQ_RESULTS_SCHEMA)
            
            # Show schema and sample data for debugging
            logger.info("DQ Results Schema:")
            dq_df.printSchema()
            
            logger.info("Sample DQ Results (first 3):")
            dq_df.show(3, truncate=False)
            
            # Align schema with BigQuery table using DQ-specific method
            logger.info("Aligning schema with BigQuery table...")
            aligned_df = self.align_dq_schema_with_bq(dq_df, results_table)
            
            # Write to BigQuery with overwrite capability
            logger.info("Writing DataFrame to BigQuery with overwrite...")
            written_check_ids, failed_checks = self.write_dq_to_bq_with_overwrite(
                aligned_df, results_table, run_date
            )
            
            if written_check_ids:
                logger.info(f"Successfully wrote {len(written_check_ids)} DQ checks to {results_table}")
                logger.info(f"Written checks: {written_check_ids[:5]}{'...' if len(written_check_ids) > 5 else ''}")
            
            if failed_checks:
                logger.error(f"Failed to write {len(failed_checks)} DQ checks")
                for i, failed_check in enumerate(failed_checks[:3], 1):
                    logger.error(f"   {i}. {failed_check['check_id']}: {failed_check['error_message'][:100]}...")
            
        except Exception as e:
            logger.error(f"Failed to write DQ results to BigQuery: {str(e)}")
            raise BigQueryError(f"Failed to write DQ results: {str(e)}")

    def _validate_dq_result_structure(self, result: Dict) -> Dict:
        """
        Validate and ensure DQ result has proper structure
        
        Args:
            result: DQ result dictionary
            
        Returns:
            Validated result dictionary
        """
        validated = result.copy()
        
        # Ensure all required fields exist
        required_fields = ['check_id', 'execution_status', 'validation_status', 'execution_timestamp', 'run_date']
        for field in required_fields:
            if field not in validated or validated[field] is None:
                validated[field] = 'UNKNOWN' if field != 'run_date' else datetime.now().date()
        
        # Ensure array fields are lists
        array_fields = ['actual_output', 'expected_output', 'impacted_downstream', 'tags']
        for field in array_fields:
            if field not in validated or not isinstance(validated[field], list):
                validated[field] = []
        
        # Clean error message
        if validated.get('error_message'):
            validated['error_message'] = StringUtils.clean_error_message(validated['error_message'])
        
        return validated

    def _create_safe_result(self, original_result: Dict) -> Dict:
        """
        Create a safe fallback result when validation fails
        
        Args:
            original_result: Original result that failed validation
            
        Returns:
            Safe fallback result
        """
        return {
            'check_id': original_result.get('check_id', 'UNKNOWN'),
            'category': original_result.get('category', 'Unknown'),
            'sql_query': original_result.get('sql_query', ''),
            'description': original_result.get('description', ''),
            'severity': original_result.get('severity', 'Medium'),
            'execution_status': 'ERROR',
            'error_message': 'Result validation failed',
            'actual_output': [],
            'expected_output': [],
            'comparison_type': original_result.get('comparison_type', 'numeric_condition'),
            'validation_status': 'FAIL',
            'impacted_downstream': original_result.get('impacted_downstream', []),
            'tags': original_result.get('tags', []),
            'execution_duration': '0s',
            'execution_timestamp': DateUtils.get_current_timestamp(),
            'run_date': datetime.now().date()
        }

    def write_dq_results_to_bq(self, dq_results: List[Dict], results_table: str, run_date: str) -> None:
        """
        Write DQ results to BigQuery table with overwrite capability
        
        Args:
            dq_results: List of DQ check results
            results_table: Target BigQuery table
            run_date: Run date for overwrite partitioning
        """
        if not dq_results:
            logger.info("No DQ results to write")
            return
            
        logger.info(f"Writing {len(dq_results)} DQ results to BigQuery table: {results_table}")
        
        try:
            # Validate results before creating DataFrame
            validated_results = []
            for i, result in enumerate(dq_results):
                try:
                    # Ensure all array fields are properly formatted
                    validated_result = self._validate_dq_result_structure(result)
                    validated_results.append(validated_result)
                except Exception as e:
                    logger.error(f"Error validating result {i}: {str(e)}")
                    # Create safe fallback result
                    safe_result = self._create_safe_result(result)
                    validated_results.append(safe_result)
            
            # Convert results to DataFrame
            logger.info("Creating Spark DataFrame from DQ results...")
            dq_df = self.spark.createDataFrame(validated_results, self.DQ_RESULTS_SCHEMA)
            
            # Show schema and sample data for debugging
            logger.info("DQ Results Schema:")
            dq_df.printSchema()
            
            logger.info("Sample DQ Results (first 3):")
            dq_df.show(3, truncate=False)
            
            # Align schema with BigQuery table
            logger.info("Aligning schema with BigQuery table...")
            aligned_df = self.bq_operations.align_dataframe_schema_with_bq(dq_df, results_table)
            
            # Write to BigQuery with overwrite capability
            logger.info("Writing DataFrame to BigQuery with overwrite...")
            written_check_ids, failed_checks = self.write_dq_to_bq_with_overwrite(
                aligned_df, results_table, run_date
            )
            
            if written_check_ids:
                logger.info(f"Successfully wrote {len(written_check_ids)} DQ checks to {results_table}")
            if failed_checks:
                logger.error(f"Failed to write {len(failed_checks)} DQ checks")
            
        except Exception as e:
            logger.error(f"Failed to write DQ results to BigQuery: {str(e)}")
            raise BigQueryError(f"Failed to write DQ results: {str(e)}")

    def write_dq_to_bq_with_overwrite(self, df: DataFrame, target_table: str, run_date: str) -> Tuple[List[str], List[Dict]]:
        """
        Write DQ DataFrame to BigQuery table with robust overwrite capability
        
        Args:
            df: Spark DataFrame containing DQ results
            target_table: Target BigQuery table
            run_date: Run date for partitioning
            
        Returns:
            Tuple of (successful_check_ids, failed_checks)
        """
        try:
            logger.info(f"Writing DQ DataFrame to BigQuery table with overwrite: {target_table}")
            
            # Validate input DataFrame
            if df.count() == 0:
                logger.warning("No records to process - DataFrame is empty")
                return [], []
            
            # Get check info for processing
            check_records = df.select('check_id', 'run_date').distinct().collect()
            
            if not check_records:
                logger.warning("No valid DQ check records found")
                return [], []
            
            check_ids = [row['check_id'] for row in check_records]
            record_count = df.count()
            
            logger.info(f"Processing {len(check_ids)} unique DQ checks ({record_count} total records) for run_date: {run_date}")
            
            # Check which checks already exist for robust overwrite
            existing_checks = self.check_existing_dq_checks(check_ids, run_date, target_table)
            new_checks = [check_id for check_id in check_ids if check_id not in existing_checks]
            
            # Delete existing checks if any (part of overwrite operation)
            if existing_checks:
                logger.info(f"Found {len(existing_checks)} existing DQ checks to overwrite")
                self.delete_dq_checks(existing_checks, run_date, target_table)
            
            if new_checks:
                logger.info(f"Adding {len(new_checks)} new DQ checks")
            
            # Write the DataFrame to BigQuery (append mode after deletion = overwrite)
            logger.info(f"Writing {record_count} records to {target_table}")
            self.bq_operations.write_dataframe_to_table(df, target_table, "append")
            
            # Final success logging
            logger.info(f"Successfully completed overwrite operation for {target_table}")
            logger.info(f"Total DQ checks processed: {len(check_ids)} ({len(existing_checks)} overwritten, {len(new_checks)} new)")
            
            return check_ids, []
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"Failed to write to BigQuery with overwrite: {error_message}")
            
            # Create failed checks list for error reporting
            failed_checks = []
            try:
                check_records = df.select('check_id').distinct().collect()
                for row in check_records:
                    failed_checks.append({
                        'check_id': row['check_id'],
                        'error_message': error_message
                    })
                logger.error(f"Failed DQ checks: {[fc['check_id'] for fc in failed_checks]}")
            except Exception as inner_e:
                logger.error(f"Could not extract failed check IDs: {str(inner_e)}")
            
            return [], failed_checks

    def check_existing_dq_checks(self, check_ids: List[str], run_date: str, target_table: str) -> List[str]:
        """
        Check which DQ check IDs already exist in BigQuery table for the given run date
        
        Args:
            check_ids: List of check IDs to check
            run_date: Run date
            target_table: Target table name
            
        Returns:
            List of existing check IDs
        """
        try:
            if not check_ids:
                return []
            
            # Use BigQuery operations to check existing checks
            existing_checks = self.bq_operations.check_existing_metrics(
                check_ids, run_date, target_table
            )
            
            return existing_checks
            
        except Exception as e:
            logger.error(f"Failed to check existing DQ checks: {str(e)}")
            # If we can't check, assume none exist to be safe
            return []

    def delete_dq_checks(self, check_ids: List[str], run_date: str, target_table: str) -> None:
        """
        Delete existing DQ checks from BigQuery table for the given run date
        
        Args:
            check_ids: List of check IDs to delete
            run_date: Run date
            target_table: Target table name
        """
        try:
            if not check_ids:
                logger.info("No DQ checks to delete")
                return
            
            # Use BigQuery operations to delete checks
            self.bq_operations.delete_metrics(check_ids, run_date, target_table)
            
        except Exception as e:
            logger.error(f"Failed to delete existing DQ checks: {str(e)}")
            raise BigQueryError(f"Failed to delete existing DQ checks: {str(e)}")
    
    def align_dq_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame:
    """
    Align DQ DataFrame with BigQuery table schema

    Args:
        df: Spark DataFrame to align
        target_table: Target BigQuery table name
        
    Returns:
        DataFrame with aligned schema
    """
    logger.info(f"Aligning DQ DataFrame schema with BigQuery table: {target_table}")

    try:
        # Use the existing BigQuery operations for schema alignment
        aligned_df = self.bq_operations.align_dataframe_schema_with_bq(df, target_table)
        logger.info(f"Schema alignment complete. Final columns: {aligned_df.columns}")
        return aligned_df
        
    except Exception as e:
        logger.error(f"Failed to align DQ schema with BigQuery: {str(e)}")
        # Return original DataFrame as fallback
        return df


# Factory function
def create_dq_validation_pipeline(spark: SparkSession, bq_operations) -> DQValidationPipeline:
    """Factory function to create DQValidationPipeline instance"""
    return DQValidationPipeline(spark, bq_operations)