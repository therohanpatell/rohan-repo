"""
Example implementation showing how to handle partial validation failures
where 18 checks are valid and 2 have validation errors.

This demonstrates the pattern where:
1. Each check is validated individually
2. Valid checks continue to process
3. Invalid checks are logged with clear errors
4. Results are written to BigQuery for valid checks only
"""

import json
import sys
from typing import List, Dict, Tuple
from validation import ValidationEngine
from exceptions import ValidationError


def load_and_validate_config_individually(config_file: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Load config and validate each check individually.
    
    This is the KEY CHANGE from the current implementation:
    - Instead of failing on first validation error
    - We validate each check separately
    - Collect valid checks and validation errors
    - Continue processing with valid checks
    
    Args:
        config_file: Path to JSON configuration file
        
    Returns:
        Tuple of (valid_checks, validation_errors)
        - valid_checks: List of checks that passed validation
        - validation_errors: List of dicts with error details
    """
    print(f"\n{'='*80}")
    print(f"LOADING AND VALIDATING CONFIGURATION")
    print(f"{'='*80}")
    print(f"Config file: {config_file}\n")
    
    # Load JSON file
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
    except Exception as e:
        print(f"❌ ERROR: Failed to load config file: {e}")
        sys.exit(1)
    
    if not isinstance(config_data, list):
        print(f"❌ ERROR: Config must be a list of checks")
        sys.exit(1)
    
    print(f"Total checks in configuration: {len(config_data)}\n")
    
    # Validate each check individually
    valid_checks = []
    validation_errors = []
    
    print(f"{'='*80}")
    print(f"VALIDATING INDIVIDUAL CHECKS")
    print(f"{'='*80}\n")
    
    for index, check in enumerate(config_data):
        check_id = check.get('check_id', f'Unknown_{index}')
        
        try:
            # Validate this specific check
            ValidationEngine.validate_dq_record(check, index)
            
            # If validation passes, add to valid checks
            valid_checks.append(check)
            print(f"✅ Check {index + 1}/{len(config_data)}: {check_id} - VALID")
            
        except ValidationError as e:
            # If validation fails, log error but continue
            error_info = {
                'index': index,
                'check_id': check_id,
                'error': str(e),
                'check_config': check
            }
            validation_errors.append(error_info)
            print(f"❌ Check {index + 1}/{len(config_data)}: {check_id} - VALIDATION FAILED")
            print(f"   Error: {e}\n")
    
    # Print validation summary
    print(f"\n{'='*80}")
    print(f"VALIDATION SUMMARY")
    print(f"{'='*80}")
    print(f"Total checks: {len(config_data)}")
    print(f"Valid checks: {len(valid_checks)}")
    print(f"Invalid checks: {len(validation_errors)}")
    print(f"Success rate: {(len(valid_checks) / len(config_data) * 100):.1f}%")
    print(f"{'='*80}\n")
    
    return valid_checks, validation_errors


def process_valid_checks(valid_checks: List[Dict], dq_pipeline, run_date: str, target_table: str):
    """
    Process only the valid checks.
    
    Args:
        valid_checks: List of checks that passed validation
        dq_pipeline: DQPipeline instance
        run_date: Business date for the run
        target_table: BigQuery target table
    """
    if not valid_checks:
        print("⚠️  No valid checks to process\n")
        return
    
    print(f"{'='*80}")
    print(f"PROCESSING VALID CHECKS")
    print(f"{'='*80}")
    print(f"Processing {len(valid_checks)} valid checks...\n")
    
    try:
        # Execute the valid checks
        results = dq_pipeline.execute_dq_checks(valid_checks, run_date)
        
        # Write results to BigQuery
        dq_pipeline.write_dq_results(results, target_table)
        
        print(f"\n✅ Successfully processed {len(results)} checks")
        print(f"✅ Results written to BigQuery table: {target_table}\n")
        
    except Exception as e:
        print(f"\n❌ ERROR during check execution: {e}\n")
        raise


def report_validation_errors(validation_errors: List[Dict]):
    """
    Report all validation errors with detailed information.
    
    Args:
        validation_errors: List of validation error details
    """
    if not validation_errors:
        print("✅ All checks validated successfully\n")
        return
    
    print(f"{'='*80}")
    print(f"VALIDATION ERRORS REPORT")
    print(f"{'='*80}")
    print(f"Total validation errors: {len(validation_errors)}\n")
    
    for i, error in enumerate(validation_errors, 1):
        print(f"{'-'*80}")
        print(f"Error #{i}")
        print(f"{'-'*80}")
        print(f"Check ID: {error['check_id']}")
        print(f"Index: {error['index']}")
        print(f"Error: {error['error']}")
        print(f"\nCheck Configuration:")
        print(json.dumps(error['check_config'], indent=2))
        print()
    
    print(f"{'='*80}\n")


def log_validation_errors_to_file(validation_errors: List[Dict], log_file: str = 'validation_errors.log'):
    """
    Log validation errors to a file for debugging.
    
    Args:
        validation_errors: List of validation error details
        log_file: Path to log file
    """
    if not validation_errors:
        return
    
    from datetime import datetime
    
    with open(log_file, 'a') as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"Validation Errors - {datetime.now().isoformat()}\n")
        f.write(f"{'='*80}\n\n")
        
        for error in validation_errors:
            f.write(f"Check ID: {error['check_id']}\n")
            f.write(f"Index: {error['index']}\n")
            f.write(f"Error: {error['error']}\n")
            f.write(f"Configuration:\n{json.dumps(error['check_config'], indent=2)}\n")
            f.write(f"{'-'*80}\n\n")
    
    print(f"📝 Validation errors logged to: {log_file}\n")


def main_with_partial_validation():
    """
    Main function demonstrating partial validation handling.
    
    This is the recommended pattern for handling scenarios where:
    - Some checks have validation errors
    - Other checks are valid and should process
    - Results should be written for valid checks
    - Errors should be clearly reported
    """
    import argparse
    from pyspark.sql import SparkSession
    from bigquery import BigQueryOperations
    from dq_pipeline import DQPipeline
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='DQ Pipeline with Partial Validation')
    parser.add_argument('--config', required=True, help='Path to DQ config JSON file')
    parser.add_argument('--run-date', required=True, help='Business date (YYYY-MM-DD)')
    parser.add_argument('--target-table', required=True, help='BigQuery target table')
    parser.add_argument('--project-id', required=True, help='GCP project ID')
    args = parser.parse_args()
    
    print(f"\n{'='*80}")
    print(f"DQ PIPELINE - PARTIAL VALIDATION MODE")
    print(f"{'='*80}")
    print(f"Config: {args.config}")
    print(f"Run Date: {args.run_date}")
    print(f"Target Table: {args.target_table}")
    print(f"Project ID: {args.project_id}")
    print(f"{'='*80}\n")
    
    # Initialize Spark and BigQuery
    spark = SparkSession.builder \
        .appName("DQ Pipeline - Partial Validation") \
        .getOrCreate()
    
    bq_operations = BigQueryOperations(project_id=args.project_id)
    dq_pipeline = DQPipeline(spark, bq_operations)
    
    # Step 1: Load and validate config individually
    valid_checks, validation_errors = load_and_validate_config_individually(args.config)
    
    # Step 2: Process valid checks (even if some failed validation)
    if valid_checks:
        process_valid_checks(valid_checks, dq_pipeline, args.run_date, args.target_table)
    
    # Step 3: Report validation errors
    report_validation_errors(validation_errors)
    
    # Step 4: Log errors to file
    if validation_errors:
        log_validation_errors_to_file(validation_errors)
    
    # Step 5: Exit with appropriate code
    if validation_errors:
        print(f"⚠️  Pipeline completed with {len(validation_errors)} validation errors")
        print(f"⚠️  {len(valid_checks)} valid checks were processed successfully")
        print(f"⚠️  Review validation_errors.log for details\n")
        sys.exit(1)  # Non-zero exit code indicates errors
    else:
        print(f"✅ Pipeline completed successfully")
        print(f"✅ All {len(valid_checks)} checks validated and processed\n")
        sys.exit(0)  # Zero exit code indicates success


# Example usage for testing
if __name__ == "__main__":
    """
    Test the validation logic without running the full pipeline
    """
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python IMPLEMENTATION_EXAMPLE.py <config_file>")
        print("\nExample:")
        print("  python IMPLEMENTATION_EXAMPLE.py sample_dq_config_negative_tests.json")
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    # Test validation only
    valid_checks, validation_errors = load_and_validate_config_individually(config_file)
    
    # Report results
    report_validation_errors(validation_errors)
    
    # Show what would be processed
    if valid_checks:
        print(f"{'='*80}")
        print(f"CHECKS THAT WOULD BE PROCESSED")
        print(f"{'='*80}")
        for check in valid_checks:
            print(f"  ✓ {check['check_id']}: {check.get('description', 'No description')}")
        print(f"\nTotal: {len(valid_checks)} checks would be processed\n")
    
    # Exit with appropriate code
    sys.exit(1 if validation_errors else 0)


"""
EXAMPLE OUTPUT for sample_dq_config_negative_tests.json:

================================================================================
LOADING AND VALIDATING CONFIGURATION
================================================================================
Config file: sample_dq_config_negative_tests.json

Total checks in configuration: 20

================================================================================
VALIDATING INDIVIDUAL CHECKS
================================================================================

✅ Check 1/20: CHK_NEG_001_VALID_NULL_CHECK - VALID
❌ Check 2/20: CHK_NEG_002_INVALID_SEVERITY - VALIDATION FAILED
   Error: Invalid severity 'Critical'. Must be one of: High, Medium, Low

✅ Check 3/20: CHK_NEG_003_VALID_THRESHOLD - VALID
✅ Check 4/20: CHK_NEG_004_VALID_SET_MATCH - VALID
✅ Check 5/20: CHK_NEG_005_VALID_NOT_IN_RESULT - VALID
❌ Check 6/20: CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH - VALIDATION FAILED
   Error: For comparison_type 'numeric_condition', expected_output must be a string, got list

✅ Check 7/20: CHK_NEG_007_VALID_ROW_MATCH - VALID
... (remaining checks)

================================================================================
VALIDATION SUMMARY
================================================================================
Total checks: 20
Valid checks: 18
Invalid checks: 2
Success rate: 90.0%
================================================================================

================================================================================
PROCESSING VALID CHECKS
================================================================================
Processing 18 valid checks...

[DQ Pipeline execution output...]

✅ Successfully processed 18 checks
✅ Results written to BigQuery table: project.dataset.dq_results

================================================================================
VALIDATION ERRORS REPORT
================================================================================
Total validation errors: 2

--------------------------------------------------------------------------------
Error #1
--------------------------------------------------------------------------------
Check ID: CHK_NEG_002_INVALID_SEVERITY
Index: 1
Error: Invalid severity 'Critical'. Must be one of: High, Medium, Low

Check Configuration:
{
  "check_id": "CHK_NEG_002_INVALID_SEVERITY",
  "category": "Value Check",
  ...
}

--------------------------------------------------------------------------------
Error #2
--------------------------------------------------------------------------------
Check ID: CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH
Index: 5
Error: For comparison_type 'numeric_condition', expected_output must be a string, got list

Check Configuration:
{
  "check_id": "CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH",
  "category": "Value Check",
  ...
}

================================================================================

📝 Validation errors logged to: validation_errors.log

⚠️  Pipeline completed with 2 validation errors
⚠️  18 valid checks were processed successfully
⚠️  Review validation_errors.log for details
"""
