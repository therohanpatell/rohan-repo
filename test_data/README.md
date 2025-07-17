# Test Data for Metrics Pipeline Edge Case Testing

This directory contains comprehensive test files organized by test category for the PySpark BigQuery Metrics Pipeline. Each test file corresponds to a specific Test ID from the Edge Case Test Tracking Table.

## Directory Structure

```
test_data/
├── json_configuration/          # JSON Configuration Tests (15 tests)
├── sql_execution/              # SQL Execution Tests (15 tests)
├── data_validation/            # Data Validation Tests (15 tests)
├── bigquery_operations/        # BigQuery Operations Tests (13 tests)
├── recon_records/              # Recon Record Tests (12 tests)
├── network_connectivity/       # Network & Connectivity Tests (8 tests)
├── permission_security/        # Permission & Security Tests (8 tests)
├── resource_performance/       # Resource & Performance Tests (10 tests)
├── error_handling/             # Error Handling Tests (8 tests)
├── pipeline_flow/              # Pipeline Flow Tests (8 tests)
└── README.md                   # This file
```

## Test Categories

### 1. JSON Configuration Tests (15 tests)
Tests for JSON file validation, field requirements, and data integrity.

**Test Files:**
- `JSON_001_valid_complete.json` - Valid complete JSON with all required fields
- `JSON_002_empty_file.json` - Empty JSON file
- `JSON_003_missing_metric_id.json` - Missing metric_id field
- `JSON_004_missing_metric_name.json` - Missing metric_name field
- `JSON_005_missing_sql.json` - Missing sql field
- `JSON_006_missing_target_table.json` - Missing target_table field
- `JSON_007_null_metric_id.json` - Null metric_id value
- `JSON_008_empty_metric_id.json` - Empty metric_id string
- `JSON_009_whitespace_metric_id.json` - Whitespace-only metric_id
- `JSON_010_duplicate_metric_id.json` - Duplicate metric_id values
- `JSON_011_invalid_table_format.json` - Invalid target_table format
- `JSON_012_empty_table_parts.json` - Empty table parts in target_table
- `JSON_013_malformed_json.json` - Malformed JSON syntax
- `JSON_014_invalid_dependency.json` - Invalid dependency reference
- `JSON_015_special_characters.json` - Special characters in metric names

### 2. SQL Execution Tests (15 tests)
Tests for SQL query execution, placeholder replacement, and error handling.

**Test Files:**
- `SQL_001_valid_sql_with_placeholders.json` - Valid SQL with {currently} placeholder
- `SQL_002_sql_syntax_error.json` - SQL with syntax error
- `SQL_003_table_not_found.json` - Non-existent table reference
- `SQL_004_column_not_found.json` - Invalid column reference
- `SQL_005_permission_denied.json` - Restricted table access
- `SQL_006_empty_sql.json` - Empty SQL query
- `SQL_007_no_placeholders.json` - SQL without placeholders
- `SQL_008_partition_table_missing.json` - {partition_info} but table not found
- `SQL_009_no_partition_data.json` - {partition_info} but no data
- `SQL_010_query_timeout.json` - Long-running query
- `SQL_011_complex_joins.json` - SQL with multiple table joins
- `SQL_012_subqueries.json` - SQL with nested subqueries
- `SQL_013_both_placeholders.json` - Both {currently} and {partition_info}
- `SQL_014_multiple_placeholders.json` - Multiple {currently} placeholders
- `SQL_015_malformed_placeholders.json` - Malformed placeholder syntax

### 3. Data Validation Tests (15 tests)
Tests for data type validation, numeric handling, and business rules.

**Test Files:**
- `DATA_001_valid_numeric_values.json` - Valid numeric values
- `DATA_002_zero_denominator.json` - Zero denominator value
- `DATA_003_negative_denominator.json` - Negative denominator value
- `DATA_004_very_small_denominator.json` - Very small denominator
- `DATA_005_missing_business_data_date.json` - Missing business_data_date
- `DATA_006_invalid_date_format.json` - Invalid date format
- `DATA_007_large_numbers.json` - Very large numeric values
- `DATA_008_decimal_precision.json` - High precision decimals
- `DATA_009_null_numeric_values.json` - Null numeric values
- `DATA_010_non_numeric_strings.json` - Non-numeric string values
- `DATA_011_infinity_values.json` - Infinity values
- `DATA_012_nan_values.json` - NaN values
- `DATA_013_zero_numerator.json` - Zero numerator value
- `DATA_014_valid_date_format.json` - Valid date format
- `DATA_015_future_date.json` - Future date values

### 4. BigQuery Operations Tests (13 tests)
Tests for BigQuery write operations, schema alignment, and error handling.

**Test Files:**
- `BQ_001_successful_write.json` - Successful write to existing table
- `BQ_002_overwrite_existing.json` - Overwrite existing metrics
- `BQ_003_table_not_found.json` - Write to non-existent table
- `BQ_004_schema_mismatch.json` - DataFrame with incompatible schema
- `BQ_005_permission_denied.json` - Write to restricted table
- `BQ_006_connection_timeout.json` - Network issues during write
- `BQ_007_quota_exceeded.json` - Write exceeding BigQuery quotas
- `BQ_008_invalid_project.json` - Non-existent project
- `BQ_009_invalid_dataset.json` - Non-existent dataset
- `BQ_010_empty_dataframe.json` - Write empty DataFrame
- `BQ_011_large_dataset.json` - Write thousands of records
- `BQ_012_schema_alignment.json` - Different column order
- `BQ_013_multiple_tables.json` - Write to multiple target tables

### 5. Recon Record Tests (12 tests)
Tests for reconciliation record creation and error message handling.

**Test Files:**
- `RECON_001_success_recon.json` - Successful metric processing
- `RECON_002_failed_metric.json` - Failed metric processing
- `RECON_003_long_error_message.json` - Error message > 500 characters
- `RECON_004_special_characters.json` - Error with special characters
- `RECON_005_newline_in_error.json` - Error message with newlines
- `RECON_006_multiple_success.json` - Multiple successful metrics
- `RECON_007_mixed_results.json` - Some success, some failures
- `RECON_008_recon_table_missing.json` - Recon table doesn't exist
- `RECON_009_recon_write_failure.json` - Cannot write to recon table
- `RECON_010_database_error_detail.json` - SQL execution fails
- `RECON_011_network_error_detail.json` - Network failure during processing
- `RECON_012_permission_error_detail.json` - Permission denied during processing

### 6. Network & Connectivity Tests (8 tests)
Tests for network connectivity issues and recovery.

**Test Files:**
- `NET_001_stable_connection.json` - Normal network conditions
- `NET_002_no_internet.json` - Complete network failure
- `NET_003_dns_failure.json` - DNS resolution fails
- `NET_004_connection_timeout.json` - Request times out
- `NET_005_ssl_errors.json` - SSL/TLS certificate issues
- `NET_006_intermittent_failures.json` - Random network drops
- `NET_007_slow_connection.json` - Very slow but stable network
- `NET_008_connection_recovery.json` - Network drops then recovers

### 7. Permission & Security Tests (8 tests)
Tests for authentication and authorization scenarios.

**Test Files:**
- `PERM_001_full_access.json` - User has all required permissions
- `PERM_002_no_read_access.json` - User cannot read source tables
- `PERM_003_no_write_access.json` - User cannot write to target tables
- `PERM_004_no_bigquery_access.json` - User has no BigQuery permissions
- `PERM_005_no_gcs_access.json` - User cannot read from GCS
- `PERM_006_expired_credentials.json` - Authentication credentials expired
- `PERM_007_invalid_credentials.json` - Wrong or malformed credentials
- `PERM_008_partial_permissions.json` - Can read but not write some tables

### 8. Resource & Performance Tests (10 tests)
Tests for resource constraints and performance scenarios.

**Test Files:**
- `RES_001_normal_load.json` - Standard dataset size
- `RES_002_large_dataset.json` - Processing large amounts of data
- `RES_003_memory_exhausted.json` - Processing exceeds available memory
- `RES_004_cpu_overload.json` - Processing exceeds CPU limits
- `RES_005_disk_full.json` - Insufficient disk space
- `RES_006_query_complexity.json` - Extremely complex queries
- `RES_007_bigquery_slots.json` - Insufficient BigQuery slots
- `RES_008_multiple_tables.json` - Processing multiple target tables
- `RES_009_concurrent_processing.json` - Multiple metrics processed simultaneously
- `RES_010_resource_recovery.json` - System recovers after resource constraint

### 9. Error Handling Tests (8 tests)
Tests for error handling and recovery mechanisms.

**Test Files:**
- `ERR_001_graceful_degradation.json` - Some metrics fail, others succeed
- `ERR_002_complete_failure.json` - All metrics fail
- `ERR_003_rollback_success.json` - Error occurs, rollback triggered
- `ERR_004_rollback_failure.json` - Rollback operation fails
- `ERR_005_error_logging.json` - Various errors occur and are logged
- `ERR_006_unhandled_exception.json` - Unexpected error type
- `ERR_007_logging_failure.json` - Cannot write to logs
- `ERR_008_critical_error.json` - Unrecoverable error

### 10. Pipeline Flow Tests (8 tests)
Tests for end-to-end pipeline integration and flow control.

**Test Files:**
- `PIPE_001_complete_success.json` - All steps successful
- `PIPE_002_partial_success.json` - Some metrics fail, others succeed
- `PIPE_003_configuration_error.json` - Invalid CLI arguments
- `PIPE_004_gcs_path_invalid.json` - Invalid GCS path
- `PIPE_005_date_format_invalid.json` - Invalid run_date format
- `PIPE_006_dependencies_missing.json` - Dependency not in JSON
- `PIPE_007_recovery_scenario.json` - Pipeline recovers from transient errors
- `PIPE_008_environment_variables.json` - Missing required environment variables

## Usage Instructions

### Running Individual Tests

1. **Select a test file** from the appropriate category
2. **Upload to GCS** or use as local file
3. **Execute pipeline** with the test file:

```bash
python pysaprk_copy.py \
  --gcs_path gs://your-bucket/test_data/json_configuration/JSON_001_valid_complete.json \
  --run_date 2024-01-15 \
  --dependencies Employee_Details_Latest \
  --partition_info_table test-project.test_dataset.partition_info \
  --env TEST \
  --recon_table test-project.test_dataset.recon_table
```

### Running Test Categories

To test an entire category, you can iterate through all files in a directory:

```bash
for file in test_data/json_configuration/*.json; do
  echo "Testing: $file"
  python pysaprk_copy.py \
    --gcs_path gs://your-bucket/$file \
    --run_date 2024-01-15 \
    --dependencies Employee_Details_Latest \
    --partition_info_table test-project.test_dataset.partition_info \
    --env TEST \
    --recon_table test-project.test_dataset.recon_table
done
```

### Expected Behaviors

Each test file is designed to trigger specific behaviors:

- **Success Tests**: Should complete successfully with expected output
- **Failure Tests**: Should fail gracefully with appropriate error messages
- **Edge Case Tests**: Should handle edge cases without crashing
- **Performance Tests**: Should complete within reasonable time limits
- **Error Tests**: Should provide meaningful error messages and logging

### Test Data Requirements

Before running tests, ensure you have:

1. **BigQuery Setup**:
   - Test project with appropriate datasets
   - Partition info table with test data
   - Recon table with proper schema
   - Target tables for metrics

2. **GCS Setup**:
   - Test bucket with appropriate permissions
   - Test files uploaded to GCS

3. **Environment Setup**:
   - PySpark environment configured
   - BigQuery client authenticated
   - Required dependencies installed

4. **Test Data**:
   - Source tables with test data
   - Appropriate permissions for test operations

## Test File Format

All test files follow the standard JSON format expected by the pipeline:

```json
[
  {
    "metric_id": "TEST_ID",
    "metric_name": "Test Metric Name",
    "metric_type": "Count|Percentage",
    "metric_description": "Description of the test",
    "sql": "SELECT ... FROM `project.dataset.table` WHERE date = '{currently}'",
    "dependency": "Dependency_Name",
    "frequency": "Daily",
    "modified_date": "2024-01-15",
    "target_table": "project.dataset.table"
  }
]
```

## Notes

- All test files use placeholder project names (e.g., `test-project`, `restricted-project`)
- Replace these with actual project names when running tests
- Some tests require specific error conditions to be set up in the environment
- Network and permission tests may require specific infrastructure setup
- Resource tests may need to be run in environments with controlled resource limits

## Contributing

When adding new test cases:

1. Follow the naming convention: `CATEGORY_XXX_description.json`
2. Include all required fields in the JSON structure
3. Use realistic test data and scenarios
4. Document the expected behavior in the test description
5. Update this README with new test information 