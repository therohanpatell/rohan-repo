# Edge Case Test Tracking Table for Metrics Pipeline

## Instructions for Use

1. **Status Symbols**: 
   - ✅ **PASSED**: Test executed successfully with expected output
   - ❌ **FAILED**: Test failed or produced unexpected output  
   - ⚠️ **PARTIAL**: Test partially successful with minor issues
   - 🔄 **IN_PROGRESS**: Test currently being executed
   - ❌ **NOT_TESTED**: Test not yet executed

2. **How to Test**: 
   - Execute each test case systematically
   - Record actual output in the "Actual Output" column
   - Update status based on comparison with expected output
   - Add notes for any observations or issues

3. **Priority Order**: Execute tests in the order listed for optimal coverage

---

## Test Results Tracking

### 1. JSON Configuration Tests (Critical Foundation)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| JSON_001 | Valid complete JSON | Complete valid JSON with all fields | Pipeline processes successfully | | ❌ NOT_TESTED | |
| JSON_002 | Empty JSON file | `{}` or `[]` | MetricsPipelineError: No data found | | ❌ NOT_TESTED | |
| JSON_003 | Missing metric_id | JSON without metric_id field | MetricsPipelineError: Missing required field 'metric_id' | | ❌ NOT_TESTED | |
| JSON_004 | Missing metric_name | JSON without metric_name field | MetricsPipelineError: Missing required field 'metric_name' | | ❌ NOT_TESTED | |
| JSON_005 | Missing sql | JSON without sql field | MetricsPipelineError: Missing required field 'sql' | | ❌ NOT_TESTED | |
| JSON_006 | Missing target_table | JSON without target_table field | MetricsPipelineError: Missing required field 'target_table' | | ❌ NOT_TESTED | |
| JSON_007 | Null metric_id | `"metric_id": null` | MetricsPipelineError: Field 'metric_id' is null | | ❌ NOT_TESTED | |
| JSON_008 | Empty metric_id | `"metric_id": ""` | MetricsPipelineError: Field 'metric_id' is empty | | ❌ NOT_TESTED | |
| JSON_009 | Whitespace metric_id | `"metric_id": "   "` | MetricsPipelineError: Field 'metric_id' contains only whitespace | | ❌ NOT_TESTED | |
| JSON_010 | Duplicate metric_id | Two records with same metric_id | MetricsPipelineError: Duplicate metric_id found | | ❌ NOT_TESTED | |
| JSON_011 | Invalid table format | target_table as `"table"` | MetricsPipelineError: target_table must be in format 'project.dataset.table' | | ❌ NOT_TESTED | |
| JSON_012 | Empty table parts | target_table as `"project..table"` | MetricsPipelineError: target_table has empty dataset part | | ❌ NOT_TESTED | |
| JSON_013 | Malformed JSON | Invalid JSON syntax | JSON parsing error | | ❌ NOT_TESTED | |
| JSON_014 | Invalid dependency | Dependency not in available list | MetricsPipelineError: Missing dependencies in JSON data | | ❌ NOT_TESTED | |
| JSON_015 | Special characters | Metric names with special chars | Names handled correctly | | ❌ NOT_TESTED | |

### 2. SQL Execution Tests (Core Functionality)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| SQL_001 | Valid SQL with placeholders | `SELECT COUNT(*) FROM table WHERE date = '{currently}'` | Query executed successfully | | ❌ NOT_TESTED | |
| SQL_002 | SQL syntax error | `SELECT COUNT( FROM table` | MetricsPipelineError: Failed to execute SQL: Syntax error | | ❌ NOT_TESTED | |
| SQL_003 | Table not found | `SELECT * FROM non_existent_table` | MetricsPipelineError: Table not found | | ❌ NOT_TESTED | |
| SQL_004 | Column not found | `SELECT invalid_column FROM table` | MetricsPipelineError: Invalid column name | | ❌ NOT_TESTED | |
| SQL_005 | Permission denied | Query on restricted table | MetricsPipelineError: Access denied | | ❌ NOT_TESTED | |
| SQL_006 | Empty SQL | `"sql": ""` | MetricsPipelineError: SQL cannot be empty | | ❌ NOT_TESTED | |
| SQL_007 | No placeholders | SQL without {currently} or {partition_info} | Warning logged, query executed | | ❌ NOT_TESTED | |
| SQL_008 | Partition table missing | {partition_info} but table not found | MetricsPipelineError: Could not determine partition_dt | | ❌ NOT_TESTED | |
| SQL_009 | No partition data | Partition table exists but no data | MetricsPipelineError: Could not determine partition_dt | | ❌ NOT_TESTED | |
| SQL_010 | Query timeout | Very long-running query | MetricsPipelineError: Query timeout | | ❌ NOT_TESTED | |
| SQL_011 | Complex joins | SQL with multiple table joins | Query executed successfully | | ❌ NOT_TESTED | |
| SQL_012 | Subqueries | SQL with nested subqueries | Query executed successfully | | ❌ NOT_TESTED | |
| SQL_013 | Both placeholders | {currently} and {partition_info} | Both placeholders replaced correctly | | ❌ NOT_TESTED | |
| SQL_014 | Multiple placeholders | Multiple {currently} in same query | All placeholders replaced with same date | | ❌ NOT_TESTED | |
| SQL_015 | Malformed placeholders | `{current}` instead of `{currently}` | Placeholder not replaced, query may fail | | ❌ NOT_TESTED | |

### 3. Data Validation Tests (Data Integrity)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| DATA_001 | Valid numeric values | All numeric fields with valid numbers | Values processed correctly | | ❌ NOT_TESTED | |
| DATA_002 | Zero denominator | `denominator_value = 0` | MetricsPipelineError: Invalid denominator value: denominator_value is 0 | | ❌ NOT_TESTED | |
| DATA_003 | Negative denominator | `denominator_value = -5` | MetricsPipelineError: Invalid denominator value: denominator_value is negative | | ❌ NOT_TESTED | |
| DATA_004 | Very small denominator | `denominator_value = 0.0000001` | Warning logged about precision issues | | ❌ NOT_TESTED | |
| DATA_005 | Missing business_data_date | `business_data_date = null` | MetricsPipelineError: business_data_date is required | | ❌ NOT_TESTED | |
| DATA_006 | Invalid date format | `business_data_date = "2024-13-01"` | MetricsPipelineError: Invalid date format | | ❌ NOT_TESTED | |
| DATA_007 | Large numbers | Very large numeric values | Values handled with precision | | ❌ NOT_TESTED | |
| DATA_008 | Decimal precision | High precision decimal values | Precision preserved | | ❌ NOT_TESTED | |
| DATA_009 | Null numeric values | `metric_output = null` | Processed as None | | ❌ NOT_TESTED | |
| DATA_010 | Non-numeric strings | `"metric_output": "abc"` | Warning logged, conversion attempted | | ❌ NOT_TESTED | |
| DATA_011 | Infinity values | `metric_output = infinity` | Warning logged, conversion attempted | | ❌ NOT_TESTED | |
| DATA_012 | NaN values | `metric_output = NaN` | Warning logged, conversion attempted | | ❌ NOT_TESTED | |
| DATA_013 | Zero numerator | `numerator_value = 0` | Processed successfully | | ❌ NOT_TESTED | |
| DATA_014 | Valid date format | `business_data_date = "2024-01-15"` | Date processed correctly | | ❌ NOT_TESTED | |
| DATA_015 | Future date | `business_data_date = "2025-01-01"` | Date processed correctly | | ❌ NOT_TESTED | |

### 4. BigQuery Operations Tests (Data Persistence)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| BQ_001 | Successful write | Valid DataFrame to existing table | Records written successfully | | ❌ NOT_TESTED | |
| BQ_002 | Overwrite existing | Metrics with same ID and partition_dt | Existing records deleted, new records inserted | | ❌ NOT_TESTED | |
| BQ_003 | Table not found | Write to non-existent table | MetricsPipelineError: Table not found | | ❌ NOT_TESTED | |
| BQ_004 | Schema mismatch | DataFrame with incompatible schema | Schema alignment error | | ❌ NOT_TESTED | |
| BQ_005 | Permission denied | Write to restricted table | MetricsPipelineError: Access denied | | ❌ NOT_TESTED | |
| BQ_006 | Connection timeout | Network issues during write | MetricsPipelineError: Connection timeout | | ❌ NOT_TESTED | |
| BQ_007 | Quota exceeded | Write exceeding BigQuery quotas | MetricsPipelineError: Resource exhausted | | ❌ NOT_TESTED | |
| BQ_008 | Invalid project | Table with non-existent project | MetricsPipelineError: Project not found | | ❌ NOT_TESTED | |
| BQ_009 | Invalid dataset | Table with non-existent dataset | MetricsPipelineError: Dataset not found | | ❌ NOT_TESTED | |
| BQ_010 | Empty DataFrame | Write empty DataFrame | No records written, success reported | | ❌ NOT_TESTED | |
| BQ_011 | Large dataset | Write thousands of records | All records written successfully | | ❌ NOT_TESTED | |
| BQ_012 | Schema alignment | DataFrame with different column order | Schema aligned automatically | | ❌ NOT_TESTED | |
| BQ_013 | Multiple tables | Write to multiple target tables | All tables updated successfully | | ❌ NOT_TESTED | |

### 5. Recon Record Tests (Audit Trail)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| RECON_001 | Success recon | Successful metric processing | Recon record with "Passed" status | | ❌ NOT_TESTED | |
| RECON_002 | Failed metric | Failed metric processing | Recon record with "Failed" status and error message | | ❌ NOT_TESTED | |
| RECON_003 | Long error message | Error message > 500 characters | Error message truncated with "..." | | ❌ NOT_TESTED | |
| RECON_004 | Special characters | Error with special characters | Special characters handled correctly | | ❌ NOT_TESTED | |
| RECON_005 | Newline in error | Error message with newlines | Newlines converted to spaces | | ❌ NOT_TESTED | |
| RECON_006 | Multiple success | Multiple successful metrics | All recon records with "Passed" status | | ❌ NOT_TESTED | |
| RECON_007 | Mixed results | Some success, some failures | Mixed recon records with appropriate status | | ❌ NOT_TESTED | |
| RECON_008 | Recon table missing | Recon table doesn't exist | MetricsPipelineError: Recon table not found | | ❌ NOT_TESTED | |
| RECON_009 | Recon write failure | Cannot write to recon table | MetricsPipelineError: Failed to write recon records | | ❌ NOT_TESTED | |
| RECON_010 | Database error detail | SQL execution fails | Recon record includes specific database error | | ❌ NOT_TESTED | |
| RECON_011 | Network error detail | Network failure during processing | Recon record includes network error details | | ❌ NOT_TESTED | |
| RECON_012 | Permission error detail | Permission denied during processing | Recon record includes permission error details | | ❌ NOT_TESTED | |

### 6. Network & Connectivity Tests (Reliability)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| NET_001 | Stable connection | Normal network conditions | All operations successful | | ❌ NOT_TESTED | |
| NET_002 | No internet | Complete network failure | MetricsPipelineError: Network connection failed | | ❌ NOT_TESTED | |
| NET_003 | DNS failure | DNS resolution fails | MetricsPipelineError: DNS resolution failed | | ❌ NOT_TESTED | |
| NET_004 | Connection timeout | Request times out | MetricsPipelineError: Connection timeout | | ❌ NOT_TESTED | |
| NET_005 | SSL errors | SSL/TLS certificate issues | MetricsPipelineError: SSL certificate error | | ❌ NOT_TESTED | |
| NET_006 | Intermittent failures | Random network drops | Some operations fail, others succeed | | ❌ NOT_TESTED | |
| NET_007 | Slow connection | Very slow but stable network | Operations complete with delays | | ❌ NOT_TESTED | |
| NET_008 | Connection recovery | Network drops then recovers | Operations retry and succeed | | ❌ NOT_TESTED | |

### 7. Permission & Security Tests (Security)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| PERM_001 | Full access | User has all required permissions | All operations successful | | ❌ NOT_TESTED | |
| PERM_002 | No read access | User cannot read source tables | MetricsPipelineError: Access denied to source table | | ❌ NOT_TESTED | |
| PERM_003 | No write access | User cannot write to target tables | MetricsPipelineError: Access denied to target table | | ❌ NOT_TESTED | |
| PERM_004 | No BigQuery access | User has no BigQuery permissions | MetricsPipelineError: BigQuery access denied | | ❌ NOT_TESTED | |
| PERM_005 | No GCS access | User cannot read from GCS | MetricsPipelineError: GCS access denied | | ❌ NOT_TESTED | |
| PERM_006 | Expired credentials | Authentication credentials expired | MetricsPipelineError: Authentication failed | | ❌ NOT_TESTED | |
| PERM_007 | Invalid credentials | Wrong or malformed credentials | MetricsPipelineError: Invalid credentials | | ❌ NOT_TESTED | |
| PERM_008 | Partial permissions | Can read but not write some tables | Mixed success/failure results | | ❌ NOT_TESTED | |

### 8. Resource & Performance Tests (Scalability)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| RES_001 | Normal load | Standard dataset size | Processing completes normally | | ❌ NOT_TESTED | |
| RES_002 | Large dataset | Processing large amounts of data | Processing completes with adequate resources | | ❌ NOT_TESTED | |
| RES_003 | Memory exhausted | Processing exceeds available memory | MetricsPipelineError: Out of memory | | ❌ NOT_TESTED | |
| RES_004 | CPU overload | Processing exceeds CPU limits | MetricsPipelineError: CPU limit exceeded | | ❌ NOT_TESTED | |
| RES_005 | Disk full | Insufficient disk space | MetricsPipelineError: Disk space exhausted | | ❌ NOT_TESTED | |
| RES_006 | Query complexity | Extremely complex queries | MetricsPipelineError: Query too complex | | ❌ NOT_TESTED | |
| RES_007 | BigQuery slots | Insufficient BigQuery slots | MetricsPipelineError: Insufficient slots | | ❌ NOT_TESTED | |
| RES_008 | Multiple tables | Processing multiple target tables | All tables processed successfully | | ❌ NOT_TESTED | |
| RES_009 | Concurrent processing | Multiple metrics processed simultaneously | All metrics processed successfully | | ❌ NOT_TESTED | |
| RES_010 | Resource recovery | System recovers after resource constraint | Processing continues after resource becomes available | | ❌ NOT_TESTED | |

### 9. Error Handling Tests (Resilience)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| ERR_001 | Graceful degradation | Some metrics fail, others succeed | Pipeline continues with remaining metrics | | ❌ NOT_TESTED | |
| ERR_002 | Complete failure | All metrics fail | Pipeline completes with all failures reported | | ❌ NOT_TESTED | |
| ERR_003 | Rollback success | Error occurs, rollback triggered | Successfully rolls back processed metrics | | ❌ NOT_TESTED | |
| ERR_004 | Rollback failure | Rollback operation fails | Pipeline reports rollback failure | | ❌ NOT_TESTED | |
| ERR_005 | Error logging | Various errors occur | All errors properly logged with details | | ❌ NOT_TESTED | |
| ERR_006 | Unhandled exception | Unexpected error type | Pipeline fails with generic error handling | | ❌ NOT_TESTED | |
| ERR_007 | Logging failure | Cannot write to logs | Pipeline continues but logging issues reported | | ❌ NOT_TESTED | |
| ERR_008 | Critical error | Unrecoverable error | Pipeline fails and attempts rollback | | ❌ NOT_TESTED | |

### 10. Pipeline Flow Tests (Integration)

| Test ID | Test Case | Input Data | Expected Output | Actual Output | Status | Notes |
|---------|-----------|------------|----------------|---------------|--------|-------|
| PIPE_001 | Complete success | All steps successful | Pipeline completes successfully | | ❌ NOT_TESTED | |
| PIPE_002 | Partial success | Some metrics fail, others succeed | Pipeline continues, reports mixed results | | ❌ NOT_TESTED | |
| PIPE_003 | Configuration error | Invalid CLI arguments | Pipeline fails early with configuration error | | ❌ NOT_TESTED | |
| PIPE_004 | GCS path invalid | Invalid GCS path | MetricsPipelineError: GCS path inaccessible | | ❌ NOT_TESTED | |
| PIPE_005 | Date format invalid | Invalid run_date format | MetricsPipelineError: Invalid date format | | ❌ NOT_TESTED | |
| PIPE_006 | Dependencies missing | Dependency not in JSON | MetricsPipelineError: Missing dependencies | | ❌ NOT_TESTED | |
| PIPE_007 | Recovery scenario | Pipeline recovers from transient errors | Pipeline completes after retries | | ❌ NOT_TESTED | |
| PIPE_008 | Environment variables | Missing required environment variables | Pipeline fails with environment error | | ❌ NOT_TESTED | |

---

## Test Summary Dashboard

### Overall Results
- **Total Tests**: 121
- **Passed**: 0 ✅
- **Failed**: 0 ❌
- **Partial**: 0 ⚠️
- **In Progress**: 0 🔄
- **Not Tested**: 121 ❌

### Category Summary
| Category | Total | Passed | Failed | Partial | Not Tested | Success Rate |
|----------|-------|--------|--------|---------|------------|--------------|
| JSON Configuration | 15 | 0 | 0 | 0 | 15 | 0% |
| SQL Execution | 15 | 0 | 0 | 0 | 15 | 0% |
| Data Validation | 15 | 0 | 0 | 0 | 15 | 0% |
| BigQuery Operations | 13 | 0 | 0 | 0 | 13 | 0% |
| Recon Records | 12 | 0 | 0 | 0 | 12 | 0% |
| Network & Connectivity | 8 | 0 | 0 | 0 | 8 | 0% |
| Permission & Security | 8 | 0 | 0 | 0 | 8 | 0% |
| Resource & Performance | 10 | 0 | 0 | 0 | 10 | 0% |
| Error Handling | 8 | 0 | 0 | 0 | 8 | 0% |
| Pipeline Flow | 8 | 0 | 0 | 0 | 8 | 0% |

### Critical Path Testing Priority
1. **JSON Configuration** - Foundation for all other tests
2. **SQL Execution** - Core functionality 
3. **Data Validation** - Data integrity
4. **BigQuery Operations** - Data persistence
5. **Recon Records** - Audit trail with enhanced error messages
6. **Error Handling** - Resilience testing
7. **Pipeline Flow** - End-to-end integration
8. **Network & Connectivity** - Reliability testing
9. **Permission & Security** - Security validation
10. **Resource & Performance** - Scalability testing

### Notes Section
- Update this table as you execute tests
- Focus on critical path tests first
- Document any unexpected behaviors
- Include environment details for reproducibility
- Use the automated test runner (`edge_case_test_runner.py`) for systematic testing

---

## Test Data Files Required

To execute these tests, create the following test data files:

1. **valid_complete.json** - Complete valid JSON configuration
2. **empty_file.json** - Empty JSON file
3. **missing_fields.json** - JSON with missing required fields
4. **null_values.json** - JSON with null values
5. **invalid_formats.json** - JSON with invalid formats
6. **sql_errors.json** - JSON with SQL errors
7. **large_dataset.json** - Large JSON for performance testing
8. **special_characters.json** - JSON with special characters

## Environment Setup Checklist

Before testing, ensure:
- [ ] Valid BigQuery project and datasets
- [ ] GCS bucket with test files
- [ ] Service account with appropriate permissions
- [ ] Test partition info table exists
- [ ] Test recon table exists
- [ ] Network connectivity to GCP
- [ ] PySpark environment configured
- [ ] Pipeline dependencies installed

This comprehensive test suite ensures complete coverage of all edge cases with systematic tracking and reporting. 