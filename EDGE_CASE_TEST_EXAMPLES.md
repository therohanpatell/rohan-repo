# Edge Case Test Examples - Concrete Test Cases

This document provides concrete examples of how to test each edge case category with actual test data and expected outputs.

## 1. JSON Configuration Test Examples

### Test Case: Valid Complete JSON
**Input File: `valid_complete.json`**
```json
[
  {
    "metric_id": "VALID_001",
    "metric_name": "Valid Test Metric",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, '{currently}' as business_data_date FROM `test_project.test_dataset.test_table` WHERE date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Command:**
```bash
python pysaprk.py --gcs_path "gs://test-bucket/valid_complete.json" --run_date "2024-01-15" --dependencies "test_dep" --partition_info_table "test_project.test_dataset.partition_info" --env "TEST" --recon_table "test_project.test_dataset.recon_table"
```

**Expected Output:**
```
Pipeline processes successfully
Successfully wrote 1 metrics to test_project.test_dataset.metrics_table
Created 1 recon records
```

**Recon Record Expected:**
```
excldd_reason_tx: "Metric data was successfully written."
rcncln_exact_pass_in: "Passed"
target_vl: "0"
```

---

### Test Case: Missing Required Field
**Input File: `missing_metric_id.json`**
```json
[
  {
    "metric_name": "Missing ID Metric",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output FROM table",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
MetricsPipelineError: Record 0: Missing required field 'metric_id'
```

---

### Test Case: Duplicate Metric IDs
**Input File: `duplicate_ids.json`**
```json
[
  {
    "metric_id": "DUPLICATE_001",
    "metric_name": "First Duplicate",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output FROM table1",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  },
  {
    "metric_id": "DUPLICATE_001",
    "metric_name": "Second Duplicate",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output FROM table2",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
MetricsPipelineError: Record 1: Duplicate metric_id 'DUPLICATE_001' found
```

---

## 2. SQL Execution Test Examples

### Test Case: SQL Syntax Error
**Input File: `sql_syntax_error.json`**
```json
[
  {
    "metric_id": "SQL_ERROR_001",
    "metric_name": "SQL Syntax Error Test",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT( FROM `test_project.test_dataset.test_table`",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
Failed to process metric_id SQL_ERROR_001: Failed to execute SQL: Syntax error
```

**Recon Record Expected:**
```
excldd_reason_tx: "Metric data was failed written. Error: Failed to execute SQL: Syntax error at [1:13]"
rcncln_exact_pass_in: "Failed"
target_vl: "1"
```

---

### Test Case: Table Not Found
**Input File: `table_not_found.json`**
```json
[
  {
    "metric_id": "TABLE_ERROR_001",
    "metric_name": "Table Not Found Test",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, '{currently}' as business_data_date FROM `test_project.test_dataset.non_existent_table` WHERE date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
Failed to process metric_id TABLE_ERROR_001: Failed to execute SQL: Table not found: test_project.test_dataset.non_existent_table
```

**Recon Record Expected:**
```
excldd_reason_tx: "Metric data was failed written. Error: Failed to execute SQL: Table not found: test_project.test_dataset.non_existent_table"
rcncln_exact_pass_in: "Failed"
target_vl: "1"
```

---

## 3. Data Validation Test Examples

### Test Case: Zero Denominator
**Input File: `zero_denominator.json`**
```json
[
  {
    "metric_id": "ZERO_DENOM_001",
    "metric_name": "Zero Denominator Test",
    "metric_type": "RATIO",
    "sql": "SELECT 0.5 as metric_output, 100 as numerator_value, 0 as denominator_value, '{currently}' as business_data_date FROM `test_project.test_dataset.test_table` WHERE date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
Failed to process metric_id ZERO_DENOM_001: Invalid denominator value: denominator_value is 0. Cannot calculate metrics with zero denominator.
```

**Recon Record Expected:**
```
excldd_reason_tx: "Metric data was failed written. Error: Invalid denominator value: denominator_value is 0. Cannot calculate metrics with zero denominator."
rcncln_exact_pass_in: "Failed"
target_vl: "1"
```

---

### Test Case: Missing Business Data Date
**Input File: `missing_business_date.json`**
```json
[
  {
    "metric_id": "MISSING_DATE_001",
    "metric_name": "Missing Business Date Test",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, NULL as business_data_date FROM `test_project.test_dataset.test_table`",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
Failed to process metric_id MISSING_DATE_001: business_data_date is required but was not returned by the SQL query
```

**Recon Record Expected:**
```
excldd_reason_tx: "Metric data was failed written. Error: business_data_date is required but was not returned by the SQL query"
rcncln_exact_pass_in: "Failed"
target_vl: "1"
```

---

## 4. BigQuery Operations Test Examples

### Test Case: Target Table Not Found
**Input File: `target_table_missing.json`**
```json
[
  {
    "metric_id": "TARGET_MISSING_001",
    "metric_name": "Target Table Missing Test",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, '{currently}' as business_data_date FROM `test_project.test_dataset.test_table` WHERE date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.non_existent_target_table"
  }
]
```

**Expected Output:**
```
Failed to write metrics to test_project.test_dataset.non_existent_target_table: Table not found: test_project.test_dataset.non_existent_target_table
```

**Recon Record Expected:**
```
excldd_reason_tx: "Metric data was failed written. Error: Table not found: test_project.test_dataset.non_existent_target_table"
rcncln_exact_pass_in: "Failed"
target_vl: "1"
```

---

## 5. Network & Connectivity Test Examples

### Test Case: Connection Timeout
**Simulation:** Use a long-running query that times out
**Input File: `timeout_query.json`**
```json
[
  {
    "metric_id": "TIMEOUT_001",
    "metric_name": "Connection Timeout Test",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, '{currently}' as business_data_date FROM `test_project.test_dataset.very_large_table` WHERE RAND() < 0.00001 AND date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
Failed to process metric_id TIMEOUT_001: Connection timeout: Query exceeded timeout of 30 seconds
```

**Recon Record Expected:**
```
excldd_reason_tx: "Metric data was failed written. Error: Connection timeout: Query exceeded timeout of 30 seconds"
rcncln_exact_pass_in: "Failed"
target_vl: "1"
```

---

## 6. Permission Test Examples

### Test Case: Access Denied to Source Table
**Input File: `permission_denied.json`**
```json
[
  {
    "metric_id": "PERM_DENIED_001",
    "metric_name": "Permission Denied Test",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, '{currently}' as business_data_date FROM `restricted_project.private_dataset.confidential_table` WHERE date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
Failed to process metric_id PERM_DENIED_001: Access denied: User does not have permission to access table restricted_project.private_dataset.confidential_table
```

**Recon Record Expected:**
```
excldd_reason_tx: "Metric data was failed written. Error: Access denied: User does not have permission to access table restricted_project.private_dataset.confidential_table"
rcncln_exact_pass_in: "Failed"
target_vl: "1"
```

---

## 7. Complex Error Message Test Examples

### Test Case: Long Error Message (Truncation)
**Input File: `long_error.json`**
```json
[
  {
    "metric_id": "LONG_ERROR_001",
    "metric_name": "Long Error Message Test",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, '{currently}' as business_data_date FROM `test_project.test_dataset.table_with_very_long_name_that_causes_extremely_detailed_error_messages_when_it_does_not_exist_in_the_database_system` WHERE date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Recon Record:**
```
excldd_reason_tx: "Metric data was failed written. Error: Failed to execute SQL: Table not found: test_project.test_dataset.table_with_very_long_name_that_causes_extremely_detailed_error_messages_when_it_does_not_exist_in_the_database_system. The table name exceeds the maximum length allowed and contains characters that are not supported in BigQuery table names. Please ensure the table name follows the naming conventions: alphanumeric characters and underscores only, maximum length of 1024 characters..."
```

### Test Case: Multiline Error Message
**Input File: `multiline_error.json`**
```json
[
  {
    "metric_id": "MULTILINE_ERROR_001",
    "metric_name": "Multiline Error Test",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output\n, 100 as numerator_value\n, 200 as denominator_value\n, '{currently}' as business_data_date\nFROM `test_project.test_dataset.test_table`\nWHERE invalid_column = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Recon Record:**
```
excldd_reason_tx: "Metric data was failed written. Error: Failed to execute SQL: Invalid column name 'invalid_column' at line 6:7. Column 'invalid_column' does not exist in table test_project.test_dataset.test_table. Available columns: id, name, date, value, status"
```

---

## 8. Mixed Success/Failure Test Examples

### Test Case: Some Metrics Succeed, Others Fail
**Input File: `mixed_results.json`**
```json
[
  {
    "metric_id": "SUCCESS_001",
    "metric_name": "Successful Metric",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, '{currently}' as business_data_date FROM `test_project.test_dataset.test_table` WHERE date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  },
  {
    "metric_id": "FAILURE_001",
    "metric_name": "Failed Metric",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, 100 as numerator_value, 200 as denominator_value, '{currently}' as business_data_date FROM `test_project.test_dataset.non_existent_table` WHERE date = '{currently}'",
    "dependency": "test_dep",
    "target_table": "test_project.test_dataset.metrics_table"
  }
]
```

**Expected Output:**
```
Successfully processed metric_id: SUCCESS_001
Failed to process metric_id FAILURE_001: Failed to execute SQL: Table not found
Processing complete: 1 successful, 1 failed
Successfully wrote 1 metrics to test_project.test_dataset.metrics_table
Created 2 recon records
```

**Expected Recon Records:**
```
SUCCESS_001:
  excldd_reason_tx: "Metric data was successfully written."
  rcncln_exact_pass_in: "Passed"
  target_vl: "0"

FAILURE_001:
  excldd_reason_tx: "Metric data was failed written. Error: Failed to execute SQL: Table not found: test_project.test_dataset.non_existent_table"
  rcncln_exact_pass_in: "Failed"
  target_vl: "1"
```

---

## 9. How to Execute Tests

### Step 1: Set up Test Environment
```bash
# Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
export PROJECT_ID="your-test-project"
export DATASET_ID="test_dataset"
```

### Step 2: Create Test Tables
```sql
-- Create test source table
CREATE TABLE `test_project.test_dataset.test_table` (
  id INT64,
  name STRING,
  date DATE,
  value FLOAT64,
  status STRING
);

-- Create partition info table
CREATE TABLE `test_project.test_dataset.partition_info` (
  project_dataset STRING,
  table_name STRING,
  partition_dt DATE
);

-- Create recon table (use actual DDL from your system)
CREATE TABLE `test_project.test_dataset.recon_table` (
  module_id STRING NOT NULL,
  module_type_nm STRING NOT NULL,
  -- ... other columns as per your DDL
);
```

### Step 3: Upload Test Files to GCS
```bash
# Upload each test JSON file to GCS
gsutil cp valid_complete.json gs://your-test-bucket/
gsutil cp missing_metric_id.json gs://your-test-bucket/
gsutil cp duplicate_ids.json gs://your-test-bucket/
# ... etc
```

### Step 4: Execute Tests
```bash
# Test each JSON file
python pysaprk.py --gcs_path "gs://your-test-bucket/valid_complete.json" --run_date "2024-01-15" --dependencies "test_dep" --partition_info_table "test_project.test_dataset.partition_info" --env "TEST" --recon_table "test_project.test_dataset.recon_table"
```

### Step 5: Verify Results
```sql
-- Check metrics table
SELECT * FROM `test_project.test_dataset.metrics_table` ORDER BY pipeline_execution_ts DESC;

-- Check recon table
SELECT metric_id, rcncln_exact_pass_in, excldd_reason_tx FROM `test_project.test_dataset.recon_table` ORDER BY load_ts DESC;
```

## 10. Automated Test Execution

Use the provided test runner for systematic testing:

```bash
# Run all tests automatically
python edge_case_test_runner.py

# Check generated reports
ls -la edge_case_test_results_*.csv
ls -la edge_case_test_results_*.html
```

This comprehensive test suite provides concrete examples for every edge case, ensuring complete coverage of the enhanced metrics pipeline with detailed error message tracking in recon records. 