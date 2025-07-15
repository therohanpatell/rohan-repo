# Edge Cases and Expected Outputs for Metrics Pipeline

## Overview

This document outlines all potential edge cases where query execution can fail in the metrics pipeline, along with the expected behavior and outputs from the code.

## 1. SQL Syntax and Structure Issues

### 1.1 Missing or Invalid Placeholders

**Edge Case**: SQL query missing required placeholders
```sql
-- Missing {currently} placeholder
SELECT COUNT(*) as metric_output, 0 as numerator_value, 0 as denominator_value, 
       '2024-01-01' as business_data_date FROM `project.dataset.table`

-- Invalid placeholder format
SELECT COUNT(*) as metric_output WHERE date = {current}  -- Should be {currently}
```

**Expected Output**:
- **Log Message**: `WARNING: No placeholders found in SQL query`
- **Behavior**: Query executes without date filtering (may return incorrect results)
- **Recon Record**: `rcncln_exact_pass_in = 'Passed'` (if query succeeds)
- **Pipeline**: Continues processing

### 1.2 SQL Syntax Errors

**Edge Case**: Malformed SQL after placeholder replacement
```sql
-- Missing closing backtick
SELECT COUNT(*) FROM `project.dataset.table WHERE date = '{currently}'

-- Typo in SQL keywords
SELECT COUNT(*) FORM `project.dataset.table` WHERE date = '{currently}'

-- Invalid table reference
SELECT COUNT(*) FROM project.dataset.table WHERE date = '{currently}'  -- Missing backticks
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to execute SQL: <BigQuery syntax error>`
- **Behavior**: Metric marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`, `target_vl = '1'`
- **Pipeline**: Continues processing other metrics

### 1.3 Missing Required Output Columns

**Edge Case**: SQL doesn't return expected columns
```sql
-- Missing business_data_date (required)
SELECT COUNT(*) as metric_output, 0 as numerator_value, 0 as denominator_value FROM table

-- Wrong column names
SELECT COUNT(*) as wrong_name FROM table
```

**Expected Output**:
- **Log Message**: `ERROR: business_data_date is required but was not returned by the SQL query`
- **Behavior**: MetricsPipelineError raised for this metric
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`, `target_vl = '1'`
- **Pipeline**: Continues processing other metrics

## 2. Data Access and Permissions

### 2.1 BigQuery Access Issues

**Edge Case**: Service account lacks permissions
```python
# Scenarios:
# - No BigQuery job execution permissions
# - No access to source tables
# - No access to partition_info_table
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to execute SQL: Access Denied: <table_name>`
- **Behavior**: Metric marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`, `excldd_reason_tx = 'Metric data was failed written.'`
- **Pipeline**: Continues processing other metrics

### 2.2 Non-existent Tables/Datasets

**Edge Case**: Referenced tables don't exist
```sql
SELECT * FROM `nonexistent_project.dataset.table`
SELECT * FROM `project.nonexistent_dataset.table`
SELECT * FROM `project.dataset.nonexistent_table`
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to execute SQL: Not found: Table project.dataset.table`
- **Behavior**: Metric marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`, `source_databs_nm = 'UNKNOWN'`, `source_table_nm = 'UNKNOWN'`
- **Pipeline**: Continues processing other metrics

## 3. Data Type and Format Issues

### 3.1 Invalid Date Formats

**Edge Case**: Invalid run_date format
```python
# Command line examples:
python pysaprk.py --run_date 2024-13-01  # Invalid month
python pysaprk.py --run_date 2024/01/01  # Wrong format
python pysaprk.py --run_date 01-01-2024  # Wrong order
```

**Expected Output**:
- **Log Message**: `ERROR: Invalid date format: 2024-13-01. Expected YYYY-MM-DD`
- **Behavior**: MetricsPipelineError raised, pipeline stops
- **Recon Record**: No recon records created
- **Pipeline**: Fails before metric processing

### 3.2 Partition Info Table Issues

**Edge Case**: partition_info_table returns invalid data
```sql
-- No rows returned
SELECT partition_dt FROM partition_info_table WHERE project_dataset = 'xxx' AND table_name = 'yyy'

-- NULL values returned
SELECT NULL as partition_dt FROM partition_info_table WHERE ...

-- Invalid date format
SELECT 'invalid_date' as partition_dt FROM partition_info_table WHERE ...
```

**Expected Output**:
- **Log Message**: `WARNING: No partition info found for dataset.table`
- **Behavior**: MetricsPipelineError for affected metrics
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`, `latest_source_parttn_dt = run_date`
- **Pipeline**: Continues processing other metrics

## 4. Resource and Performance Issues

### 4.1 Query Timeout

**Edge Case**: Long-running queries exceed timeout
```sql
-- Resource-intensive query
SELECT COUNT(*) FROM very_large_table 
WHERE complex_condition_scanning_entire_table
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to execute SQL: Query exceeded timeout`
- **Behavior**: Metric marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`, `excldd_reason_tx = 'Metric data was failed written.'`
- **Pipeline**: Continues processing other metrics

### 4.2 Memory/Resource Exhaustion

**Edge Case**: Query requires too much memory
```sql
-- Complex joins on large tables
SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id 
WHERE complex_condition
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to execute SQL: Resources exceeded`
- **Behavior**: Metric marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`
- **Pipeline**: Continues processing other metrics

## 5. Data Validation Failures

### 5.1 Invalid Denominator Values

**Edge Case**: Division by zero or negative denominators
```sql
-- Returns denominator_value = 0
SELECT 100 as metric_output, 100 as numerator_value, 0 as denominator_value, 
       CURRENT_DATE() as business_data_date

-- Returns negative denominator
SELECT 100 as metric_output, 100 as numerator_value, -50 as denominator_value,
       CURRENT_DATE() as business_data_date
```

**Expected Output**:
- **Log Message**: `ERROR: Invalid denominator value: denominator_value is 0. Cannot calculate metrics with zero denominator.`
- **Behavior**: MetricsPipelineError raised for this metric
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`
- **Pipeline**: Continues processing other metrics

### 5.2 Very Small Denominator Values

**Edge Case**: Precision issues with small denominators
```sql
SELECT 100 as metric_output, 100 as numerator_value, 0.0000001 as denominator_value,
       CURRENT_DATE() as business_data_date
```

**Expected Output**:
- **Log Message**: `WARNING: Very small denominator value detected: 0.0000001. This may cause precision issues.`
- **Behavior**: Metric processes but with warning
- **Recon Record**: `rcncln_exact_pass_in = 'Passed'`
- **Pipeline**: Continues processing

## 6. Network and Connectivity Issues

### 6.1 Connection Timeouts

**Edge Case**: Network timeout to BigQuery
```python
# Scenarios:
# - Network connectivity issues
# - BigQuery API endpoint unavailable
# - Firewall blocking connections
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to execute SQL: Connection timeout`
- **Behavior**: Metric marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`
- **Pipeline**: Continues processing other metrics

### 6.2 Authentication Failures

**Edge Case**: Service account authentication fails
```python
# Scenarios:
# - Service account key expired
# - Service account deleted
# - Wrong service account configured
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to execute SQL: Authentication failed`
- **Behavior**: Metric marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`
- **Pipeline**: Continues processing other metrics

## 7. Configuration Issues

### 7.1 Invalid JSON Configuration

**Edge Case**: Malformed JSON input
```json
{
  "metric_id": "METRIC_001",
  "metric_name": "Test Metric",
  "target_table": "invalid_format",  // Should be project.dataset.table
  "sql": "SELECT * FROM table",
  "dependency": "dep1"
}
```

**Expected Output**:
- **Log Message**: `ERROR: target_table 'invalid_format' must be in format 'project.dataset.table'`
- **Behavior**: MetricsPipelineError raised during validation
- **Recon Record**: No recon records created for invalid metrics
- **Pipeline**: Fails during validation phase

### 7.2 Missing Dependencies

**Edge Case**: Referenced dependencies don't exist in JSON
```python
# CLI: --dependencies dep1,dep2,dep3
# JSON only contains: dep1, dep2 (missing dep3)
```

**Expected Output**:
- **Log Message**: `ERROR: Missing dependencies in JSON data: {'dep3'}`
- **Behavior**: MetricsPipelineError raised
- **Recon Record**: No recon records created
- **Pipeline**: Fails before metric processing

## 8. Target Table Write Failures

### 8.1 Schema Mismatch

**Edge Case**: Target table schema changed
```python
# DataFrame has columns that don't exist in BigQuery table
# Or data types don't match
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to write to BigQuery with overwrite: Schema mismatch`
- **Behavior**: Write marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'` (based on write failure)
- **Pipeline**: Continues processing other tables

### 8.2 Target Table Doesn't Exist

**Edge Case**: Target table referenced in JSON doesn't exist
```json
{
  "target_table": "project.dataset.nonexistent_table"
}
```

**Expected Output**:
- **Log Message**: `ERROR: Table not found: project.dataset.nonexistent_table`
- **Behavior**: Write marked as failed
- **Recon Record**: `rcncln_exact_pass_in = 'Failed'`
- **Pipeline**: Continues processing other tables

## 9. Numeric Conversion Issues

### 9.1 Invalid Numeric Values

**Edge Case**: SQL returns non-numeric values in numeric columns
```sql
-- Returns string in numeric column
SELECT 'invalid' as metric_output, 'abc' as numerator_value, 'def' as denominator_value,
       CURRENT_DATE() as business_data_date
```

**Expected Output**:
- **Log Message**: `WARNING: Could not normalize numeric value: invalid, error: ...`
- **Behavior**: Numeric values set to None
- **Recon Record**: `rcncln_exact_pass_in = 'Passed'` (processing continues)
- **Pipeline**: Continues processing

### 9.2 Overflow Errors

**Edge Case**: Numbers too large for processing
```sql
-- Returns extremely large numbers
SELECT 999999999999999999999999999999999999999 as metric_output, ...
```

**Expected Output**:
- **Log Message**: `WARNING: Could not normalize numeric value: <large_number>, error: Overflow`
- **Behavior**: Value set to None, processing continues
- **Recon Record**: `rcncln_exact_pass_in = 'Passed'`
- **Pipeline**: Continues processing

## 10. Expected Success Scenarios

### 10.1 Normal Successful Execution

**Input**: Valid JSON, valid SQL, accessible tables
```sql
SELECT COUNT(*) as metric_output, 
       SUM(value) as numerator_value, 
       COUNT(DISTINCT id) as denominator_value,
       '{currently}' as business_data_date
FROM `project.dataset.table` 
WHERE date = '{currently}'
```

**Expected Output**:
- **Log Message**: `INFO: Successfully processed metric_id: METRIC_001`
- **Behavior**: Metric processed and written successfully
- **Recon Record**: `rcncln_exact_pass_in = 'Passed'`, `target_vl = '0'`, `source_vl = '0'`
- **Pipeline**: Continues processing

### 10.2 Partial Success (Some Metrics Fail)

**Scenario**: 5 metrics in batch, 3 succeed, 2 fail

**Expected Output**:
- **Log Messages**: 
  - `INFO: Successfully processed metric_id: METRIC_001`
  - `INFO: Successfully processed metric_id: METRIC_002`
  - `INFO: Successfully processed metric_id: METRIC_003`
  - `ERROR: Failed to process metric_id METRIC_004: <error>`
  - `ERROR: Failed to process metric_id METRIC_005: <error>`
- **Behavior**: Pipeline completes successfully with partial results
- **Recon Records**: 5 records created (3 passed, 2 failed)
- **Pipeline**: Logs summary: `INFO: Processing complete: 3 successful, 2 failed`

## 11. Recon Table Write Failures

### 11.1 Recon Table Access Issues

**Edge Case**: Cannot write to recon table
```python
# Scenarios:
# - No write permissions to recon table
# - Recon table doesn't exist
# - Schema mismatch in recon table
```

**Expected Output**:
- **Log Message**: `ERROR: Failed to write recon records to BigQuery: <error>`
- **Behavior**: MetricsPipelineError raised
- **Recon Record**: No recon records written
- **Pipeline**: Fails after metric processing (metrics already written)

## 12. Pipeline Summary Outputs

### 12.1 Complete Success

**Expected Log Summary**:
```
INFO: Processing complete: 10 successful, 0 failed
INFO: Write results: 10 successful, 0 failed
INFO: Successfully written metrics by table:
  project.dataset.table1: 5 metrics
  project.dataset.table2: 5 metrics
INFO: Total recon records created: 10
INFO: Successful metric reconciliations: 10
INFO: Pipeline completed successfully!
```

### 12.2 Partial Success

**Expected Log Summary**:
```
INFO: Processing complete: 7 successful, 3 failed
WARNING: Failed to execute metrics: ['METRIC_003', 'METRIC_007', 'METRIC_009']
INFO: Write results: 7 successful, 3 failed
INFO: Successfully written metrics by table:
  project.dataset.table1: 4 metrics
  project.dataset.table2: 3 metrics
WARNING: Failed metrics by table (execution + write failures):
  project.dataset.table1: 2 metrics
  project.dataset.table2: 1 metrics
INFO: Total recon records created: 10
INFO: Successful metric reconciliations: 7
INFO: Failed metric reconciliations: 3
INFO: Pipeline completed successfully!
```

### 12.3 Complete Failure (All Metrics Fail)

**Expected Log Summary**:
```
INFO: Processing complete: 0 successful, 10 failed
WARNING: Failed to execute metrics: ['METRIC_001', 'METRIC_002', ...]
WARNING: No metrics were successfully executed, skipping target table writes
INFO: Write results: 0 successful, 10 failed
WARNING: Failed metrics by table (execution + write failures):
  project.dataset.table1: 5 metrics
  project.dataset.table2: 5 metrics
INFO: Total recon records created: 10
INFO: Successful metric reconciliations: 0
INFO: Failed metric reconciliations: 10
INFO: Pipeline completed successfully!
```

## 13. Monitoring and Alerting Recommendations

### 13.1 Key Metrics to Monitor

1. **Failure Rate**: `failed_metrics / total_metrics`
2. **Table Write Success Rate**: `successful_writes / total_writes`
3. **Recon Record Success Rate**: `passed_recon_records / total_recon_records`
4. **Processing Time**: Time from start to completion

### 13.2 Alert Thresholds

- **Critical**: Failure rate > 50%
- **Warning**: Failure rate > 20%
- **Info**: Any individual metric consistently failing

### 13.3 Log Analysis Patterns

- **ERROR patterns**: Look for "Failed to process metric_id" or "Failed to write to BigQuery"
- **WARNING patterns**: Look for "No partition info found" or "Very small denominator"
- **SUCCESS patterns**: Look for "Successfully processed metric_id" and "Pipeline completed successfully"

## 14. Testing Recommendations

### 14.1 Unit Test Cases

Create test cases for each edge case category:
- Invalid SQL syntax
- Missing permissions
- Invalid date formats
- Network timeouts
- Schema mismatches

### 14.2 Integration Test Scenarios

- End-to-end pipeline with mixed success/failure
- All metrics fail scenario
- Target table write failures
- Recon table write failures

### 14.3 Performance Test Cases

- Large number of metrics (1000+)
- Complex SQL queries
- Resource-intensive operations
- Concurrent pipeline executions

The resilient processing implementation ensures that the pipeline continues running and provides comprehensive audit trails even when these edge cases occur, making it suitable for production environments where partial failures are acceptable. 