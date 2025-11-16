# Test Execution Guide for Dynamic Schema Framework

## Overview
This guide provides step-by-step instructions for setting up and executing comprehensive tests for the dynamic schema framework.

---

## Test Artifacts Created

1. **test_scenarios.csv** - Excel-compatible CSV with all 100+ test scenarios
2. **test_metrics_positive.json** - 15 positive test case metrics
3. **test_metrics_negative.json** - 20 negative test case metrics
4. **bigquery_test_schemas.sql** - DDL for 14 test tables
5. **bigquery_test_data.sql** - Sample data inserts
6. **test_queries_multiple_records.sql** - 20 test queries returning multiple records
7. **TEST_EXECUTION_GUIDE.md** - This guide

---

## Prerequisites

### 1. BigQuery Setup
- Access to a test BigQuery project
- Permissions to create datasets and tables
- Permissions to insert and query data

### 2. GCS Setup
- Access to a test GCS bucket
- Permissions to upload JSON files

### 3. Environment Variables
```bash
export TEST_PROJECT="test_project"
export TEST_DATASET="test_dataset"
export TEST_GCS_BUCKET="gs://your-test-bucket"
export TEST_PARTITION_INFO_TABLE="${TEST_PROJECT}.${TEST_DATASET}.test_partition_info"
export TEST_RECON_TABLE="${TEST_PROJECT}.${TEST_DATASET}.test_recon_table"
```

---

## Setup Instructions

### Step 1: Create BigQuery Dataset
```bash
bq mk --dataset \
  --location=europe-west2 \
  --description="Test dataset for dynamic schema framework" \
  ${TEST_PROJECT}:${TEST_DATASET}
```

### Step 2: Create Test Tables
```bash
# Execute the schema creation script
bq query --use_legacy_sql=false < bigquery_test_schemas.sql
```

Or create tables individually:
```bash
bq query --use_legacy_sql=false < bigquery_test_schemas.sql
```

### Step 3: Load Test Data
```bash
# Execute the data loading script
bq query --use_legacy_sql=false < bigquery_test_data.sql
```

### Step 4: Upload Test JSON Files to GCS
```bash
# Upload positive test scenarios
gsutil cp test_metrics_positive.json ${TEST_GCS_BUCKET}/test_metrics_positive.json

# Upload negative test scenarios
gsutil cp test_metrics_negative.json ${TEST_GCS_BUCKET}/test_metrics_negative.json
```

### Step 5: Verify Setup
```bash
# Check tables created
bq ls ${TEST_PROJECT}:${TEST_DATASET}

# Check data loaded
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`${TEST_PROJECT}.${TEST_DATASET}.test_source_data\`"

# Check GCS files
gsutil ls ${TEST_GCS_BUCKET}/*.json
```

---

## Test Execution

### Test Category 1: Schema Conversion Tests

#### Test 1.1: All Data Types
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_5 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric TEST_005 processes successfully
- All data types (STRING, INTEGER, FLOAT, NUMERIC, BOOLEAN, DATE, TIMESTAMP) handled correctly
- Recon record shows 'Passed' status

#### Test 1.4: Simple Table Schema
```bash
# Query to test simple schema
bq query --use_legacy_sql=false \
  "SELECT * FROM \`${TEST_PROJECT}.${TEST_DATASET}.test_simple_table\`"
```

**Expected Result:**
- Schema has exactly 3 columns: id (INTEGER), name (STRING), created_date (DATE)

#### Test 1.6: Non-Existent Table (Negative)
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_neg_1 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric NEG_001 fails with BigQueryError
- Recon record shows 'Failed' status
- Error message contains "nonexistent_table"

---

### Test Category 2: Multi-Record SQL Execution Tests

#### Test 2.1: Single Record Return
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_1 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric TEST_001 returns exactly 1 record
- All columns present in result
- Recon record shows 1 record written

#### Test 2.2: Multiple Records (10 rows)
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_2 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric TEST_002 returns exactly 10 records
- All records have consistent structure
- Recon record shows 10 records written

#### Test 2.3: Medium Volume (1000 rows)
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_8 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric TEST_008 returns exactly 1000 records
- Performance acceptable (< 30 seconds)
- Recon record shows 1000 records written

#### Test 2.8: Empty Result Set (Negative)
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_neg_6 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric NEG_006 returns 0 records
- No error raised
- Recon record shows 0 records written

#### Test 2.9: SQL Syntax Error (Negative)
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_neg_3 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric NEG_003 fails with SQLExecutionError
- Error category is 'SQL_SYNTAX_ERROR'
- Recon record shows 'Failed' status

---

### Test Category 3: Schema Caching Tests

#### Test 3.1 & 3.2: Cache Miss and Hit
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_9 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- First metric (TEST_009) causes cache miss - schema fetched from BigQuery
- Second metric (TEST_010) causes cache hit - schema returned from cache
- Log shows "Schema cache miss" for TEST_009
- Log shows "Schema cache hit" for TEST_010

#### Test 3.4: Cache Reuse Across Metrics
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_9,test_dependency_10 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Multiple metrics targeting same table
- Schema fetched only once per unique table
- Significant performance improvement

---

### Test Category 4: Schema Alignment Tests

#### Test 4.2: Extra Columns Dropped
Create a test metric with extra columns:
```json
{
  "metric_id": "TEST_EXTRA_COLS",
  "sql": "SELECT 'TEST_EXTRA_COLS' AS metric_id, 'Test' AS metric_name, 'Count' AS metric_type, 'Test' AS metric_description, 'Daily' AS frequency, 100 AS numerator_value, 100 AS denominator_value, 100.0 AS metric_output, '2024-01-15' AS business_data_date, 'EXTRA_COL_1' AS extra_column_1, 'EXTRA_COL_2' AS extra_column_2",
  "dependency": "test_extra",
  "frequency": "Daily",
  "target_table": "test_project.test_dataset.test_metrics_output"
}
```

**Expected Result:**
- Extra columns (extra_column_1, extra_column_2) are dropped
- Only columns matching target schema are written
- No error raised

#### Test 4.7: Missing Required Column (Negative)
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_neg_7 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric NEG_007 fails with ValidationError
- Error message lists 'metric_id' as missing required column
- Recon record shows 'Failed' status with SCHEMA_VALIDATION_ERROR

---

### Test Category 5: End-to-End Tests

#### Test 5.1: Single Metric Single Record
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_1 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- 1 metric processed successfully
- 1 record written to target table
- 1 recon record created with 'Passed' status

#### Test 5.3: Multiple Metrics Same Table
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_9 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Multiple metrics (TEST_009, TEST_010) processed
- Schema fetched once and cached
- All metrics write successfully

#### Test 5.7: Currently Placeholder
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_3 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- {currently} placeholder replaced with '2024-01-15'
- SQL executes with correct date
- Records written successfully

---

### Test Category 6: Recon Record Tests

#### Test 6.1: Successful Metric Recon
After running any successful test, verify recon record:
```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`${TEST_RECON_TABLE}\` WHERE module_id = 'TEST_001' ORDER BY load_ts DESC LIMIT 1"
```

**Expected Result:**
- rcncln_exact_pass_in = 'Passed'
- source_vl shows record count
- target_vl shows record count
- No error in excldd_reason_tx

#### Test 6.3: Failed Metric Recon
After running a negative test, verify recon record:
```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`${TEST_RECON_TABLE}\` WHERE module_id = 'NEG_001' ORDER BY load_ts DESC LIMIT 1"
```

**Expected Result:**
- rcncln_exact_pass_in = 'Failed'
- excldd_reason_tx contains error details
- Error category present

---

### Test Category 7: Integration Tests

#### Test 7.1: Backward Compatibility
```bash
# Run with old JSON format (from metric.json)
spark-submit main.py \
  --gcs_path gs://your-bucket/metric.json \
  --run_date 2024-01-15 \
  --dependencies Employee_Details_Latest \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Old JSON format still works
- Metrics process successfully
- Results match previous behavior

#### Test 7.2: New JSON with Custom Columns
```bash
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_7 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Metric TEST_007 with custom columns (region, department, category) processes successfully
- Custom columns written to test_custom_metrics table
- Schema dynamically fetched and aligned

#### Test 7.6: Mixed Success and Failure
```bash
# Create mixed JSON with both positive and negative metrics
spark-submit main.py \
  --gcs_path ${TEST_GCS_BUCKET}/test_metrics_mixed.json \
  --run_date 2024-01-15 \
  --dependencies test_dependency_1,test_dependency_neg_1 \
  --partition_info_table ${TEST_PARTITION_INFO_TABLE} \
  --env TEST \
  --recon_table ${TEST_RECON_TABLE}
```

**Expected Result:**
- Some metrics succeed, some fail
- Pipeline completes without crashing
- Recon records created for all metrics (both success and failure)

---

## Validation Queries

### Check Metrics Written
```sql
SELECT 
  metric_id,
  metric_name,
  metric_output,
  partition_dt,
  pipeline_execution_ts
FROM `test_project.test_dataset.test_metrics_output`
WHERE partition_dt = '2024-01-15'
ORDER BY metric_id;
```

### Check Recon Records
```sql
SELECT 
  module_id,
  target_table_nm,
  source_vl,
  target_vl,
  rcncln_exact_pass_in,
  excldd_reason_tx,
  load_ts
FROM `test_project.test_dataset.test_recon_table`
WHERE schdld_dt = DATE('2024-01-15')
ORDER BY load_ts DESC;
```

### Check Schema Cache Performance
```sql
-- Count unique target tables in metrics
SELECT 
  COUNT(DISTINCT target_table) AS unique_tables,
  COUNT(*) AS total_metrics
FROM (
  SELECT JSON_EXTRACT_SCALAR(json_data, '$.target_table') AS target_table
  FROM `test_project.test_dataset.test_metrics_json`
);
```

### Verify Custom Columns
```sql
SELECT 
  metric_id,
  metric_name,
  region,
  gender,
  department,
  category,
  metric_output
FROM `test_project.test_dataset.test_custom_metrics`
WHERE partition_dt = '2024-01-15'
ORDER BY metric_id;
```

---

## Performance Benchmarks

### Expected Performance Metrics

| Test Scenario | Expected Time | Max Acceptable Time |
|--------------|---------------|---------------------|
| Single record | < 5 seconds | 10 seconds |
| 10 records | < 10 seconds | 20 seconds |
| 1,000 records | < 30 seconds | 60 seconds |
| 100,000 records | < 5 minutes | 10 minutes |
| Schema cache hit | < 1 second | 2 seconds |
| Schema cache miss | < 5 seconds | 10 seconds |

---

## Troubleshooting

### Issue: Table Not Found
**Solution:** Verify table exists and project/dataset names are correct
```bash
bq ls ${TEST_PROJECT}:${TEST_DATASET}
```

### Issue: Permission Denied
**Solution:** Check BigQuery IAM permissions
```bash
bq show ${TEST_PROJECT}:${TEST_DATASET}.test_metrics_output
```

### Issue: Schema Mismatch
**Solution:** Verify target table schema matches expected structure
```bash
bq show --schema --format=prettyjson ${TEST_PROJECT}:${TEST_DATASET}.test_metrics_output
```

### Issue: GCS File Not Found
**Solution:** Verify GCS path and file exists
```bash
gsutil ls ${TEST_GCS_BUCKET}/*.json
```

---

## Cleanup

### Remove Test Data
```bash
# Delete test dataset and all tables
bq rm -r -f ${TEST_PROJECT}:${TEST_DATASET}

# Delete GCS test files
gsutil rm ${TEST_GCS_BUCKET}/test_metrics_*.json
```

---

## Test Coverage Summary

| Category | Total Tests | Positive | Negative |
|----------|-------------|----------|----------|
| Schema Conversion | 10 | 5 | 5 |
| Multi-Record SQL | 13 | 7 | 6 |
| Schema Caching | 6 | 4 | 2 |
| Schema Alignment | 11 | 6 | 5 |
| End-to-End | 14 | 8 | 6 |
| Recon Records | 7 | 5 | 2 |
| Integration | 6 | 4 | 2 |
| Logging | 6 | 3 | 3 |
| Performance | 5 | 3 | 2 |
| Edge Cases | 6 | 3 | 3 |
| **TOTAL** | **84** | **48** | **36** |

---

## Success Criteria

✅ All positive tests pass without errors
✅ All negative tests fail with expected error messages
✅ Recon records created for all metrics (success and failure)
✅ Schema caching reduces BigQuery API calls
✅ Performance meets benchmarks
✅ No data loss or corruption
✅ Backward compatibility maintained
✅ Custom columns supported

---

## Next Steps

1. Execute high-priority tests first (marked in test_scenarios.csv)
2. Review logs for any unexpected behavior
3. Validate recon records for accuracy
4. Measure performance improvements from schema caching
5. Document any issues or edge cases discovered
6. Update test scenarios based on findings

---

## Contact

For questions or issues with test execution, refer to:
- unit_testing_scenarios.md - Detailed test scenario descriptions
- test_scenarios.csv - Complete test matrix
- Pipeline logs - Detailed execution logs

