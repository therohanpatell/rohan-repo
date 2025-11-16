# Quick Test Reference Card

## Environment Setup (One-Time)
```bash
export TEST_PROJECT="test_project"
export TEST_DATASET="test_dataset"
export TEST_GCS_BUCKET="gs://your-test-bucket"
export PARTITION_INFO="${TEST_PROJECT}.${TEST_DATASET}.test_partition_info"
export RECON_TABLE="${TEST_PROJECT}.${TEST_DATASET}.test_recon_table"
```

## Setup Commands
```bash
# Create dataset
bq mk --dataset --location=europe-west2 ${TEST_PROJECT}:${TEST_DATASET}

# Create all tables
bq query --use_legacy_sql=false < bigquery_test_schemas.sql

# Load test data
bq query --use_legacy_sql=false < bigquery_test_data.sql

# Upload JSON files
gsutil cp test_metrics_positive.json ${TEST_GCS_BUCKET}/
gsutil cp test_metrics_negative.json ${TEST_GCS_BUCKET}/
```

## Common Test Commands

### Positive Tests
```bash
# Single record test
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json --run_date 2024-01-15 --dependencies test_dependency_1 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}

# Multiple records (10)
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json --run_date 2024-01-15 --dependencies test_dependency_2 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}

# Schema caching test
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json --run_date 2024-01-15 --dependencies test_dependency_9 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}

# Custom columns test
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json --run_date 2024-01-15 --dependencies test_dependency_7 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}

# All positive tests
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json --run_date 2024-01-15 --dependencies test_dependency_1,test_dependency_2,test_dependency_3,test_dependency_4,test_dependency_5,test_dependency_6,test_dependency_7,test_dependency_8,test_dependency_9,test_dependency_10,test_dependency_11,test_dependency_12,test_dependency_13,test_dependency_14 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}
```

### Negative Tests
```bash
# Non-existent table
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json --run_date 2024-01-15 --dependencies test_dependency_neg_1 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}

# SQL syntax error
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json --run_date 2024-01-15 --dependencies test_dependency_neg_3 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}

# Missing required column
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json --run_date 2024-01-15 --dependencies test_dependency_neg_7 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}

# Empty result set
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json --run_date 2024-01-15 --dependencies test_dependency_neg_6 --partition_info_table ${PARTITION_INFO} --env TEST --recon_table ${RECON_TABLE}
```

## Validation Queries

### Check metrics written
```sql
SELECT metric_id, metric_name, metric_output, partition_dt 
FROM `test_project.test_dataset.test_metrics_output` 
WHERE partition_dt = '2024-01-15' 
ORDER BY metric_id;
```

### Check recon records
```sql
SELECT module_id, rcncln_exact_pass_in, source_vl, target_vl, excldd_reason_tx 
FROM `test_project.test_dataset.test_recon_table` 
WHERE schdld_dt = DATE('2024-01-15') 
ORDER BY load_ts DESC;
```

### Count success vs failure
```sql
SELECT 
  rcncln_exact_pass_in,
  COUNT(*) AS count
FROM `test_project.test_dataset.test_recon_table` 
WHERE schdld_dt = DATE('2024-01-15')
GROUP BY rcncln_exact_pass_in;
```

### Check custom columns
```sql
SELECT metric_id, metric_name, region, gender, department, category, metric_output 
FROM `test_project.test_dataset.test_custom_metrics` 
WHERE partition_dt = '2024-01-15';
```

## Quick Checks

### List all tables
```bash
bq ls ${TEST_PROJECT}:${TEST_DATASET}
```

### Check table schema
```bash
bq show --schema --format=prettyjson ${TEST_PROJECT}:${TEST_DATASET}.test_metrics_output
```

### Count records in table
```bash
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`${TEST_PROJECT}.${TEST_DATASET}.test_source_data\`"
```

### Check GCS files
```bash
gsutil ls ${TEST_GCS_BUCKET}/*.json
```

## Cleanup

### Delete specific table
```bash
bq rm -f ${TEST_PROJECT}:${TEST_DATASET}.test_metrics_output
```

### Delete all test data
```bash
bq rm -r -f ${TEST_PROJECT}:${TEST_DATASET}
gsutil rm ${TEST_GCS_BUCKET}/test_metrics_*.json
```

## Test Metrics by Dependency

| Dependency | Metric IDs | Test Type |
|------------|-----------|-----------|
| test_dependency_1 | TEST_001 | Single record |
| test_dependency_2 | TEST_002 | Multiple records (10) |
| test_dependency_3 | TEST_003 | {currently} placeholder |
| test_dependency_4 | TEST_004 | {partition_info} placeholder |
| test_dependency_5 | TEST_005 | All data types |
| test_dependency_6 | TEST_006 | NULL values |
| test_dependency_7 | TEST_007 | Custom columns |
| test_dependency_8 | TEST_008 | Medium volume (1000) |
| test_dependency_9 | TEST_009, TEST_010 | Schema caching |
| test_dependency_10 | TEST_011 | Different table |
| test_dependency_11 | TEST_012 | GROUP BY |
| test_dependency_12 | TEST_013 | CTE |
| test_dependency_13 | TEST_014 | Decimal precision |
| test_dependency_14 | TEST_015 | JOIN |
| test_dependency_neg_1 | NEG_001 | Non-existent table |
| test_dependency_neg_3 | NEG_003 | SQL syntax error |
| test_dependency_neg_4 | NEG_004 | Table not found |
| test_dependency_neg_6 | NEG_006 | Empty result |
| test_dependency_neg_7 | NEG_007 | Missing required column |

## Expected Results Quick Reference

### Positive Test Success Indicators
- ✅ Exit code 0
- ✅ Log shows "Pipeline execution completed successfully"
- ✅ Recon record with rcncln_exact_pass_in = 'Passed'
- ✅ Records written to target table
- ✅ No errors in logs

### Negative Test Success Indicators
- ✅ Appropriate error raised
- ✅ Recon record with rcncln_exact_pass_in = 'Failed'
- ✅ Error details in excldd_reason_tx
- ✅ Error category assigned
- ✅ Pipeline completes without crash

## Performance Benchmarks

| Test | Records | Expected Time |
|------|---------|---------------|
| TEST_001 | 1 | < 5 sec |
| TEST_002 | 10 | < 10 sec |
| TEST_008 | 1,000 | < 30 sec |
| Large volume | 100,000 | < 5 min |

## Troubleshooting Quick Fixes

### "Table not found"
```bash
bq show ${TEST_PROJECT}:${TEST_DATASET}.test_metrics_output
```

### "Permission denied"
```bash
bq show ${TEST_PROJECT}:${TEST_DATASET}
```

### "GCS file not found"
```bash
gsutil ls -l ${TEST_GCS_BUCKET}/test_metrics_positive.json
```

### "Schema mismatch"
```bash
bq show --schema ${TEST_PROJECT}:${TEST_DATASET}.test_metrics_output
```

## Files Reference

| File | Purpose |
|------|---------|
| test_scenarios.csv | Test tracking matrix (Excel) |
| test_metrics_positive.json | 15 positive test metrics |
| test_metrics_negative.json | 20 negative test metrics |
| bigquery_test_schemas.sql | DDL for 14 test tables |
| bigquery_test_data.sql | Sample data inserts |
| test_queries_multiple_records.sql | 20 example queries |
| TEST_EXECUTION_GUIDE.md | Detailed execution guide |
| TEST_ARTIFACTS_SUMMARY.md | Complete artifact overview |
| QUICK_TEST_REFERENCE.md | This file |

