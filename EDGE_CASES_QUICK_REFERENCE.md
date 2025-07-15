# Edge Cases Quick Reference Guide

## Common Failure Scenarios and Expected Outputs

### 1. SQL Execution Failures

| **Edge Case** | **Expected Log** | **Recon Record** | **Pipeline Behavior** |
|---------------|------------------|------------------|----------------------|
| Invalid SQL syntax | `ERROR: Failed to execute SQL: <BigQuery error>` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |
| Missing table | `ERROR: Not found: Table project.dataset.table` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |
| Access denied | `ERROR: Access Denied: <table_name>` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |
| Query timeout | `ERROR: Query exceeded timeout` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |
| Missing business_data_date | `ERROR: business_data_date is required` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |

### 2. Data Validation Failures

| **Edge Case** | **Expected Log** | **Recon Record** | **Pipeline Behavior** |
|---------------|------------------|------------------|----------------------|
| Denominator = 0 | `ERROR: Invalid denominator value: 0` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |
| Negative denominator | `ERROR: Negative denominators not allowed` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |
| Very small denominator | `WARNING: Very small denominator detected` | `rcncln_exact_pass_in = 'Passed'` | Continues processing |
| Invalid numeric values | `WARNING: Could not normalize numeric value` | `rcncln_exact_pass_in = 'Passed'` | Continues processing |

### 3. Configuration Issues

| **Edge Case** | **Expected Log** | **Recon Record** | **Pipeline Behavior** |
|---------------|------------------|------------------|----------------------|
| Invalid date format | `ERROR: Invalid date format` | None created | Pipeline fails |
| Missing dependencies | `ERROR: Missing dependencies in JSON` | None created | Pipeline fails |
| Invalid target_table format | `ERROR: target_table must be project.dataset.table` | None created | Pipeline fails |
| Duplicate metric_id | `ERROR: Duplicate metric_id found` | None created | Pipeline fails |

### 4. Write Operation Failures

| **Edge Case** | **Expected Log** | **Recon Record** | **Pipeline Behavior** |
|---------------|------------------|------------------|----------------------|
| Target table doesn't exist | `ERROR: Table not found: <table>` | `rcncln_exact_pass_in = 'Failed'` | Continues with other tables |
| Schema mismatch | `ERROR: Schema mismatch` | `rcncln_exact_pass_in = 'Failed'` | Continues with other tables |
| Write permissions denied | `ERROR: Access denied for write` | `rcncln_exact_pass_in = 'Failed'` | Continues with other tables |

### 5. Partition Info Issues

| **Edge Case** | **Expected Log** | **Recon Record** | **Pipeline Behavior** |
|---------------|------------------|------------------|----------------------|
| No partition data found | `WARNING: No partition info found` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |
| Invalid partition date | `ERROR: Could not determine partition_dt` | `rcncln_exact_pass_in = 'Failed'` | Continues processing |

## Pipeline Success Scenarios

### Complete Success (All Metrics Pass)
```
INFO: Processing complete: 10 successful, 0 failed
INFO: Write results: 10 successful, 0 failed
INFO: Total recon records created: 10
INFO: Successful metric reconciliations: 10
INFO: Pipeline completed successfully!
```

### Partial Success (Some Metrics Fail)
```
INFO: Processing complete: 7 successful, 3 failed
WARNING: Failed to execute metrics: ['METRIC_003', 'METRIC_007', 'METRIC_009']
INFO: Write results: 7 successful, 3 failed
INFO: Total recon records created: 10
INFO: Successful metric reconciliations: 7
INFO: Failed metric reconciliations: 3
INFO: Pipeline completed successfully!
```

### Complete Failure (All Metrics Fail)
```
INFO: Processing complete: 0 successful, 10 failed
WARNING: No metrics were successfully executed, skipping target table writes
INFO: Write results: 0 successful, 10 failed
INFO: Total recon records created: 10
INFO: Successful metric reconciliations: 0
INFO: Failed metric reconciliations: 10
INFO: Pipeline completed successfully!
```

## Recon Record States

### Successful Metric
```python
{
    'rcncln_exact_pass_in': 'Passed',
    'target_vl': '0',
    'source_vl': '0',
    'excldd_vl': '0',
    'clcltn_ds': 'Success',
    'excldd_reason_tx': 'Metric data was successfully written.'
}
```

### Failed Metric
```python
{
    'rcncln_exact_pass_in': 'Failed',
    'target_vl': '1',
    'source_vl': '0',
    'excldd_vl': '1',
    'clcltn_ds': 'Failed',
    'excldd_reason_tx': 'Metric data was failed written.'
}
```

## Monitoring Commands

### Check Pipeline Status
```bash
# Look for these patterns in logs
grep "Processing complete:" pipeline.log
grep "Pipeline completed successfully!" pipeline.log
```

### Check Failure Rates
```bash
# Count failed metrics
grep "Failed to process metric_id" pipeline.log | wc -l

# Count successful metrics
grep "Successfully processed metric_id" pipeline.log | wc -l
```

### Check Recon Status
```bash
# Query recon table for failure analysis
SELECT rcncln_exact_pass_in, COUNT(*) as count 
FROM `project.dataset.recon_table` 
WHERE schdld_dt = '2024-01-01' 
GROUP BY rcncln_exact_pass_in;
```

## Common Troubleshooting

### High Failure Rate
1. Check BigQuery permissions
2. Verify source table accessibility
3. Validate SQL syntax in JSON
4. Check partition_info_table data quality

### All Metrics Failing
1. Verify service account authentication
2. Check network connectivity
3. Validate JSON configuration format
4. Ensure target tables exist

### Recon Records Not Created
1. Check recon table permissions
2. Verify recon table schema
3. Check for BigQuery quota limits
4. Validate recon table exists

## Performance Considerations

### Expected Processing Times
- **Small batch (1-10 metrics)**: 1-2 minutes
- **Medium batch (50-100 metrics)**: 5-10 minutes  
- **Large batch (500+ metrics)**: 30+ minutes

### Resource Usage
- **Memory**: ~2GB per 100 metrics
- **CPU**: Scales with query complexity
- **Network**: Depends on data volume

### Optimization Tips
1. Use efficient SQL queries
2. Limit data scans with proper WHERE clauses
3. Batch similar metrics together
4. Monitor BigQuery slot usage 