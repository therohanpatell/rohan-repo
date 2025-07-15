# Edge Cases Summary for Metrics Pipeline

## Key Edge Cases Where Query Execution Fails

### 1. SQL and Data Issues
- **Invalid SQL syntax**: Malformed queries after placeholder replacement
- **Missing tables**: Referenced tables don't exist or are inaccessible
- **Permission errors**: No access to source tables or BigQuery
- **Missing required columns**: SQL doesn't return `business_data_date` or other required fields
- **Invalid denominators**: Zero, negative, or very small denominator values
- **Date format issues**: Invalid partition dates or run_date format

### 2. Configuration Problems
- **Invalid JSON**: Malformed metric definitions or missing required fields
- **Missing dependencies**: Referenced dependencies don't exist in JSON
- **Invalid target_table**: Not in `project.dataset.table` format
- **Duplicate metric_ids**: Same metric_id appears multiple times

### 3. Infrastructure Issues
- **Network timeouts**: Connection issues to BigQuery
- **Authentication failures**: Service account problems
- **Resource limits**: Query timeouts or memory exhaustion
- **API quotas**: BigQuery quota exceeded

### 4. Write Operation Failures
- **Target table missing**: Table referenced in JSON doesn't exist
- **Schema mismatches**: DataFrame schema incompatible with BigQuery table
- **Write permissions**: No access to target tables
- **Recon table issues**: Cannot write to reconciliation table

## Expected Code Outputs

### Successful Execution
```
INFO: Processing complete: 10 successful, 0 failed
INFO: Write results: 10 successful, 0 failed
INFO: Total recon records created: 10
INFO: Successful metric reconciliations: 10
INFO: Pipeline completed successfully!
```

### Partial Success (Resilient Processing)
```
INFO: Processing complete: 7 successful, 3 failed
WARNING: Failed to execute metrics: ['METRIC_003', 'METRIC_007', 'METRIC_009']
INFO: Write results: 7 successful, 3 failed
WARNING: Failed metrics by table (execution + write failures):
  project.dataset.table1: 2 metrics
  project.dataset.table2: 1 metrics
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
    'module_id': '103',
    'module_type_nm': 'Metrics',
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
    'module_id': '103',
    'module_type_nm': 'Metrics',
    'rcncln_exact_pass_in': 'Failed',
    'target_vl': '1',
    'source_vl': '0',
    'excldd_vl': '1',
    'clcltn_ds': 'Failed',
    'excldd_reason_tx': 'Metric data was failed written.'
}
```

## Common Error Messages

### SQL Execution Errors
- `ERROR: Failed to execute SQL: <BigQuery error message>`
- `ERROR: business_data_date is required but was not returned by the SQL query`
- `ERROR: Invalid denominator value: denominator_value is 0`
- `ERROR: Could not determine partition_dt for table dataset.table`

### Configuration Errors
- `ERROR: Invalid date format: <date>. Expected YYYY-MM-DD`
- `ERROR: Missing dependencies in JSON data: <missing_deps>`
- `ERROR: target_table '<table>' must be in format 'project.dataset.table'`
- `ERROR: Duplicate metric_id '<id>' found`

### Write Operation Errors
- `ERROR: Failed to write to BigQuery with overwrite: <error>`
- `ERROR: Table not found: <table_name>`
- `ERROR: Failed to write recon records to BigQuery: <error>`

## Resilient Processing Behavior

### Key Features
1. **Individual metric failures don't stop the pipeline**
2. **Failed metrics are tracked and get proper recon records**
3. **Pipeline continues processing other metrics**
4. **Partial successes are captured**
5. **Comprehensive logging for troubleshooting**

### Pipeline Flow
1. Process all metrics (some may fail)
2. Write successful metrics to target tables
3. Track execution and write failures
4. Create recon records for ALL metrics (success + failure)
5. Write recon records to recon table
6. Complete successfully even with partial failures

## Monitoring and Troubleshooting

### Key Metrics to Monitor
- **Failure Rate**: `failed_metrics / total_metrics`
- **Write Success Rate**: `successful_writes / total_writes`
- **Recon Success Rate**: `passed_recon_records / total_recon_records`

### Alert Thresholds
- **Critical**: Failure rate > 50%
- **Warning**: Failure rate > 20%
- **Info**: Individual metric consistently failing

### Quick Troubleshooting Steps
1. **High failure rate**: Check BigQuery permissions and table accessibility
2. **All metrics failing**: Verify service account and network connectivity
3. **Write failures**: Check target table existence and schema
4. **No recon records**: Verify recon table permissions and schema

### Log Analysis Commands
```bash
# Check processing results
grep "Processing complete:" pipeline.log

# Count failed metrics
grep "Failed to process metric_id" pipeline.log | wc -l

# Count successful metrics  
grep "Successfully processed metric_id" pipeline.log | wc -l

# Check pipeline completion
grep "Pipeline completed successfully!" pipeline.log
```

### BigQuery Monitoring Queries
```sql
-- Check recon record distribution
SELECT rcncln_exact_pass_in, COUNT(*) as count
FROM `project.dataset.recon_table`
WHERE schdld_dt = '2024-01-01'
GROUP BY rcncln_exact_pass_in;

-- Find failed metrics
SELECT source_system_id, Job_Name, excldd_reason_tx
FROM `project.dataset.recon_table`
WHERE rcncln_exact_pass_in = 'Failed'
AND schdld_dt = '2024-01-01';
```

## Production Recommendations

### Best Practices
1. **Monitor failure rates** regularly
2. **Set up alerting** for high failure rates
3. **Review failed metrics** in recon table
4. **Validate JSON configuration** before deployment
5. **Test with small batches** first

### Performance Optimization
1. **Optimize SQL queries** to reduce execution time
2. **Use appropriate WHERE clauses** to limit data scans
3. **Monitor BigQuery slot usage**
4. **Batch similar metrics** together

The resilient processing ensures your pipeline can handle real-world scenarios where some metrics may fail while still providing comprehensive audit trails through recon records. 