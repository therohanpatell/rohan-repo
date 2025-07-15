# Reconciliation (Recon) Functionality

## Overview

The metrics pipeline now includes comprehensive reconciliation functionality that creates a record for each metric processed, tracking the success/failure status and metadata for audit purposes.

## New CLI Arguments

Two new required arguments have been added:

- `--env`: Environment name (e.g., BLD, PRD, DEV)
- `--recon_table`: BigQuery table for reconciliation data (format: project.dataset.table)

## Recon Table Schema

The reconciliation table contains 29 columns with the following required columns (as specified):

**Important Notes:**
- Most numeric-looking values (source_vl, target_vl, excldd_vl) are stored as STRING types in BigQuery
- Field "Job Name" contains a space in the name
- schdld_dt is a DATE type, load_ts is a STRING type
- module_id is a STRING type despite looking numeric

| Column # | Field Name | Description | Source |
|----------|------------|-------------|---------|
| 1 | module_id | Static value: "101702" | Static - STRING type |
| 2 | module_type_nm | Static value: "ROWCOUNT/Duplicate check" | Static |
| 8 | source_server_nm | Environment name | CLI argument `--env` |
| 14 | target_server_nm | Environment name | CLI argument `--env` |
| 15 | source_vl | "0" (always) | Static - STRING type |
| 16 | target_vl | "0" if success, "1" if failed | Metric execution status - STRING type |
| 21 | rcncln_exact_pass_in | "Passed" if success, "Failed" if failed | Metric execution status |
| 23 | latest_source_parttn_dt | Partition date from SQL | SQL placeholder replacement |
| 24 | latest_target_parttn_dt | Partition date from SQL | SQL placeholder replacement |
| 25 | load_ts | Current timestamp UTC | System timestamp |
| 26 | schdld_dt | Run date | CLI argument `--run_date` - DATE type |
| 27 | source_system_id | Metric ID | JSON field `metric_id` |
| 28 | schdld_yr | Current year | System year |
| 29 | Job Name | Metric name | JSON field `metric_name` - Note: space in field name |

## Additional Context Columns

The following columns are also populated for context:

| Column # | Field Name | Description | Source |
|----------|------------|-------------|---------|
| 3 | source_databs_nm | Source dataset name | Extracted from SQL query |
| 4 | source_table_nm | Source table name | Extracted from SQL query |
| 9 | target_databs_nm | Target dataset name | JSON field `target_table` |
| 10 | target_table_nm | Target table name | JSON field `target_table` |
| 17 | clcltn_ds | "Success" or "Failed" | Metric execution status |
| 18 | excldd_vl | "0" if success, "1" if failed | Metric execution status - STRING type |
| 19 | excldd_reason_tx | Success/failure message | Metric execution status |

## Static Value Columns

The following columns have static values:

| Column # | Field Name | Value |
|----------|------------|-------|
| 5 | source_column_nm | "NA" |
| 6 | source_file_nm | "NA" |
| 7 | source_contrl_file_nm | "NA" |
| 11 | target_column_nm | "NA" |
| 12 | target_file_nm | "NA" |
| 13 | target_contrl_file_nm | "NA" |
| 20 | tolnrc_pc | "NA" |
| 22 | rcncln_tolnrc_pass_in | "NA" |

## How It Works

1. **Metric Processing**: Each metric SQL is processed and executed - individual failures don't stop the pipeline
2. **Metric Table Write**: Successfully processed metrics are written to their respective target tables
3. **Write Success Tracking**: The pipeline tracks which metrics were successfully written to target tables
4. **Resilient Processing**: Failed metrics (execution or write failures) are tracked but don't stop processing of other metrics
5. **Recon Record Creation**: For each metric, a recon record is created based on actual execution and write success/failure
6. **Source Table Extraction**: The source table dataset and name are extracted from the SQL query using regex
7. **Target Table Extraction**: The target table dataset and name are extracted from the JSON `target_table` field
8. **Status Tracking**: Success/failure status is based on actual execution and write results to target tables
9. **Timestamp Recording**: Current timestamp and run date are recorded for audit purposes
10. **Recon Table Write**: All recon records are written to the specified recon table after metric processing

## Example Usage

```bash
python pysaprk.py \
    --gcs_path gs://my-bucket/metrics.json \
    --run_date 2025-01-15 \
    --dependencies dep1,dep2,dep3 \
    --partition_info_table project.metadata.partition_info \
    --env BLD \
    --recon_table project.recon.reconciliation
```

## Key Features

- **Write-Based Tracking**: Recon records reflect actual success/failure of writing to target tables
- **Resilient Processing**: Pipeline continues processing other metrics even if individual metrics fail during execution or writing
- **Comprehensive Tracking**: Every metric gets a recon record regardless of success/failure
- **Source Table Detection**: Automatically extracts source table information from SQL queries
- **Status Mapping**: Maps target table write success/failure to multiple recon fields for different reporting needs
- **Audit Trail**: Provides complete audit trail with timestamps and execution metadata
- **Error Resilience**: Continues processing even if individual metrics or recon records fail

## Benefits

1. **Audit Compliance**: Complete audit trail of all metric processing
2. **Failure Tracking**: Detailed tracking of failed metrics with context
3. **Reconciliation**: Supports data reconciliation processes
4. **Monitoring**: Enables monitoring of pipeline health and metric success rates
5. **Troubleshooting**: Provides detailed context for investigating issues

## BigQuery DDL Compliance

The implementation has been verified to match the provided BigQuery DDL schema exactly:

- ✅ All 29 fields are present and correctly named
- ✅ All data types match BigQuery expectations (STRING, DATE, INTEGER)
- ✅ All nullable/required constraints are properly implemented
- ✅ Special field "Job Name" with space is handled correctly
- ✅ Numeric-looking fields are stored as STRING type as required
- ✅ Date fields are properly formatted and typed

The recon functionality will write directly to your BigQuery table without any schema mismatches. 