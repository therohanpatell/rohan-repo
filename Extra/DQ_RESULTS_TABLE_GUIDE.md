# DQ Results Table Guide

## Overview

The `dq_results` table stores all data quality validation check results, including execution details, validation outcomes, and performance metrics.

## Table Schema

### Core Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `check_id` | STRING | Yes | Unique identifier (e.g., CHK_001_NULL_EMPLOYEE_ID) |
| `category` | STRING | Yes | Check category (Null Check, Value Check, etc.) |
| `description` | STRING | No | Human-readable description |
| `severity` | STRING | Yes | Critical, High, Medium, or Low |
| `sql_query` | STRING | Yes | SQL query executed |
| `expected_output` | STRING | No | Expected result (JSON serialized) |
| `actual_result` | STRING | No | Actual result (JSON serialized) |
| `comparison_type` | STRING | Yes | numeric_condition, set_match, not_in_result, row_match |
| `validation_status` | STRING | Yes | PASS or FAIL |
| `error_message` | STRING | No | Error details if failed |
| `impacted_downstream` | ARRAY<STRING> | No | Affected systems |
| `tags` | ARRAY<STRING> | No | Check tags |
| `execution_timestamp` | TIMESTAMP | Yes | When executed |
| `execution_duration` | FLOAT64 | No | Execution time (seconds) |
| `run_dt` | STRING | Yes | Business date (YYYY-MM-DD) |
| `partition_dt` | STRING | Yes | Partition date (YYYY-MM-DD) |

## Table Features

### Partitioning
- **Partitioned by**: `partition_dt` (daily partitions)
- **Benefit**: Efficient querying and cost optimization
- **Requirement**: Partition filter required for queries

### Clustering
- **Clustered by**: `validation_status`, `severity`, `category`
- **Benefit**: Faster filtering on these columns

### Indexes
- `check_id` - Fast lookup by check ID
- `validation_status` - Quick filtering by status
- `severity` - Efficient severity-based queries

## Creating the Table

### Step 1: Create the Main Table

```sql
-- Replace project.dataset with your values
CREATE TABLE `your-project.your-dataset.dq_results` (
  check_id STRING NOT NULL,
  category STRING NOT NULL,
  description STRING,
  severity STRING NOT NULL,
  sql_query STRING NOT NULL,
  expected_output STRING,
  actual_result STRING,
  comparison_type STRING NOT NULL,
  validation_status STRING NOT NULL,
  error_message STRING,
  impacted_downstream ARRAY<STRING>,
  tags ARRAY<STRING>,
  execution_timestamp TIMESTAMP NOT NULL,
  execution_duration FLOAT64,
  run_dt STRING NOT NULL,
  partition_dt STRING NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
CLUSTER BY validation_status, severity, category
OPTIONS(
  description="Data Quality validation results",
  require_partition_filter=true
);
```

### Step 2: Create Helper Views

The schema file includes 5 helpful views:

1. **dq_results_latest** - Most recent results
2. **dq_results_failed_summary** - Failed checks summary
3. **dq_results_trends** - Historical trends
4. **dq_results_daily_summary** - Daily statistics
5. **dq_results_downstream_impact** - Impact on downstream systems

## Common Queries

### 1. Get Latest Results

```sql
SELECT * FROM `project.dataset.dq_results_latest`;
```

### 2. Find All Failed Checks Today

```sql
SELECT 
  check_id,
  category,
  severity,
  description,
  error_message,
  execution_timestamp
FROM `project.dataset.dq_results`
WHERE partition_dt = CURRENT_DATE()
  AND validation_status = 'FAIL'
ORDER BY 
  CASE severity
    WHEN 'Critical' THEN 1
    WHEN 'High' THEN 2
    WHEN 'Medium' THEN 3
    WHEN 'Low' THEN 4
  END;
```

### 3. Get Pass Rate by Category

```sql
SELECT
  category,
  COUNT(*) AS total_checks,
  COUNTIF(validation_status = 'PASS') AS passed,
  COUNTIF(validation_status = 'FAIL') AS failed,
  ROUND(COUNTIF(validation_status = 'PASS') / COUNT(*) * 100, 2) AS pass_rate_pct
FROM `project.dataset.dq_results`
WHERE partition_dt = CURRENT_DATE()
GROUP BY category
ORDER BY pass_rate_pct;
```

### 4. Find Slowest Checks

```sql
SELECT
  check_id,
  category,
  description,
  execution_duration,
  validation_status
FROM `project.dataset.dq_results`
WHERE partition_dt = CURRENT_DATE()
ORDER BY execution_duration DESC
LIMIT 10;
```

### 5. Check History for Specific Check

```sql
SELECT
  partition_dt,
  validation_status,
  execution_duration,
  error_message,
  execution_timestamp
FROM `project.dataset.dq_results`
WHERE check_id = 'CHK_001_NULL_EMPLOYEE_ID'
ORDER BY partition_dt DESC
LIMIT 30;
```

### 6. Failed Checks by Severity

```sql
SELECT
  severity,
  COUNT(*) AS failed_count,
  ARRAY_AGG(STRUCT(check_id, description, error_message) LIMIT 5) AS sample_failures
FROM `project.dataset.dq_results`
WHERE partition_dt = CURRENT_DATE()
  AND validation_status = 'FAIL'
GROUP BY severity
ORDER BY 
  CASE severity
    WHEN 'Critical' THEN 1
    WHEN 'High' THEN 2
    WHEN 'Medium' THEN 3
    WHEN 'Low' THEN 4
  END;
```

### 7. Checks Affecting Specific Downstream System

```sql
SELECT
  check_id,
  category,
  severity,
  validation_status,
  description,
  error_message
FROM `project.dataset.dq_results`,
UNNEST(impacted_downstream) AS downstream
WHERE partition_dt = CURRENT_DATE()
  AND downstream = 'HR_Dashboard'
  AND validation_status = 'FAIL';
```

### 8. Daily Summary Statistics

```sql
SELECT * FROM `project.dataset.dq_results_daily_summary`
WHERE partition_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY partition_dt DESC;
```

### 9. Trend Analysis (Last 30 Days)

```sql
SELECT
  DATE(PARSE_DATE('%Y-%m-%d', partition_dt)) AS check_date,
  COUNT(*) AS total_checks,
  COUNTIF(validation_status = 'PASS') AS passed,
  COUNTIF(validation_status = 'FAIL') AS failed,
  ROUND(COUNTIF(validation_status = 'PASS') / COUNT(*) * 100, 2) AS pass_rate
FROM `project.dataset.dq_results`
WHERE partition_dt >= FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
GROUP BY check_date
ORDER BY check_date DESC;
```

### 10. Parse JSON Results

```sql
SELECT
  check_id,
  validation_status,
  SAFE.PARSE_JSON(expected_output) AS expected,
  SAFE.PARSE_JSON(actual_result) AS actual,
  error_message
FROM `project.dataset.dq_results`
WHERE partition_dt = CURRENT_DATE()
  AND validation_status = 'FAIL'
LIMIT 10;
```

## Monitoring and Alerting

### Critical Failures Alert Query

```sql
SELECT
  check_id,
  description,
  error_message,
  impacted_downstream,
  execution_timestamp
FROM `project.dataset.dq_results`
WHERE partition_dt = CURRENT_DATE()
  AND validation_status = 'FAIL'
  AND severity = 'Critical';
```

### SLA Monitoring (Execution Time)

```sql
SELECT
  check_id,
  category,
  execution_duration,
  CASE 
    WHEN execution_duration > 60 THEN 'SLOW'
    WHEN execution_duration > 30 THEN 'WARNING'
    ELSE 'OK'
  END AS performance_status
FROM `project.dataset.dq_results`
WHERE partition_dt = CURRENT_DATE()
ORDER BY execution_duration DESC;
```

## Data Retention

### Archive Old Data

```sql
-- Archive data older than 90 days to a separate table
CREATE TABLE `project.dataset.dq_results_archive`
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
AS
SELECT *
FROM `project.dataset.dq_results`
WHERE DATE(PARSE_DATE('%Y-%m-%d', partition_dt)) < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);

-- Delete archived data from main table
DELETE FROM `project.dataset.dq_results`
WHERE DATE(PARSE_DATE('%Y-%m-%d', partition_dt)) < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
```

## Best Practices

### 1. Always Use Partition Filters
```sql
-- Good ✓
WHERE partition_dt = CURRENT_DATE()

-- Bad ✗ (scans entire table)
WHERE check_id = 'CHK_001'
```

### 2. Use Clustering Columns in WHERE Clause
```sql
-- Optimized query
WHERE partition_dt = CURRENT_DATE()
  AND validation_status = 'FAIL'
  AND severity = 'Critical'
  AND category = 'Null Check'
```

### 3. Limit Result Sets
```sql
-- Use LIMIT for large result sets
SELECT * FROM dq_results
WHERE partition_dt = CURRENT_DATE()
LIMIT 1000;
```

### 4. Use Views for Common Queries
```sql
-- Use pre-built views instead of complex queries
SELECT * FROM `project.dataset.dq_results_latest`;
```

## Integration with Dashboards

### Looker Studio / Data Studio

Connect to BigQuery and use these views:
- `dq_results_daily_summary` - For overview dashboard
- `dq_results_trends` - For trend charts
- `dq_results_failed_summary` - For failure analysis

### Tableau

Create extracts from:
- Last 30 days of data for performance
- Use incremental refresh on `partition_dt`

### Custom Dashboards

Query the table via:
- BigQuery API
- Python client library
- JDBC/ODBC connectors

## Troubleshooting

### Query Too Slow
- Ensure partition filter is used
- Check clustering columns are in WHERE clause
- Use LIMIT for testing

### Partition Filter Required Error
```sql
-- Add partition filter
WHERE partition_dt >= '2024-01-01'
```

### JSON Parsing Errors
```sql
-- Use SAFE.PARSE_JSON to handle invalid JSON
SELECT SAFE.PARSE_JSON(expected_output) AS expected
FROM dq_results;
```

## Cost Optimization

1. **Use partition filters** - Reduces data scanned
2. **Select specific columns** - Avoid SELECT *
3. **Use clustering** - Improves query performance
4. **Archive old data** - Move to cheaper storage
5. **Use views** - Pre-optimized queries

## Example: Complete Monitoring Query

```sql
WITH daily_stats AS (
  SELECT
    partition_dt,
    COUNT(*) AS total_checks,
    COUNTIF(validation_status = 'FAIL') AS failed_checks,
    COUNTIF(severity = 'Critical' AND validation_status = 'FAIL') AS critical_failures
  FROM `project.dataset.dq_results`
  WHERE partition_dt = CURRENT_DATE()
  GROUP BY partition_dt
)
SELECT
  partition_dt,
  total_checks,
  failed_checks,
  critical_failures,
  CASE
    WHEN critical_failures > 0 THEN 'CRITICAL ALERT'
    WHEN failed_checks > 5 THEN 'WARNING'
    ELSE 'OK'
  END AS status
FROM daily_stats;
```

## Next Steps

1. Create the table using `dq_results_table_schema.sql`
2. Run your DQ pipeline to populate data
3. Create views for common queries
4. Set up monitoring and alerting
5. Build dashboards for visualization
6. Schedule regular data archival
