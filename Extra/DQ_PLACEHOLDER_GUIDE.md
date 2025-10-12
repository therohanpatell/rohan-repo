# DQ Pipeline Placeholder Guide

## 🎯 Overview

The DQ pipeline now supports **dynamic date placeholders** in SQL queries, allowing you to write reusable DQ checks that automatically adapt to different run dates and partitions.

### Supported Placeholders

1. **`{currently}`** - Replaced with the `--run-date` parameter
2. **`{partition_info}`** - Replaced with the latest partition date from metadata table

---

## 🚀 Quick Start

### Basic Usage with {currently}

**Config:**
```json
{
  "check_id": "CHK_001",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.employees` WHERE hire_date = {currently}",
  "expected_output": ">0",
  "comparison_type": "numeric_condition",
  "active": true
}
```

**Command:**
```bash
python main.py --mode dq \
               --gcs-path gs://bucket/dq_config.json \
               --run-date 2025-10-12 \
               --dq-target-table project.dataset.dq_results
```

**Result:**
- SQL executed: `SELECT COUNT(*) as cnt FROM ... WHERE hire_date = '2025-10-12'`

### Usage with {partition_info}

**Config:**
```json
{
  "check_id": "CHK_002",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.sales` WHERE partition_dt = {partition_info}",
  "expected_output": ">0",
  "comparison_type": "numeric_condition",
  "active": true
}
```

**Command:**
```bash
python main.py --mode dq \
               --gcs-path gs://bucket/dq_config.json \
               --run-date 2025-10-12 \
               --dq-partition-info-table project.dataset.partition_info \
               --dq-target-table project.dataset.dq_results
```

**Result:**
- System queries `partition_info` table for latest partition of `project.dataset.sales`
- Finds `partition_dt = '2025-10-11'`
- SQL executed: `SELECT COUNT(*) as cnt FROM ... WHERE partition_dt = '2025-10-11'`

---

## 📋 Command Line Arguments

### Required Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--mode` | Pipeline mode | `dq` |
| `--gcs-path` | GCS path to config JSON | `gs://bucket/dq_config.json` |
| `--run-date` | Run date (YYYY-MM-DD) | `2025-10-12` |
| `--dq-target-table` | DQ results table | `project.dataset.dq_results` |

### Optional Arguments

| Argument | Description | Required For | Example |
|----------|-------------|--------------|---------|
| `--dq-partition-info-table` | Partition metadata table | `{partition_info}` placeholder | `project.dataset.partition_info` |

---

## 📝 Placeholder Details

### {currently} Placeholder

**Purpose:** Use the run date in SQL queries

**Replacement:** `{currently}` → `'YYYY-MM-DD'` (from `--run-date`)

**Use Cases:**
- Check data for specific date
- Filter by transaction date
- Validate daily data loads
- Compare against run date

**Examples:**
```sql
-- Check records for run date
SELECT COUNT(*) FROM table WHERE date = {currently}

-- Check records up to run date
SELECT COUNT(*) FROM table WHERE date <= {currently}

-- Check records in date range
SELECT COUNT(*) FROM table WHERE date BETWEEN DATE_SUB({currently}, INTERVAL 7 DAY) AND {currently}
```

### {partition_info} Placeholder

**Purpose:** Use the latest partition date from metadata table

**Replacement:** `{partition_info}` → `'YYYY-MM-DD'` (from partition metadata)

**Requirements:**
- Must provide `--dq-partition-info-table` argument
- Partition info table must exist with required structure
- Table reference must appear before placeholder in SQL

**Use Cases:**
- Check latest partition data
- Validate partition completeness
- Compare partition data
- Ensure partition exists

**Examples:**
```sql
-- Check records in latest partition
SELECT COUNT(*) FROM table WHERE partition_dt = {partition_info}

-- Check for nulls in latest partition
SELECT COUNT(*) FROM table WHERE partition_dt = {partition_info} AND column IS NULL

-- Compare partitions
SELECT COUNT(*) FROM table WHERE partition_dt = {partition_info} AND status = 'ACTIVE'
```

---

## 🔧 Complete Examples

### Example 1: Daily Null Check

**Scenario:** Check for NULL employee IDs in today's hires

**Config:**
```json
{
  "check_id": "CHK_DAILY_NULL_EMPLOYEE_ID",
  "category": "Null Check",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.employees` WHERE hire_date = {currently} AND employee_id IS NULL",
  "description": "Verify no NULL employee IDs for employees hired on run date",
  "severity": "High",
  "expected_output": "0",
  "comparison_type": "numeric_condition",
  "impacted_downstream": ["HR_Dashboard", "Payroll_System"],
  "tags": ["employee", "critical", "daily"],
  "active": true
}
```

**Command:**
```bash
python main.py --mode dq \
               --gcs-path gs://my-bucket/dq_config.json \
               --run-date 2025-10-12 \
               --dq-target-table my-project.my_dataset.dq_results
```

**Execution:**
- `{currently}` → `'2025-10-12'`
- SQL: `SELECT COUNT(*) as cnt FROM ... WHERE hire_date = '2025-10-12' AND employee_id IS NULL`
- Expected: 0
- Result: PASS if count is 0, FAIL otherwise

### Example 2: Partition Data Quality Check

**Scenario:** Check for NULL amounts in latest sales partition

**Config:**
```json
{
  "check_id": "CHK_PARTITION_NULL_AMOUNT",
  "category": "Null Check",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.sales` WHERE partition_dt = {partition_info} AND amount IS NULL",
  "description": "Verify no NULL amounts in latest sales partition",
  "severity": "High",
  "expected_output": "0",
  "comparison_type": "numeric_condition",
  "impacted_downstream": ["Sales_Dashboard", "Finance_Report"],
  "tags": ["sales", "partition", "critical"],
  "active": true
}
```

**Command:**
```bash
python main.py --mode dq \
               --gcs-path gs://my-bucket/dq_config.json \
               --run-date 2025-10-12 \
               --dq-partition-info-table my-project.my_dataset.partition_info \
               --dq-target-table my-project.my_dataset.dq_results
```

**Execution:**
1. System queries `partition_info` table:
   ```sql
   SELECT partition_dt FROM `my-project.my_dataset.partition_info`
   WHERE project_dataset = 'project.dataset' AND table_name = 'sales'
   ```
2. Finds latest partition: `'2025-10-11'`
3. `{partition_info}` → `'2025-10-11'`
4. SQL: `SELECT COUNT(*) as cnt FROM ... WHERE partition_dt = '2025-10-11' AND amount IS NULL`
5. Expected: 0
6. Result: PASS if count is 0, FAIL otherwise

### Example 3: Both Placeholders

**Scenario:** Count orders for run date in latest partition

**Config:**
```json
{
  "check_id": "CHK_ORDERS_BOTH_PLACEHOLDERS",
  "category": "Threshold Check",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.orders` WHERE order_date = {currently} AND partition_dt = {partition_info}",
  "description": "Count orders for run date in latest partition",
  "severity": "Medium",
  "expected_output": ">=100",
  "comparison_type": "numeric_condition",
  "impacted_downstream": ["Order_Dashboard"],
  "tags": ["orders", "daily"],
  "active": true
}
```

**Command:**
```bash
python main.py --mode dq \
               --gcs-path gs://my-bucket/dq_config.json \
               --run-date 2025-10-12 \
               --dq-partition-info-table my-project.my_dataset.partition_info \
               --dq-target-table my-project.my_dataset.dq_results
```

**Execution:**
- `{currently}` → `'2025-10-12'`
- `{partition_info}` → `'2025-10-11'` (from metadata)
- SQL: `SELECT COUNT(*) as cnt FROM ... WHERE order_date = '2025-10-12' AND partition_dt = '2025-10-11'`
- Expected: >= 100
- Result: PASS if count >= 100, FAIL otherwise

---

## 🚨 Error Handling

### Error 1: {partition_info} without --dq-partition-info-table

**Error Message:**
```
ValidationError: SQL contains {partition_info} placeholder but no partition_info_table provided.
Use --dq-partition-info-table argument to specify the partition metadata table.
```

**Solution:**
Add `--dq-partition-info-table` argument:
```bash
python main.py --mode dq \
               --gcs-path gs://bucket/config.json \
               --run-date 2025-10-12 \
               --dq-partition-info-table project.dataset.partition_info \
               --dq-target-table project.dataset.dq_results
```

### Error 2: Partition Not Found in Metadata

**Error Message:**
```
ValidationError: Could not find partition_dt for project.dataset.sales in project.dataset.partition_info
```

**Possible Causes:**
1. Table not in partition metadata
2. Partition metadata table empty
3. Wrong table name in SQL

**Solution:**
1. Verify table exists in partition metadata:
   ```sql
   SELECT * FROM `project.dataset.partition_info`
   WHERE project_dataset = 'project.dataset' AND table_name = 'sales'
   ```
2. Insert partition metadata if missing
3. Check table name in SQL query matches metadata

### Error 3: Invalid Table Reference

**Error Message:**
```
ValidationError: Could not find table reference for {partition_info} placeholder
```

**Cause:** SQL doesn't have proper `FROM` clause before placeholder

**Solution:** Ensure table reference appears before placeholder:
```sql
-- ❌ WRONG - No FROM clause
SELECT {partition_info} as partition_date

-- ✅ CORRECT - FROM clause before placeholder
SELECT COUNT(*) FROM `project.dataset.table` WHERE partition_dt = {partition_info}
```

---

## 📊 Sample Config File

See `sample_dq_config_with_placeholders.json` for a complete example with:
- 10 checks using placeholders
- 2 static checks (no placeholders)
- Mix of `{currently}` and `{partition_info}`
- Various comparison types

---

## 🎓 Best Practices

### 1. Use {currently} for Date-Based Checks
```json
{
  "sql_query": "SELECT COUNT(*) FROM table WHERE date = {currently}",
  "description": "Check data for run date"
}
```

### 2. Use {partition_info} for Partition-Based Checks
```json
{
  "sql_query": "SELECT COUNT(*) FROM table WHERE partition_dt = {partition_info}",
  "description": "Check latest partition data"
}
```

### 3. Combine Both for Complex Checks
```json
{
  "sql_query": "SELECT COUNT(*) FROM table WHERE date = {currently} AND partition_dt = {partition_info}",
  "description": "Check run date data in latest partition"
}
```

### 4. Static Checks Don't Need Placeholders
```json
{
  "sql_query": "SELECT COUNT(*) FROM table",
  "description": "Check total record count (static)"
}
```

### 5. Document Placeholder Usage in Description
```json
{
  "description": "Verify no NULL amounts in latest partition (uses {partition_info})"
}
```

---

## 🔄 Migration from Static Dates

### Before (Static Dates)
```json
{
  "check_id": "CHK_001",
  "sql_query": "SELECT COUNT(*) FROM table WHERE date = '2025-10-12'",
  "description": "Check data for 2025-10-12"
}
```

**Problems:**
- Must update config for each run
- Not reusable
- Error-prone

### After (Placeholders)
```json
{
  "check_id": "CHK_001",
  "sql_query": "SELECT COUNT(*) FROM table WHERE date = {currently}",
  "description": "Check data for run date"
}
```

**Benefits:**
- Reusable config
- Dynamic dates
- No manual updates needed

---

## 📈 Testing Placeholders

### Test {currently} Placeholder

**Config:**
```json
{
  "check_id": "TEST_CURRENTLY",
  "sql_query": "SELECT '{currently}' as run_date",
  "expected_output": "2025-10-12",
  "comparison_type": "numeric_condition",
  "active": true
}
```

**Command:**
```bash
python main.py --mode dq \
               --gcs-path gs://bucket/test_config.json \
               --run-date 2025-10-12 \
               --dq-target-table project.dataset.dq_results
```

**Expected:** Check should PASS with run_date = '2025-10-12'

### Test {partition_info} Placeholder

**Config:**
```json
{
  "check_id": "TEST_PARTITION_INFO",
  "sql_query": "SELECT COUNT(*) FROM `project.dataset.sales` WHERE partition_dt = {partition_info}",
  "expected_output": ">0",
  "comparison_type": "numeric_condition",
  "active": true
}
```

**Command:**
```bash
python main.py --mode dq \
               --gcs-path gs://bucket/test_config.json \
               --run-date 2025-10-12 \
               --dq-partition-info-table project.dataset.partition_info \
               --dq-target-table project.dataset.dq_results
```

**Expected:** Check should PASS if partition exists with data

---

## 💡 Tips

1. **Always use placeholders for date-based checks** - Makes configs reusable
2. **Test placeholders with simple queries first** - Verify replacement works
3. **Document which placeholders are used** - Add to description field
4. **Use descriptive check IDs** - Include "CURRENT" or "PARTITION" in ID
5. **Tag checks with placeholder usage** - Add "placeholder" tag

---

## 📞 Summary

**Placeholder Support:** ✅ IMPLEMENTED

**Placeholders Available:**
- `{currently}` - Run date
- `{partition_info}` - Latest partition date

**Benefits:**
- Reusable configs
- Dynamic date handling
- No manual date updates
- Consistent with metrics pipeline

**Usage:**
- Add placeholders to SQL queries
- Provide `--dq-partition-info-table` for `{partition_info}`
- Run pipeline with `--run-date`

**Example Command:**
```bash
python main.py --mode dq \
               --gcs-path gs://bucket/dq_config.json \
               --run-date 2025-10-12 \
               --dq-partition-info-table project.dataset.partition_info \
               --dq-target-table project.dataset.dq_results
```

---

**Document Version:** 1.0  
**Created:** 2025-10-12  
**Feature:** DQ Pipeline Placeholder Support
