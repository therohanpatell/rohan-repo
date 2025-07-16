# Metrics Pipeline Framework - Quick Reference

## 🚀 Quick Start

### Basic Command
```bash
python pysaprk.py \
  --gcs_path "gs://bucket/config.json" \
  --run_date "2024-01-15" \
  --dependencies "daily_metrics" \
  --partition_info_table "project.dataset.partition_info" \
  --env "PROD" \
  --recon_table "project.dataset.recon"
```

### Command Arguments Cheat Sheet
| Argument | Example | Description |
|----------|---------|-------------|
| `--gcs_path` | `gs://bucket/config.json` | JSON config file location |
| `--run_date` | `2024-01-15` | Processing date (YYYY-MM-DD) |
| `--dependencies` | `daily,weekly` | Comma-separated dependency list |
| `--partition_info_table` | `project.dataset.partition_info` | Partition metadata table |
| `--env` | `DEV/PROD` | Environment identifier |
| `--recon_table` | `project.dataset.recon` | Reconciliation audit table |

---

## 📋 JSON Configuration Template

### Minimal Configuration
```json
{
  "metric_id": "UNIQUE_METRIC_ID",
  "metric_name": "Human Readable Name",
  "metric_type": "SUM/COUNT/AVG/RATIO",
  "sql": "SELECT metric_output, numerator_value, denominator_value, business_data_date FROM ...",
  "dependency": "dependency_group",
  "target_table": "project.dataset.table"
}
```

### Complete Example
```json
[
  {
    "metric_id": "DAILY_REVENUE_001",
    "metric_name": "Daily Revenue",
    "metric_type": "SUM",
    "sql": "SELECT SUM(amount) as metric_output, COUNT(*) as numerator_value, 1 as denominator_value, '{currently}' as business_data_date FROM `project.dataset.sales` WHERE date = '{currently}'",
    "dependency": "daily_metrics",
    "target_table": "project.dataset.daily_output"
  }
]
```

---

## 🔧 SQL Requirements

### Required Output Columns
```sql
SELECT 
  calculated_value as metric_output,      -- Main metric result
  numerator_count as numerator_value,     -- For ratios/percentages
  denominator_count as denominator_value, -- For ratios/percentages
  '{currently}' as business_data_date     -- Business date
FROM your_table
```

### Placeholder Reference
| Placeholder | Replaced With | Use Case |
|-------------|---------------|----------|
| `{currently}` | `--run_date` value | Current processing date |
| `{partition_info}` | Latest partition from metadata | Dynamic partition lookup |

### SQL Examples
```sql
-- Count metric with current date
SELECT 
  COUNT(*) as metric_output,
  COUNT(*) as numerator_value,
  1 as denominator_value,
  '{currently}' as business_data_date
FROM `project.dataset.orders`
WHERE order_date = '{currently}'

-- Ratio metric with partition info
SELECT 
  SAFE_DIVIDE(SUM(converted), COUNT(*)) as metric_output,
  SUM(converted) as numerator_value,
  COUNT(*) as denominator_value,
  '{partition_info}' as business_data_date
FROM `project.dataset.conversions`
WHERE partition_dt = '{partition_info}'
```

---

## ⚠️ Common Issues & Quick Fixes

### 1. Import Errors
```bash
# Error: "Import pyspark.sql could not be resolved"
pip install pyspark google-cloud-bigquery

# Set environment
export PYTHONPATH="${PYTHONPATH}:/path/to/spark/python"
```

### 2. GCS Path Issues
```bash
# ❌ Wrong
--gcs_path "bucket/file.json"

# ✅ Correct
--gcs_path "gs://bucket/file.json"
```

### 3. JSON Validation Errors
```json
// ❌ Missing required fields
{
  "metric_id": "",
  "sql": null
}

// ✅ Valid
{
  "metric_id": "VALID_001",
  "metric_name": "Valid Metric",
  "metric_type": "SUM",
  "sql": "SELECT ...",
  "dependency": "daily",
  "target_table": "project.dataset.table"
}
```

### 4. Quote Issues in SQL
```sql
-- ❌ Problematic quotes
SELECT * FROM table WHERE name = "John's Data"

-- ✅ Auto-corrected by framework
SELECT * FROM table WHERE name = 'John''s Data'
```

### 5. Partition Info Table Missing
```sql
-- Required table structure
CREATE TABLE project.dataset.partition_info (
  project_dataset STRING,
  table_name STRING,
  partition_dt DATE
);
```

---

## 📊 Monitoring & Logs

### Key Log Messages
| Log Level | Message Pattern | Action |
|-----------|-----------------|---------|
| `INFO` | `Successfully processed metric_id: X` | ✅ Success |
| `WARNING` | `SQL syntax validation failed` | ⚠️ Check SQL |
| `ERROR` | `Pipeline failed:` | ❌ Check error details |
| `INFO` | `Rolling back metric X` | 🔄 Rollback in progress |

### Reconciliation Table Query
```sql
-- Check processing status
SELECT 
  source_system_id as metric_id,
  Job_Name as metric_name,
  rcncln_exact_pass_in as status,
  excldd_reason_tx as error_message,
  load_ts
FROM `project.dataset.recon`
WHERE schdld_dt = '2024-01-15'
ORDER BY load_ts DESC
```

### Pipeline Status Check
```sql
-- Count success/failure
SELECT 
  rcncln_exact_pass_in as status,
  COUNT(*) as count
FROM `project.dataset.recon`
WHERE schdld_dt = '2024-01-15'
GROUP BY rcncln_exact_pass_in
```

---

## 🛠️ Development Tips

### Local Testing
```bash
# Test with small dataset
python pysaprk.py \
  --gcs_path "gs://dev-bucket/test-config.json" \
  --run_date "2024-01-15" \
  --dependencies "test_metrics" \
  --partition_info_table "dev-project.test.partition_info" \
  --env "DEV" \
  --recon_table "dev-project.test.recon"
```

### Debug Mode
```python
# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
```

### Schema Validation
```python
# Check if target table exists
from google.cloud import bigquery
client = bigquery.Client()
table = client.get_table("project.dataset.table")
print(table.schema)
```

---

## 🎯 Best Practices Checklist

### Before Running
- [ ] JSON config validated
- [ ] All target tables exist
- [ ] Partition info table populated
- [ ] SQL queries tested manually
- [ ] Dependencies correctly grouped

### JSON Configuration
- [ ] Unique metric IDs
- [ ] Required fields present
- [ ] SQL returns required columns
- [ ] Placeholders used correctly
- [ ] Table names fully qualified

### SQL Queries
- [ ] Handle NULL values with COALESCE
- [ ] Use SAFE_DIVIDE for ratios
- [ ] Include proper date filters
- [ ] Test with sample data
- [ ] Validate output columns

### Deployment
- [ ] Test in DEV environment first
- [ ] Check recon table after run
- [ ] Monitor logs for errors
- [ ] Verify target table data
- [ ] Document any issues

---

## 📞 Quick Support

### Debug Steps
1. Check logs for specific error messages
2. Validate JSON configuration format
3. Test SQL queries manually in BigQuery
4. Verify GCS path accessibility
5. Check partition info table data

### Common Commands
```bash
# View recent logs
tail -f /path/to/logs/pipeline.log

# Check GCS file
gsutil cat gs://bucket/config.json | head -10

# Test BigQuery access
bq query --use_legacy_sql=false "SELECT 1"

# Check table schema
bq show --schema project:dataset.table
```

### Error Message Lookup
| Error Pattern | Common Cause | Solution |
|---------------|--------------|----------|
| `MetricsPipelineError` | Business logic issue | Check JSON config |
| `NotFound` | Table doesn't exist | Create target table |
| `Invalid date format` | Wrong date format | Use YYYY-MM-DD |
| `Duplicate metric_id` | Non-unique IDs | Make IDs unique |
| `Unbalanced quotes` | SQL syntax issue | Let framework handle |

---

## 📈 Performance Tips

### Optimize SQL Queries
```sql
-- ✅ Use partitioned tables
WHERE partition_dt = '{currently}'

-- ✅ Limit data scanned
WHERE date BETWEEN '{currently}' AND '{currently}'

-- ✅ Use appropriate aggregations
SELECT COUNT(*) -- instead of COUNT(column)
```

### Batch Processing
```bash
# Process related metrics together
--dependencies "daily_financial,daily_operational"

# Avoid processing everything at once
--dependencies "all_metrics"  # ❌
```

### Monitor Resources
- Watch Spark executor memory
- Check BigQuery slot usage
- Monitor GCS read throughput
- Track processing time per metric

---

## 🧠 Beginner's Code Concepts

### Understanding Key Code Patterns

```python
# Context Manager Pattern
with managed_spark_session() as spark:
    # spark is available here
    pass
# spark is automatically cleaned up

# Decorator Pattern
@contextmanager
def managed_spark_session():
    # Setup code
    yield resource  # Provide resource
    # Cleanup code

# Error Handling Pattern
try:
    result = risky_operation()
except SpecificError as e:
    logger.error(f"Operation failed: {e}")
    raise CustomError("Friendly error message")
```

### Common Python Constructs

```python
# List comprehension
new_list = [item for item in old_list if condition]

# Dictionary comprehension
new_dict = {k: v for k, v in old_dict.items() if condition}

# Generator expression
data = (item for item in large_dataset if condition)

# String formatting
message = f"Processing {count} items at {timestamp}"
```

### PySpark Basics

```python
# Create DataFrame
df = spark.createDataFrame(data, schema)

# Transform DataFrame
df_filtered = df.filter(col("date") == "2024-01-15")
df_grouped = df.groupBy("category").sum("amount")

# Collect results (brings to driver)
results = df.collect()  # Use carefully with large data

# Show results (for debugging)
df.show(10)  # Shows first 10 rows
```

### BigQuery SQL Essentials

```sql
-- Safe operations
SAFE_DIVIDE(numerator, denominator)  -- Returns NULL if denominator is 0
COALESCE(value1, value2, default)    -- Returns first non-NULL value

-- Date functions
DATE(timestamp_column)               -- Extract date part
FORMAT_DATE('%Y-%m-%d', date_col)    -- Format date as string
PARSE_DATE('%Y-%m-%d', '2024-01-15') -- Parse string to date

-- Aggregations
COUNT(*)                 -- Count all rows
COUNT(column)            -- Count non-NULL values
SUM(column)              -- Sum numeric values
AVG(column)              -- Average of numeric values
```

---

## 🔤 Glossary for Freshers

| Term | Definition | Example |
|------|------------|---------|
| **DataFrame** | Distributed table-like data structure | `df = spark.createDataFrame(data)` |
| **Schema** | Definition of data structure and types | `StructType([StructField("name", StringType())])` |
| **Partition** | Data division for performance | `WHERE partition_dt = '2024-01-15'` |
| **Context Manager** | Resource management pattern | `with open(file) as f:` |
| **Decorator** | Function that modifies other functions | `@contextmanager` |
| **Yield** | Generator function keyword | `yield value` |
| **UUID** | Universally unique identifier | `uuid.uuid4()` |
| **Reconciliation** | Data validation and audit process | Recon table records |
| **Placeholder** | Template variable in SQL | `{currently}` |
| **Rollback** | Undo database changes | Delete inserted records |

---

## 🚨 Common Beginner Mistakes

### ❌ What NOT to Do

```python
# Don't use print() in production
print("Processing metric...")  # ❌

# Don't ignore exceptions
try:
    result = operation()
except:
    pass  # ❌ Silent failure

# Don't use string concatenation for SQL
sql = "SELECT * FROM table WHERE date = " + date  # ❌ SQL injection risk
```

### ✅ What TO Do

```python
# Use logging instead of print
logger.info("Processing metric...")  # ✅

# Handle exceptions properly
try:
    result = operation()
except SpecificError as e:
    logger.error(f"Operation failed: {e}")
    raise  # ✅ Re-raise or handle appropriately

# Use parameterized queries
sql = "SELECT * FROM table WHERE date = %s"  # ✅ Safe
```

### JSON Configuration Mistakes

```json
// ❌ Don't do this
{
  "metric_id": "",                    // Empty string
  "sql": "SELECT metric_output",     // Missing required columns
  "target_table": "table_name"       // Missing project.dataset
}

// ✅ Do this instead
{
  "metric_id": "UNIQUE_ID_001",
  "sql": "SELECT metric_output, numerator_value, denominator_value, business_data_date FROM...",
  "target_table": "project.dataset.table"
}
```

---

## 📚 Learning Resources

### Understanding the Framework
1. **Start here:** Read `METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md`
2. **Practice:** Try examples in the documentation
3. **Experiment:** Use DEV environment for testing
4. **Ask questions:** Check expanded FAQ section

### Python Concepts to Learn
- **Context managers** (`with` statements)
- **Decorators** (`@` symbols)
- **Exception handling** (`try/except`)
- **List/dict comprehensions**
- **String formatting** (`f"text {variable}"`)

### SQL Concepts to Learn
- **Window functions** (`ROW_NUMBER()`, `RANK()`)
- **CTEs** (`WITH` clauses)
- **Joins** (`INNER`, `LEFT`, `RIGHT`)
- **Aggregations** (`GROUP BY`, `HAVING`)
- **Date functions** (`DATE()`, `TIMESTAMP()`)

### BigQuery Specifics
- **Standard SQL** vs Legacy SQL
- **Partitioning** and **clustering**
- **Cost optimization** techniques
- **IAM permissions** and security

---

*This quick reference complements the comprehensive documentation. For detailed explanations, see `METRICS_PIPELINE_FRAMEWORK_DOCUMENTATION.md`.* 