# Metrics Pipeline Framework Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Getting Started](#getting-started)
5. [Configuration](#configuration)
6. [Core Components](#core-components)
7. [Usage Examples](#usage-examples)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)
10. [Advanced Features](#advanced-features)
11. [FAQ](#faq)

---

## Overview

The Metrics Pipeline Framework is a robust, production-ready data processing system built on PySpark that integrates with Google Cloud Platform (GCP) services. It's designed to:

- **Process business metrics** from various data sources
- **Handle complex SQL transformations** with dynamic placeholder replacement
- **Integrate seamlessly** with BigQuery and Google Cloud Storage
- **Provide comprehensive error handling** and rollback capabilities
- **Generate audit trails** through reconciliation records
- **Support quote normalization** for SQL compatibility

### Key Benefits
- ✅ **Scalable**: Built on PySpark for distributed processing
- ✅ **Robust**: Comprehensive error handling and rollback mechanisms
- ✅ **Flexible**: Dynamic SQL placeholder replacement
- ✅ **Auditable**: Full reconciliation and logging capabilities
- ✅ **Production-ready**: Extensive validation and schema alignment

---

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   JSON Config   │    │   Partition     │    │   BigQuery      │
│   (GCS Bucket)  │    │   Info Table    │    │   Target Tables │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Metrics Pipeline Framework                     │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │
│  │   JSON      │  │    SQL      │  │   Schema    │  │  Recon  │ │
│  │ Validation  │  │ Processing  │  │ Alignment   │  │ Records │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘ │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │
│  │   Quote     │  │ Placeholder │  │   Error     │  │ Rollback│ │
│  │ Handling    │  │ Replacement │  │ Handling    │  │ Manager │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘ │
└──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────┐
                    │   Recon Table   │
                    │   (BigQuery)    │
                    └─────────────────┘
```

### Data Flow
1. **Input**: JSON configuration from GCS containing metric definitions
2. **Processing**: SQL execution with dynamic parameter replacement
3. **Validation**: Schema alignment and data validation
4. **Output**: Metrics written to BigQuery target tables
5. **Audit**: Reconciliation records written to recon table

---

## Prerequisites

### Software Requirements
- **Python 3.8+**
- **PySpark 3.x**
- **Google Cloud SDK**
- **BigQuery Client Library**

### GCP Setup
```bash
# Install required libraries
pip install pyspark google-cloud-bigquery google-cloud-storage

# Set up authentication
gcloud auth application-default login
```

### Permissions Required
- **BigQuery**: Read/Write access to target tables and recon table
- **GCS**: Read access to JSON configuration files
- **Compute**: Spark cluster or local Spark environment

---

## Getting Started

### Basic Usage

```bash
python pysaprk.py \
  --gcs_path "gs://your-bucket/config/metrics.json" \
  --run_date "2024-01-15" \
  --dependencies "daily_metrics,weekly_metrics" \
  --partition_info_table "project.dataset.partition_info" \
  --env "DEV" \
  --recon_table "project.dataset.recon_table"
```

### Command Line Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `--gcs_path` | Yes | GCS path to JSON configuration file | `gs://bucket/config.json` |
| `--run_date` | Yes | Processing date in YYYY-MM-DD format | `2024-01-15` |
| `--dependencies` | Yes | Comma-separated list of dependencies | `daily,weekly` |
| `--partition_info_table` | Yes | BigQuery table for partition metadata | `project.dataset.partition_info` |
| `--env` | Yes | Environment identifier | `DEV`, `PROD` |
| `--recon_table` | Yes | BigQuery table for reconciliation records | `project.dataset.recon` |

---

## Configuration

### JSON Configuration Structure

The framework expects a JSON file with the following structure:

```json
[
  {
    "metric_id": "DAILY_SALES_001",
    "metric_name": "Daily Sales Revenue",
    "metric_type": "SUM",
    "sql": "SELECT SUM(amount) as metric_output, COUNT(*) as numerator_value, 1 as denominator_value, '{currently}' as business_data_date FROM `project.dataset.sales` WHERE date = '{currently}'",
    "dependency": "daily_metrics",
    "target_table": "project.dataset.daily_metrics_output"
  },
  {
    "metric_id": "WEEKLY_ORDERS_002",
    "metric_name": "Weekly Order Count",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, COUNT(*) as numerator_value, 1 as denominator_value, '{partition_info}' as business_data_date FROM `project.dataset.orders` WHERE date = '{partition_info}'",
    "dependency": "weekly_metrics",
    "target_table": "project.dataset.weekly_metrics_output"
  }
]
```

### Required Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `metric_id` | String | Unique identifier for the metric | `DAILY_SALES_001` |
| `metric_name` | String | Human-readable metric name | `Daily Sales Revenue` |
| `metric_type` | String | Type of metric calculation | `SUM`, `COUNT`, `AVG` |
| `sql` | String | SQL query with placeholders | See SQL examples below |
| `dependency` | String | Dependency group for processing | `daily_metrics` |
| `target_table` | String | BigQuery table for output | `project.dataset.table` |

### SQL Query Requirements

Your SQL queries must return these columns:
- `metric_output`: The calculated metric value
- `numerator_value`: Numerator for ratio calculations
- `denominator_value`: Denominator for ratio calculations
- `business_data_date`: The business date for the metric

### SQL Placeholders

The framework supports dynamic placeholder replacement:

| Placeholder | Description | Replaced With |
|-------------|-------------|---------------|
| `{currently}` | Current run date | The `--run_date` parameter |
| `{partition_info}` | Latest partition date | Retrieved from partition_info_table |

#### Example SQL with Placeholders

```sql
-- Basic example with {currently}
SELECT 
  SUM(revenue) as metric_output,
  COUNT(*) as numerator_value,
  1 as denominator_value,
  '{currently}' as business_data_date
FROM `project.dataset.sales`
WHERE date = '{currently}'

-- Example with {partition_info}
SELECT 
  COUNT(DISTINCT customer_id) as metric_output,
  COUNT(DISTINCT customer_id) as numerator_value,
  1 as denominator_value,
  '{partition_info}' as business_data_date
FROM `project.dataset.customer_data`
WHERE partition_dt = '{partition_info}'
```

---

## Core Components

### 1. MetricsPipeline Class

The main orchestrator that handles the entire pipeline flow.

```python
# Initialize pipeline
pipeline = MetricsPipeline(spark, bq_client)

# Process metrics
metrics_dfs, successful_metrics, failed_metrics = pipeline.process_metrics(
    json_data, run_date, dependencies, partition_info_table
)
```

### 2. Quote Handling System

Automatically normalizes SQL quotes for BigQuery compatibility.

#### What it does:
- Converts double quotes to single quotes for string literals
- Preserves backticks for table/column identifiers
- Handles nested quotes and escaping

#### Example:
```python
# Input SQL with double quotes
sql = 'SELECT * FROM table WHERE status = "active"'

# Automatically converted to:
sql = "SELECT * FROM table WHERE status = 'active'"
```

### 3. Schema Alignment

Ensures Spark DataFrames match BigQuery table schemas.

```python
# Automatic schema alignment
aligned_df = pipeline.align_schema_with_bq(df, target_table)
```

### 4. Error Handling and Rollback

Comprehensive error handling with automatic rollback capabilities.

```python
try:
    # Process metrics
    process_metrics()
except MetricsPipelineError as e:
    # Automatic rollback of processed metrics
    pipeline.rollback_all_processed_metrics(partition_dt)
```

### 5. Reconciliation Records

Generates audit trail for every metric processed.

```python
# Automatically creates recon records
recon_records = pipeline.create_recon_records_from_write_results(
    validated_data, run_date, dependencies, partition_info_table,
    env, successful_writes, failed_metrics, partition_dt
)
```

---

## Usage Examples

### Example 1: Basic Daily Metrics

**JSON Configuration:**
```json
[
  {
    "metric_id": "DAILY_REVENUE_001",
    "metric_name": "Daily Revenue",
    "metric_type": "SUM",
    "sql": "SELECT SUM(amount) as metric_output, COUNT(*) as numerator_value, 1 as denominator_value, '{currently}' as business_data_date FROM `myproject.sales.transactions` WHERE DATE(created_at) = '{currently}'",
    "dependency": "daily_metrics",
    "target_table": "myproject.metrics.daily_output"
  }
]
```

**Command:**
```bash
python pysaprk.py \
  --gcs_path "gs://my-bucket/configs/daily_metrics.json" \
  --run_date "2024-01-15" \
  --dependencies "daily_metrics" \
  --partition_info_table "myproject.metadata.partition_info" \
  --env "PROD" \
  --recon_table "myproject.audit.recon"
```

### Example 2: Multiple Dependencies

**JSON Configuration:**
```json
[
  {
    "metric_id": "DAILY_ORDERS_001",
    "metric_name": "Daily Order Count",
    "metric_type": "COUNT",
    "sql": "SELECT COUNT(*) as metric_output, COUNT(*) as numerator_value, 1 as denominator_value, '{currently}' as business_data_date FROM `myproject.orders.daily` WHERE order_date = '{currently}'",
    "dependency": "daily_metrics",
    "target_table": "myproject.metrics.daily_output"
  },
  {
    "metric_id": "WEEKLY_SUMMARY_001",
    "metric_name": "Weekly Sales Summary",
    "metric_type": "SUM",
    "sql": "SELECT SUM(total_amount) as metric_output, COUNT(*) as numerator_value, 1 as denominator_value, '{partition_info}' as business_data_date FROM `myproject.sales.weekly_summary` WHERE week_ending = '{partition_info}'",
    "dependency": "weekly_metrics",
    "target_table": "myproject.metrics.weekly_output"
  }
]
```

**Command:**
```bash
python pysaprk.py \
  --gcs_path "gs://my-bucket/configs/mixed_metrics.json" \
  --run_date "2024-01-15" \
  --dependencies "daily_metrics,weekly_metrics" \
  --partition_info_table "myproject.metadata.partition_info" \
  --env "PROD" \
  --recon_table "myproject.audit.recon"
```

### Example 3: Complex SQL with Ratios

**JSON Configuration:**
```json
[
  {
    "metric_id": "CONVERSION_RATE_001",
    "metric_name": "Daily Conversion Rate",
    "metric_type": "RATIO",
    "sql": "SELECT SAFE_DIVIDE(SUM(CASE WHEN converted = true THEN 1 ELSE 0 END), COUNT(*)) as metric_output, SUM(CASE WHEN converted = true THEN 1 ELSE 0 END) as numerator_value, COUNT(*) as denominator_value, '{currently}' as business_data_date FROM `myproject.analytics.user_sessions` WHERE session_date = '{currently}'",
    "dependency": "daily_analytics",
    "target_table": "myproject.metrics.conversion_metrics"
  }
]
```

---

## Best Practices

### 1. JSON Configuration Best Practices

```json
{
  "metric_id": "CLEAR_NAMING_001",           // Use descriptive, unique IDs
  "metric_name": "Clear Metric Description", // Human-readable names
  "metric_type": "SUM",                      // Consistent type naming
  "sql": "SELECT...",                        // Well-formatted SQL
  "dependency": "logical_grouping",          // Group related metrics
  "target_table": "project.dataset.table"   // Full table names
}
```

### 2. SQL Best Practices

```sql
-- ✅ Good: Clear column aliases
SELECT 
  SUM(revenue) as metric_output,
  COUNT(*) as numerator_value,
  1 as denominator_value,
  '{currently}' as business_data_date
FROM `project.dataset.sales`
WHERE date = '{currently}'

-- ❌ Bad: Missing required columns
SELECT SUM(revenue) FROM sales

-- ✅ Good: Handle NULL values
SELECT 
  COALESCE(SUM(revenue), 0) as metric_output,
  COUNT(*) as numerator_value,
  1 as denominator_value,
  '{currently}' as business_data_date
FROM `project.dataset.sales`
WHERE date = '{currently}'
```

### 3. Error Handling Best Practices

```python
# Always use try-catch for pipeline operations
try:
    pipeline.process_metrics(data, run_date, dependencies, partition_table)
except MetricsPipelineError as e:
    logger.error(f"Pipeline failed: {e}")
    # Automatic rollback will occur
```

### 4. Dependency Management

```bash
# Group related metrics logically
--dependencies "daily_financial,daily_operational"

# Avoid overly broad dependencies
--dependencies "all_metrics"  # ❌ Bad

# Use specific, meaningful names
--dependencies "customer_metrics,product_metrics"  # ✅ Good
```

### 5. Monitoring and Logging

The framework provides comprehensive logging:

```python
# Pipeline automatically logs:
# - Quote conversions performed
# - SQL placeholder replacements
# - Schema alignments
# - Reconciliation records
# - Error details and rollback actions
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. **"Import pyspark.sql could not be resolved"**
```bash
# Solution: Install PySpark
pip install pyspark

# Or set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/path/to/spark/python"
```

#### 2. **"Invalid GCS path format"**
```bash
# ❌ Wrong
--gcs_path "bucket/file.json"

# ✅ Correct
--gcs_path "gs://bucket/file.json"
```

#### 3. **"Missing required field 'metric_id'"**
Check your JSON configuration:
```json
{
  "metric_id": "",  // ❌ Empty string
  "metric_name": "Test Metric",
  // ...
}

// ✅ Correct
{
  "metric_id": "VALID_METRIC_001",
  "metric_name": "Test Metric",
  // ...
}
```

#### 4. **"Duplicate metric_id found"**
Ensure all metric IDs are unique in your JSON:
```json
[
  {"metric_id": "METRIC_001", ...},
  {"metric_id": "METRIC_001", ...}  // ❌ Duplicate
]
```

#### 5. **"Could not determine partition_dt"**
Check your partition_info_table:
```sql
-- Required structure
CREATE TABLE project.dataset.partition_info (
  project_dataset STRING,
  table_name STRING,
  partition_dt DATE
);
```

#### 6. **"Unbalanced quotes in SQL"**
The framework handles most quote issues automatically, but check for:
```sql
-- ❌ Unbalanced quotes
SELECT * FROM table WHERE name = 'John's Data"

-- ✅ Properly escaped
SELECT * FROM table WHERE name = 'John''s Data'
```

### Debug Mode

Enable debug logging by modifying the logging level:
```python
logging.basicConfig(level=logging.DEBUG)
```

### Common Log Messages

| Log Level | Message Pattern | Meaning |
|-----------|-----------------|---------|
| INFO | `Found X placeholders in SQL` | Placeholder replacement working |
| WARNING | `SQL syntax validation failed` | SQL may have issues |
| ERROR | `Pipeline failed:` | Critical error occurred |
| INFO | `Successfully wrote X records` | Data written successfully |

---

## Advanced Features

### 1. Custom Error Handling

```python
class CustomMetricError(MetricsPipelineError):
    """Custom error for specific business logic"""
    pass

# Use in your extensions
if business_rule_violation:
    raise CustomMetricError("Business rule violated")
```

### 2. Schema Validation

```python
# Framework automatically validates:
# - Required fields presence
# - Data types compatibility
# - Table format (project.dataset.table)
# - SQL syntax (basic validation)
```

### 3. Rollback Strategies

```python
# Automatic rollback options:
# 1. Rollback only new metrics (default)
# 2. Skip rollback for overwritten metrics
# 3. Custom rollback logic

pipeline.rollback_all_processed_metrics(partition_dt)
```

### 4. Quote Conversion Reporting

```python
# Get detailed quote conversion report
quote_report = pipeline.get_quote_conversion_report()
print(f"Total conversions: {quote_report['total_conversions']}")
```

### 5. Batch Processing

```python
# Process multiple dependency groups
dependencies = ["daily_metrics", "weekly_metrics", "monthly_metrics"]
for dep_group in dependencies:
    pipeline.process_metrics(data, run_date, [dep_group], partition_table)
```

---

## FAQ

### General Framework Questions

### Q: What happens if a metric already exists?
**A:** The framework automatically detects existing metrics and overwrites them. Original data is not recoverable, but the action is logged in reconciliation records.

### Q: Can I use the framework with other cloud providers?
**A:** Currently, the framework is designed for GCP (BigQuery, GCS). Adapting to other providers would require significant modifications to the storage and query components.

### Q: How do I handle large datasets?
**A:** The framework uses PySpark for distributed processing. Configure your Spark cluster with appropriate resources:
```python
spark = SparkSession.builder \
    .appName("MetricsPipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

### Q: What if my SQL query returns no results?
**A:** The framework will log a warning and continue processing other metrics. The metric will be marked as failed in reconciliation records.

### Q: Can I process metrics in parallel?
**A:** Metrics within the same dependency group are processed sequentially for consistency. Different dependency groups can be processed in separate pipeline runs.

### Q: How do I monitor pipeline performance?
**A:** Check the logs for:
- Processing times per metric
- Schema alignment duration
- BigQuery write performance
- Error rates and rollback actions

### Q: What's the maximum number of metrics I can process?
**A:** There's no hard limit, but consider:
- Spark cluster resources
- BigQuery quotas and limits
- GCS file size limits
- Network bandwidth

### Q: How do I handle time zones?
**A:** The framework uses UTC timestamps. Ensure your `run_date` parameter and SQL queries handle time zones consistently.

### Q: Can I extend the framework?
**A:** Yes! The framework is designed to be extensible. You can:
- Add custom validation logic
- Extend the MetricsPipeline class
- Add new placeholder types
- Implement custom error handling

---

### Code Understanding Questions (For Freshers)

### Q: What is PySpark and why do we use it?
**A:** PySpark is Python's interface to Apache Spark, a distributed computing framework. We use it because:
- **Scalability**: Can process large datasets across multiple machines
- **Performance**: Optimized for big data processing
- **Integration**: Works well with BigQuery and other GCP services
- **Fault tolerance**: Automatically handles failures in distributed processing

### Q: What are decorators like `@contextmanager` in the code?
**A:** Decorators modify function behavior. `@contextmanager` creates a context manager for resource management:
```python
@contextmanager
def managed_spark_session():
    spark = SparkSession.builder.getOrCreate()
    try:
        yield spark  # Provide spark session
    finally:
        spark.stop()  # Always cleanup

# Usage:
with managed_spark_session() as spark:
    # Use spark here
    pass
# Spark automatically cleaned up
```

### Q: What does `self._handle_operation()` do?
**A:** It's a wrapper method that provides consistent error handling across all operations:
```python
def _handle_operation(self, operation, operation_name, *args, **kwargs):
    try:
        return operation(*args, **kwargs)
    except Exception as e:
        error_msg = f"{operation_name} failed: {str(e)}"
        logger.error(error_msg)
        raise MetricsPipelineError(error_msg)
```
This ensures all errors are logged consistently and converted to our custom exception type.

### Q: Why do we use `StructType` and `StructField`?
**A:** These define the schema (structure) of DataFrames in PySpark:
```python
schema = StructType([
    StructField("metric_id", StringType(), False),      # False = not nullable
    StructField("metric_output", DecimalType(38, 9), True)  # True = nullable
])
```
This ensures data types are consistent and prevents runtime errors.

### Q: What is the difference between `DataFrame` and regular Python data structures?
**A:** 
- **DataFrame**: Distributed, structured data that can be processed across multiple machines
- **Regular Python list/dict**: Single-machine, in-memory data structures
- **DataFrames** are optimized for big data operations and can handle datasets larger than memory

### Q: Why do we use `uuid.uuid4()` for `execution_id`?
**A:** It generates a unique identifier for each pipeline run:
```python
self.execution_id = str(uuid.uuid4())
# Example: "550e8400-e29b-41d4-a716-446655440000"
```
This helps track and debug specific pipeline executions.

### Q: What does `yield` do in the context manager?
**A:** `yield` pauses the function and returns control to the caller, then resumes after the `with` block:
```python
@contextmanager
def managed_spark_session():
    spark = SparkSession.builder.getOrCreate()  # Setup
    try:
        yield spark  # Pause here, return spark to caller
    finally:
        spark.stop()  # Resume here when 'with' block ends
```

### Q: What are `*args` and `**kwargs`?
**A:** 
- `*args`: Accepts any number of positional arguments as a tuple
- `**kwargs`: Accepts any number of keyword arguments as a dictionary
```python
def flexible_function(*args, **kwargs):
    print(f"Args: {args}")      # Args: (1, 2, 3)
    print(f"Kwargs: {kwargs}")  # Kwargs: {'name': 'John', 'age': 30}

flexible_function(1, 2, 3, name="John", age=30)
```

### Q: Why do we use `logging` instead of `print()`?
**A:** Logging provides better control and features:
- **Levels**: INFO, WARNING, ERROR, DEBUG
- **Formatting**: Timestamps, module names, etc.
- **Output control**: Can redirect to files, suppress certain levels
- **Production ready**: Better for monitoring and debugging

### Q: What is `bigquery.Client()` and how does it work?
**A:** It's the main interface to interact with Google BigQuery:
```python
client = bigquery.Client()
query = "SELECT * FROM table"
results = client.query(query).result()  # Execute query
```
It handles authentication, query execution, and result retrieval.

---

### Development and Setup Questions

### Q: How do I set up my development environment?
**A:** Follow these steps:
```bash
# 1. Install Python 3.8+
python --version

# 2. Install required packages
pip install pyspark google-cloud-bigquery google-cloud-storage

# 3. Set up Google Cloud authentication
gcloud auth application-default login

# 4. Set environment variables (optional)
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export PYTHONPATH="${PYTHONPATH}:/path/to/spark/python"
```

### Q: How do I test my changes locally?
**A:** Use a development environment:
```bash
# Create test data
python pysaprk.py \
  --gcs_path "gs://dev-bucket/test-config.json" \
  --run_date "2024-01-15" \
  --dependencies "test_metrics" \
  --partition_info_table "dev-project.test.partition_info" \
  --env "DEV" \
  --recon_table "dev-project.test.recon"
```

### Q: How do I debug issues in the pipeline?
**A:** Enable debug logging and check specific areas:
```python
# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Check specific components
logger.debug(f"Processing metric: {metric_id}")
logger.debug(f"SQL query: {sql}")
logger.debug(f"Results: {results}")
```

### Q: What files should I modify to add new features?
**A:** The main file is `pysaprk.py`. Common modification areas:
- **New placeholder types**: Modify `PLACEHOLDER_PATTERNS` and `replace_sql_placeholders()`
- **New validation rules**: Add to `validate_json()` or `_validate_field_content()`
- **New error handling**: Extend `MetricsPipelineError` or add new exception types
- **New output formats**: Modify `_create_dataframe_schema()` and related methods

### Q: How do I understand the error messages?
**A:** Error messages follow patterns:
```python
# Pattern: "Operation failed: specific error details"
"SQL execution failed: Table not found: project.dataset.table"
"JSON validation failed: Missing required field 'metric_id'"
"Schema alignment failed: Column 'metric_output' not found"
```
The first part tells you what operation failed, the second part gives specific details.

---

### Data and SQL Questions

### Q: Why do we need `numerator_value` and `denominator_value`?
**A:** They support ratio and percentage calculations:
```sql
-- For conversion rate (successful/total)
SELECT 
  SAFE_DIVIDE(SUM(converted), COUNT(*)) as metric_output,
  SUM(converted) as numerator_value,    -- Successes
  COUNT(*) as denominator_value         -- Total attempts
FROM conversions
```

### Q: What is `SAFE_DIVIDE()` and why use it?
**A:** It prevents division by zero errors:
```sql
-- Regular division (can cause errors)
SELECT 10 / 0  -- ERROR: division by zero

-- Safe division (returns NULL for zero)
SELECT SAFE_DIVIDE(10, 0)  -- Returns NULL
```

### Q: Why do we use `COALESCE()` in SQL?
**A:** It handles NULL values by providing defaults:
```sql
-- Without COALESCE (can return NULL)
SELECT SUM(revenue) FROM sales WHERE date = '2024-01-01'

-- With COALESCE (ensures non-NULL result)
SELECT COALESCE(SUM(revenue), 0) FROM sales WHERE date = '2024-01-01'
```

### Q: What does `DATE()` function do?
**A:** It extracts the date part from a timestamp:
```sql
-- Convert timestamp to date
SELECT DATE(created_at) FROM orders
-- 2024-01-15 14:30:25 → 2024-01-15
```

### Q: How do I handle different date formats?
**A:** Use BigQuery's date functions:
```sql
-- Parse string to date
SELECT PARSE_DATE('%Y-%m-%d', '2024-01-15')

-- Format date to string
SELECT FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())

-- Convert timestamp to date
SELECT DATE(timestamp_column)
```

### Q: What are backticks (`) in SQL for?
**A:** They quote identifiers (table names, column names) that might be reserved words or contain special characters:
```sql
-- Required for project.dataset.table format
SELECT * FROM `my-project.my_dataset.my_table`

-- Required for columns with special characters
SELECT `weird-column-name` FROM table

-- Required for reserved words
SELECT `order` FROM `table`
```

---

### Error Handling Questions

### Q: What happens when the pipeline fails?
**A:** The framework has multiple failure handling levels:
1. **Individual metric failure**: Logs error, continues with other metrics
2. **Table write failure**: Logs error, continues with other tables
3. **Configuration failure**: Stops pipeline immediately
4. **Critical system failure**: Triggers rollback of processed metrics

### Q: How does the rollback mechanism work?
**A:** It removes newly inserted metrics but preserves overwritten ones:
```python
# Only rollback metrics that were newly inserted
new_metrics = [mid for mid in processed_metrics if mid not in overwritten_metrics]

# Delete from target tables
DELETE FROM target_table 
WHERE metric_id IN (new_metrics) 
AND partition_dt = 'current_date'
```

### Q: What is `MetricsPipelineError` and when is it raised?
**A:** It's our custom exception class for pipeline-specific errors:
```python
class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass

# Raised for business logic errors
if denominator == 0:
    raise MetricsPipelineError("Division by zero in metric calculation")
```

### Q: How do I handle missing data gracefully?
**A:** Use defensive programming techniques:
```python
# Check for None/empty values
if not value or value.strip() == "":
    logger.warning(f"Empty value for field: {field_name}")
    return default_value

# Use try-catch for conversions
try:
    return Decimal(value)
except (ValueError, TypeError):
    logger.warning(f"Could not convert to Decimal: {value}")
    return None
```

---

### Performance and Optimization Questions

### Q: How do I make my SQL queries faster?
**A:** Follow these optimization patterns:
```sql
-- ✅ Use partitioned columns in WHERE clauses
WHERE partition_dt = '{currently}'

-- ✅ Limit data scanned with specific date ranges
WHERE date BETWEEN '{start_date}' AND '{end_date}'

-- ✅ Use appropriate aggregations
SELECT COUNT(*) instead of COUNT(column_name)

-- ✅ Use indexes and clustered columns
-- (Set up during table creation)
```

### Q: Why does the pipeline use Spark instead of running SQL directly?
**A:** Spark provides several advantages:
- **Distributed processing**: Can handle datasets larger than single machine memory
- **Data transformation**: Better for complex ETL operations
- **Fault tolerance**: Automatically retries failed operations
- **Integration**: Works well with both BigQuery and GCS

### Q: How do I monitor memory usage?
**A:** Check Spark configurations and logs:
```python
# Configure Spark for your resources
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

### Q: What's the difference between `append` and `overwrite` modes?
**A:** They determine how data is written to target tables:
- **append**: Adds new data to existing table (our default)
- **overwrite**: Replaces all data in table (dangerous for production)
```python
df.write.mode("append").save()    # Adds to existing data
df.write.mode("overwrite").save() # Replaces all data
```

---

### Testing and Validation Questions

### Q: How do I test my SQL queries before adding them to the pipeline?
**A:** Test them manually in BigQuery console:
```sql
-- Test with actual date values first
SELECT 
  SUM(revenue) as metric_output,
  COUNT(*) as numerator_value,
  1 as denominator_value,
  '2024-01-15' as business_data_date
FROM `project.dataset.sales`
WHERE date = '2024-01-15'
```

### Q: How do I validate my JSON configuration?
**A:** Use online JSON validators and check required fields:
```json
{
  "metric_id": "TEST_001",           // ✅ Non-empty string
  "metric_name": "Test Metric",      // ✅ Descriptive name
  "metric_type": "SUM",              // ✅ Valid type
  "sql": "SELECT ...",               // ✅ Valid SQL
  "dependency": "test_group",        // ✅ Logical grouping
  "target_table": "project.dataset.table"  // ✅ Full table name
}
```

### Q: What should I check in reconciliation records?
**A:** Look for these key fields:
```sql
SELECT 
  source_system_id,        -- Your metric_id
  rcncln_exact_pass_in,    -- 'Passed' or 'Failed'
  excldd_reason_tx,        -- Error message if failed
  load_ts                  -- When it was processed
FROM recon_table
WHERE schdld_dt = '2024-01-15'
```

### Q: How do I create test data?
**A:** Create small, focused test cases:
```json
[
  {
    "metric_id": "TEST_SIMPLE_001",
    "metric_name": "Simple Count Test",
    "metric_type": "COUNT",
    "sql": "SELECT 1 as metric_output, 1 as numerator_value, 1 as denominator_value, '{currently}' as business_data_date",
    "dependency": "test",
    "target_table": "test-project.test.output"
  }
]
```

---

### Deployment and Production Questions

### Q: How do I deploy changes to production?
**A:** Follow these steps:
1. **Test in DEV environment** with sample data
2. **Code review** with team members
3. **Backup production data** before deployment
4. **Deploy during low-traffic periods**
5. **Monitor logs** immediately after deployment

### Q: What environment variables should I set?
**A:** Common environment variables:
```bash
# Authentication
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Python path (if needed)
export PYTHONPATH="${PYTHONPATH}:/path/to/spark/python"

# Spark configuration
export SPARK_HOME="/path/to/spark"
export PYSPARK_PYTHON="python3"
```

### Q: How do I handle different environments (DEV/PROD)?
**A:** Use environment-specific configurations:
```bash
# DEV environment
--env "DEV"
--partition_info_table "dev-project.test.partition_info"
--recon_table "dev-project.test.recon"

# PROD environment
--env "PROD"
--partition_info_table "prod-project.metadata.partition_info"
--recon_table "prod-project.audit.recon"
```

### Q: What should I monitor in production?
**A:** Key metrics to track:
- **Success rate**: Percentage of successful metrics
- **Processing time**: How long each run takes
- **Error patterns**: Common failure reasons
- **Resource usage**: Memory and CPU consumption
- **Data quality**: Unexpected nulls or outliers

### Q: How do I handle sensitive data?
**A:** Follow these security practices:
- **Never log sensitive data** (PII, financial info)
- **Use IAM permissions** to control access
- **Encrypt data in transit and at rest**
- **Audit access** through reconciliation records
- **Use service accounts** instead of personal credentials

---

## Getting Help

### Logging and Debugging
- Enable DEBUG logging for detailed information
- Check reconciliation records for processing history
- Review quote conversion reports for SQL issues

### Support Resources
- Check the comprehensive logging output
- Review the troubleshooting section
- Examine the reconciliation table for detailed error messages
- Use the quote conversion report for SQL debugging

### Best Practices for Support
1. Include full error logs
2. Provide sample JSON configuration
3. Share the exact command used
4. Mention environment details (DEV/PROD)
5. Include relevant reconciliation records

---

This documentation provides a comprehensive guide for using the Metrics Pipeline Framework. For additional support or feature requests, please refer to your team's internal documentation or contact the framework maintainers. 