# Metrics Pipeline Framework - Developer Guide

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Key Features](#key-features)
5. [Configuration](#configuration)
6. [Usage Examples](#usage-examples)
7. [Error Handling](#error-handling)
8. [Performance Optimizations](#performance-optimizations)
9. [Testing](#testing)
10. [Troubleshooting](#troubleshooting)
11. [Best Practices](#best-practices)

## Overview

The Metrics Pipeline Framework is a robust, production-ready system for processing and calculating business metrics using PySpark and BigQuery. It's designed to handle complex data transformations, ensure data quality, and provide comprehensive reconciliation capabilities.

### Key Capabilities
- **Dynamic SQL Processing**: Supports placeholder replacement for date-based queries
- **Partition Optimization**: Intelligent caching and batching of partition lookups
- **Generic Column Configuration**: Flexible column mapping for different data schemas
- **Transaction Safety**: Rollback capabilities and overwrite protection
- **Comprehensive Reconciliation**: Detailed tracking of metric processing status
- **Error Recovery**: Graceful handling of failures with detailed error reporting

## Architecture

### High-Level Flow
```
1. Read JSON Configuration → 2. Validate Data → 3. Process Metrics → 4. Write to BigQuery → 5. Create Reconciliation Records
```

### Component Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   JSON Config   │───▶│  MetricsPipeline│───▶│   BigQuery      │
│   (GCS)         │    │   (Core Logic)  │    │   (Target)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │ Reconciliation  │
                       │   (Tracking)    │
                       └─────────────────┘
```

## Core Components

### 1. MetricsPipeline Class

The main orchestrator class that handles all pipeline operations.

#### Key Methods:

**Initialization & Configuration**
```python
def __init__(self, spark: SparkSession, bq_client: bigquery.Client, 
             column_config: Optional[Dict[str, str]] = None)
```
- Initializes the pipeline with Spark session and BigQuery client
- Sets up column configuration for flexible data mapping
- Creates execution tracking for rollback capabilities

**Column Configuration Management**
```python
def update_column_config(self, new_config: Dict[str, str]) -> None
def add_column_mapping(self, internal_name: str, sql_column_name: str) -> None
def get_sql_column_name(self, internal_name: str) -> str
def get_internal_name(self, sql_column_name: str) -> str
```

### 2. Data Processing Pipeline

#### JSON Reading & Validation
```python
def read_json_from_gcs(self, gcs_path: str) -> List[Dict]
def validate_json(self, json_data: List[Dict]) -> List[Dict]
```

**JSON Structure Requirements:**
```json
{
  "metric_id": "unique_identifier",
  "metric_name": "Human readable name",
  "metric_type": "calculation_type",
  "sql": "SELECT {currently} as business_data_date, ...",
  "dependency": "dependency_group",
  "target_table": "project.dataset.table"
}
```

#### SQL Processing
```python
def execute_sql(self, sql: str, run_date: str, partition_info_table: str, 
                metric_id: Optional[str] = None) -> Dict
```

**Supported Placeholders:**
- `{currently}`: Replaced with CLI run_date
- `{partition_info}`: Replaced with partition_dt from metadata table

### 3. Partition Optimization System

#### Batch Partition Lookups
```python
def _batch_get_partition_dts(self, table_references: List[Tuple[str, str]], 
                            partition_info_table: str) -> Dict[Tuple[str, str], str]
def _preload_partition_cache(self, json_data: List[Dict], dependencies: List[str], 
                            partition_info_table: str) -> None
```

**Performance Benefits:**
- Reduces BigQuery queries from N to 1 for partition lookups
- Implements intelligent caching to avoid repeated queries
- Uses partition filtering to reduce data scanned

#### Cache Management
```python
def clear_partition_cache(self) -> None
def get_partition_cache_stats(self) -> Dict[str, int]
```

### 4. BigQuery Integration

#### Schema Alignment
```python
def align_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame
def get_bq_table_schema(self, table_name: str) -> List[bigquery.SchemaField]
```

#### Write Operations
```python
def write_to_bq_with_overwrite(self, df: DataFrame, target_table: str) -> Tuple[List[str], List[Dict]]
def check_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]
def delete_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> None
```

### 5. Reconciliation System

#### Recon Record Creation
```python
def build_recon_record(self, metric_record: Dict, sql: str, run_date: str, 
                      env: str, execution_status: str, partition_dt: str, 
                      error_message: Optional[str] = None) -> Dict
```

**Recon Record Structure:**
```python
{
    'module_id': '103',
    'module_type_nm': 'Metrics',
    'source_server_nm': env,
    'target_server_nm': env,
    'source_vl': '0',
    'target_vl': '0' if success else '1',
    'rcncln_exact_pass_in': 'Passed' if success else 'Failed',
    'latest_source_parttn_dt': run_date,
    'latest_target_parttn_dt': run_date,
    'load_ts': current_timestamp,
    'schdld_dt': partition_dt,
    'source_system_id': metric_id,
    'schdld_yr': current_year,
    'Job_Name': metric_name
}
```

## Key Features

### 1. Generic Column Configuration

The framework supports flexible column mapping to handle different data schemas:

```python
# Default configuration
DEFAULT_COLUMN_CONFIG = {
    'metric_output': 'metric_output',
    'numerator_value': 'numerator_value', 
    'denominator_value': 'denominator_value',
    'business_data_date': 'business_data_date'
}

# Runtime configuration update
pipeline.update_column_config({
    'metric_output': 'output',
    'numerator_value': 'numerator',
    'denominator_value': 'denominator'
})
```

### 2. Precision Preservation

Numeric values are handled with precision preservation:
```python
def normalize_numeric_value(self, value: Union[int, float, Decimal, None]) -> Optional[str]
def safe_decimal_conversion(self, value: Optional[str]) -> Optional[Decimal]
```

### 3. Transaction Safety

Comprehensive rollback capabilities:
```python
def rollback_metric(self, metric_id: str, target_table: str, partition_dt: str) -> None
def rollback_all_processed_metrics(self, partition_dt: str) -> None
```

### 4. Error Handling

Graceful error handling with detailed reporting:
- Execution failures are tracked separately from write failures
- Error messages are preserved in reconciliation records
- Pipeline continues processing other metrics when individual metrics fail

## Configuration

### Environment Setup

**Required Dependencies:**
```python
# PySpark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DateType

# BigQuery
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
```

**Spark Configuration:**
```python
spark = SparkSession.builder \
    .appName("MetricsPipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

### Command Line Arguments

```bash
python pysaprk_copy.py \
    --gcs_path gs://bucket/path/to/config.json \
    --run_date 2024-01-15 \
    --dependencies "dep1,dep2,dep3" \
    --partition_info_table project.dataset.partition_info \
    --env PRD \
    --recon_table project.dataset.reconciliation
```

## Usage Examples

### Basic Usage

```python
from pysaprk_copy import MetricsPipeline, managed_spark_session
from google.cloud import bigquery

# Initialize components
with managed_spark_session("MyMetricsPipeline") as spark:
    bq_client = bigquery.Client()
    
    # Create pipeline with custom column config
    column_config = {
        'metric_output': 'output',
        'numerator_value': 'numerator',
        'denominator_value': 'denominator',
        'business_data_date': 'business_date'
    }
    
    pipeline = MetricsPipeline(spark, bq_client, column_config)
    
    # Process metrics
    json_data = pipeline.read_json_from_gcs("gs://bucket/config.json")
    validated_data = pipeline.validate_json(json_data)
    
    metrics_dfs, successful_metrics, failed_metrics = pipeline.process_metrics(
        validated_data, 
        "2024-01-15", 
        ["dependency1"], 
        "project.dataset.partition_info"
    )
```

### Custom Column Configuration

```python
# Add new column mappings
pipeline.add_column_mapping('custom_metric', 'custom_value')

# Update existing mappings
pipeline.update_column_config({
    'metric_output': 'calculated_value',
    'business_data_date': 'data_date'
})

# Get column names
sql_column = pipeline.get_sql_column_name('metric_output')  # Returns 'calculated_value'
internal_name = pipeline.get_internal_name('calculated_value')  # Returns 'metric_output'
```

### Error Handling Example

```python
try:
    # Process metrics
    metrics_dfs, successful_metrics, failed_metrics = pipeline.process_metrics(...)
    
    # Handle failed metrics
    if failed_metrics:
        logger.warning(f"Failed metrics: {[fm['metric_record']['metric_id'] for fm in failed_metrics]}")
        for failed_metric in failed_metrics:
            logger.error(f"Metric {failed_metric['metric_record']['metric_id']}: {failed_metric['error_message']}")
    
except MetricsPipelineError as e:
    logger.error(f"Pipeline failed: {str(e)}")
    # Rollback will be attempted automatically
```

## Error Handling

### Exception Types

**MetricsPipelineError**: Custom exception for pipeline-specific errors
- Validation failures
- SQL execution errors
- BigQuery write failures
- Configuration errors

### Error Recovery

1. **Automatic Rollback**: Failed pipelines automatically attempt to rollback processed metrics
2. **Graceful Degradation**: Individual metric failures don't stop the entire pipeline
3. **Detailed Logging**: Comprehensive error messages with context
4. **Reconciliation Tracking**: All failures are recorded in reconciliation tables

### Common Error Scenarios

**SQL Execution Errors:**
```python
# Invalid SQL syntax
# Missing table references
# Permission denied
# Query timeout
```

**Data Validation Errors:**
```python
# Zero denominators
# Missing required columns
# Invalid date formats
# Duplicate metric IDs
```

**BigQuery Errors:**
```python
# Schema mismatches
# Table not found
# Permission issues
# Quota exceeded
```

## Performance Optimizations

### 1. Partition Cache Optimization

**Before Optimization:**
- N individual BigQuery queries for N tables
- No caching of partition information
- Repeated queries for same tables

**After Optimization:**
- Single batch query for all tables
- Intelligent caching with partition filtering
- Cache statistics tracking

```python
# Cache statistics
cache_stats = pipeline.get_partition_cache_stats()
print(f"Cache size: {cache_stats['cache_size']}")
print(f"Cached tables: {cache_stats['cached_tables']}")
```

### 2. Batch Processing

**Partition Lookups:**
```python
# Batch query for multiple tables
partition_map = pipeline._batch_get_partition_dts(table_references, partition_info_table)
```

**BigQuery Writes:**
```python
# Batch write with overwrite capability
written_metric_ids, failed_metrics = pipeline.write_to_bq_with_overwrite(df, target_table)
```

### 3. Schema Optimization

**Dynamic Schema Creation:**
```python
# Schema created based on column configuration
schema_fields = [
    StructField("metric_id", StringType(), False),
    StructField("metric_name", StringType(), False),
    # ... dynamic columns based on configuration
]
```

## Testing

### Unit Testing

**Test Column Configuration:**
```python
def test_column_configuration():
    pipeline = MetricsPipeline(spark, bq_client)
    
    # Test default configuration
    assert pipeline.get_sql_column_name('metric_output') == 'metric_output'
    
    # Test custom configuration
    pipeline.update_column_config({'metric_output': 'output'})
    assert pipeline.get_sql_column_name('metric_output') == 'output'
```

**Test Partition Cache:**
```python
def test_partition_cache():
    pipeline = MetricsPipeline(spark, bq_client)
    
    # Test cache operations
    pipeline._partition_cache[('dataset', 'table')] = '2024-01-15'
    assert pipeline.get_partition_dt('dataset', 'table', 'partition_table') == '2024-01-15'
    
    # Test cache statistics
    stats = pipeline.get_partition_cache_stats()
    assert stats['cache_size'] == 1
```

### Integration Testing

**End-to-End Pipeline Test:**
```python
def test_full_pipeline():
    # Setup test data
    test_json = [
        {
            "metric_id": "test_metric",
            "metric_name": "Test Metric",
            "metric_type": "ratio",
            "sql": "SELECT 100 as metric_output, 10 as denominator_value, '2024-01-15' as business_data_date",
            "dependency": "test_dep",
            "target_table": "test_project.test_dataset.test_table"
        }
    ]
    
    # Execute pipeline
    pipeline = MetricsPipeline(spark, bq_client)
    metrics_dfs, successful_metrics, failed_metrics = pipeline.process_metrics(
        test_json, "2024-01-15", ["test_dep"], "partition_table"
    )
    
    # Verify results
    assert len(successful_metrics) == 1
    assert len(failed_metrics) == 0
```

## Troubleshooting

### Common Issues

**1. Import Errors**
```python
# Solution: Install required packages
pip install pyspark google-cloud-bigquery
```

**2. BigQuery Permission Errors**
```python
# Solution: Ensure proper authentication
# Set GOOGLE_APPLICATION_CREDENTIALS environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

**3. Schema Mismatch Errors**
```python
# Solution: Check column configuration
pipeline.column_config  # Verify column mappings
pipeline.get_bq_table_schema(target_table)  # Check BigQuery schema
```

**4. Partition Cache Issues**
```python
# Solution: Clear cache and retry
pipeline.clear_partition_cache()
# Or check cache statistics
stats = pipeline.get_partition_cache_stats()
```

### Debug Mode

Enable detailed logging for troubleshooting:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Performance Monitoring

Monitor pipeline performance:
```python
# Track processing time
start_time = datetime.utcnow()
# ... pipeline execution ...
processing_time = (datetime.utcnow() - start_time).total_seconds()
logger.info(f"Processing completed in {processing_time:.2f} seconds")

# Monitor cache performance
cache_stats = pipeline.get_partition_cache_stats()
logger.info(f"Cache hit rate: {cache_stats}")
```

## Best Practices

### 1. Column Configuration

**Use Descriptive Names:**
```python
# Good
column_config = {
    'metric_output': 'calculated_metric_value',
    'business_data_date': 'data_processing_date'
}

# Avoid
column_config = {
    'metric_output': 'val',
    'business_data_date': 'dt'
}
```

**Validate Configuration:**
```python
# Always validate column configuration before processing
required_columns = ['business_data_date']
for col in required_columns:
    if col not in pipeline.column_config:
        raise ValueError(f"Missing required column: {col}")
```

### 2. Error Handling

**Implement Proper Rollback:**
```python
try:
    # Process metrics
    pipeline.process_metrics(...)
except Exception as e:
    # Rollback is automatic, but you can add custom logic
    logger.error(f"Pipeline failed: {e}")
    # Additional cleanup if needed
```

**Log Detailed Errors:**
```python
# Always log metric-specific errors
for failed_metric in failed_metrics:
    logger.error(f"Metric {failed_metric['metric_record']['metric_id']}: {failed_metric['error_message']}")
```

### 3. Performance Optimization

**Use Batch Operations:**
```python
# Preload partition cache for better performance
pipeline._preload_partition_cache(json_data, dependencies, partition_info_table)
```

**Monitor Resource Usage:**
```python
# Track memory usage
import psutil
memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  # MB
logger.info(f"Memory usage: {memory_usage:.2f} MB")
```

### 4. Data Quality

**Validate Input Data:**
```python
# Always validate JSON before processing
validated_data = pipeline.validate_json(json_data)
```

**Check for Edge Cases:**
```python
# Handle zero denominators
if denominator_value == 0:
    raise MetricsPipelineError("Cannot calculate metric with zero denominator")

# Handle very small denominators
if abs(denominator_value) < Decimal('0.0000001'):
    logger.warning("Very small denominator detected - may cause precision issues")
```

### 5. Monitoring and Alerting

**Set Up Monitoring:**
```python
# Track success/failure rates
success_rate = len(successful_metrics) / (len(successful_metrics) + len(failed_metrics))
if success_rate < 0.95:  # 95% success threshold
    logger.warning(f"Low success rate: {success_rate:.2%}")
```

**Implement Health Checks:**
```python
# Check pipeline health
def health_check():
    try:
        # Test basic operations
        pipeline.get_partition_cache_stats()
        return True
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False
```

## Conclusion

The Metrics Pipeline Framework provides a robust, scalable solution for processing business metrics. By following this guide, new developers can quickly understand the architecture, implement custom solutions, and maintain high-quality data processing pipelines.

For additional support:
- Review the test files for implementation examples
- Check the edge case testing documentation
- Refer to the reconciliation implementation guide
- Use the quick reference guides for common operations

Remember: Always test your changes thoroughly and monitor pipeline performance in production environments. 