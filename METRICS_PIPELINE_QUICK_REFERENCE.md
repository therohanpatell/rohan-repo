# Metrics Pipeline Framework - Quick Reference

## 🚀 Quick Start

### Basic Usage
```python
from pysaprk_copy import MetricsPipeline, managed_spark_session
from google.cloud import bigquery

with managed_spark_session("MyPipeline") as spark:
    bq_client = bigquery.Client()
    pipeline = MetricsPipeline(spark, bq_client)
    
    # Process metrics
    json_data = pipeline.read_json_from_gcs("gs://bucket/config.json")
    metrics_dfs, successful, failed = pipeline.process_metrics(
        json_data, "2024-01-15", ["dependency1"], "project.dataset.partition_info"
    )
```

### Command Line Execution
```bash
python pysaprk_copy.py \
    --gcs_path gs://bucket/config.json \
    --run_date 2024-01-15 \
    --dependencies "dep1,dep2" \
    --partition_info_table project.dataset.partition_info \
    --env PRD \
    --recon_table project.dataset.reconciliation
```

## 📋 JSON Configuration Format

### Required Fields
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

### SQL Placeholders
- `{currently}` → CLI run_date
- `{partition_info}` → partition_dt from metadata table

## 🔧 Column Configuration

### Default Configuration
```python
DEFAULT_COLUMN_CONFIG = {
    'metric_output': 'metric_output',
    'numerator_value': 'numerator_value',
    'denominator_value': 'denominator_value', 
    'business_data_date': 'business_data_date'
}
```

### Custom Configuration
```python
# Update at runtime
pipeline.update_column_config({
    'metric_output': 'output',
    'numerator_value': 'numerator'
})

# Add new mappings
pipeline.add_column_mapping('custom_metric', 'custom_value')

# Get column names
sql_col = pipeline.get_sql_column_name('metric_output')
internal_name = pipeline.get_internal_name('output')
```

## 🏗️ Core Classes & Methods

### MetricsPipeline Class
```python
class MetricsPipeline:
    def __init__(self, spark, bq_client, column_config=None)
    def process_metrics(self, json_data, run_date, dependencies, partition_info_table)
    def execute_sql(self, sql, run_date, partition_info_table, metric_id=None)
    def write_to_bq_with_overwrite(self, df, target_table)
    def build_recon_record(self, metric_record, sql, run_date, env, status, partition_dt)
```

### Key Methods Quick Reference
| Method | Purpose | Returns |
|--------|---------|---------|
| `read_json_from_gcs()` | Read JSON config from GCS | `List[Dict]` |
| `validate_json()` | Validate JSON structure | `List[Dict]` |
| `process_metrics()` | Process all metrics | `(DataFrames, successful, failed)` |
| `execute_sql()` | Execute single SQL query | `Dict` |
| `write_to_bq_with_overwrite()` | Write to BigQuery | `(successful_ids, failed_metrics)` |

## 🔄 Data Flow

```
JSON Config → Validation → SQL Processing → BigQuery Write → Reconciliation
     ↓              ↓            ↓              ↓              ↓
  GCS Read    Field Check   Placeholder    Schema Align   Status Track
              Duplicate     Replacement    Overwrite      Error Log
              Check         Partition      Protection     Success Log
                           Lookup
```

## ⚡ Performance Optimizations

### Partition Cache
```python
# Preload cache for better performance
pipeline._preload_partition_cache(json_data, dependencies, partition_info_table)

# Check cache stats
stats = pipeline.get_partition_cache_stats()
print(f"Cache size: {stats['cache_size']}")

# Clear cache when done
pipeline.clear_partition_cache()
```

### Batch Operations
- **Partition Lookups**: Single query for all tables instead of N queries
- **BigQuery Writes**: Batch write with overwrite capability
- **Schema Alignment**: Automatic column reordering and type conversion

## 🛡️ Error Handling

### Exception Types
```python
class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass
```

### Error Recovery
```python
try:
    pipeline.process_metrics(...)
except MetricsPipelineError as e:
    # Automatic rollback attempted
    logger.error(f"Pipeline failed: {e}")
    # Check failed_metrics for details
```

### Common Error Scenarios
| Error Type | Cause | Solution |
|------------|-------|----------|
| `ZeroDivisionError` | Zero denominator | Check data quality |
| `SchemaMismatch` | Column mismatch | Update column config |
| `PermissionDenied` | BigQuery access | Check credentials |
| `TableNotFound` | Missing table | Verify table exists |

## 📊 Reconciliation Records

### Recon Record Structure
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

## 🧪 Testing

### Unit Test Example
```python
def test_pipeline():
    pipeline = MetricsPipeline(spark, bq_client)
    
    # Test column config
    pipeline.update_column_config({'metric_output': 'output'})
    assert pipeline.get_sql_column_name('metric_output') == 'output'
    
    # Test partition cache
    pipeline._partition_cache[('dataset', 'table')] = '2024-01-15'
    assert pipeline.get_partition_dt('dataset', 'table', 'partition_table') == '2024-01-15'
```

### Integration Test Example
```python
def test_full_pipeline():
    test_json = [{
        "metric_id": "test_metric",
        "metric_name": "Test Metric",
        "metric_type": "ratio",
        "sql": "SELECT 100 as metric_output, 10 as denominator_value, '2024-01-15' as business_data_date",
        "dependency": "test_dep",
        "target_table": "test_project.test_dataset.test_table"
    }]
    
    pipeline = MetricsPipeline(spark, bq_client)
    metrics_dfs, successful, failed = pipeline.process_metrics(
        test_json, "2024-01-15", ["test_dep"], "partition_table"
    )
    
    assert len(successful) == 1
    assert len(failed) == 0
```

## 🔍 Troubleshooting

### Common Issues & Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Import Errors | `ModuleNotFoundError` | `pip install pyspark google-cloud-bigquery` |
| Permission Errors | `403 Forbidden` | Set `GOOGLE_APPLICATION_CREDENTIALS` |
| Schema Mismatch | `Column not found` | Check `pipeline.column_config` |
| Cache Issues | Slow performance | Clear cache: `pipeline.clear_partition_cache()` |

### Debug Mode
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Performance Monitoring
```python
import time
start_time = time.time()
# ... pipeline execution ...
processing_time = time.time() - start_time
logger.info(f"Processing time: {processing_time:.2f}s")

# Monitor cache
stats = pipeline.get_partition_cache_stats()
logger.info(f"Cache stats: {stats}")
```

## 📈 Best Practices

### 1. Column Configuration
```python
# ✅ Good: Descriptive names
column_config = {
    'metric_output': 'calculated_metric_value',
    'business_data_date': 'data_processing_date'
}

# ❌ Avoid: Short names
column_config = {
    'metric_output': 'val',
    'business_data_date': 'dt'
}
```

### 2. Error Handling
```python
# ✅ Good: Log detailed errors
for failed_metric in failed_metrics:
    logger.error(f"Metric {failed_metric['metric_record']['metric_id']}: {failed_metric['error_message']}")

# ✅ Good: Check success rate
success_rate = len(successful_metrics) / (len(successful_metrics) + len(failed_metrics))
if success_rate < 0.95:
    logger.warning(f"Low success rate: {success_rate:.2%}")
```

### 3. Performance
```python
# ✅ Good: Preload partition cache
pipeline._preload_partition_cache(json_data, dependencies, partition_info_table)

# ✅ Good: Monitor memory usage
import psutil
memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
logger.info(f"Memory usage: {memory_mb:.2f} MB")
```

### 4. Data Quality
```python
# ✅ Good: Validate input
validated_data = pipeline.validate_json(json_data)

# ✅ Good: Handle edge cases
if denominator_value == 0:
    raise MetricsPipelineError("Cannot calculate with zero denominator")
```

## 🔗 Related Documentation

- **Detailed Guide**: `METRICS_PIPELINE_DEVELOPER_GUIDE.md`
- **Generic Columns**: `GENERIC_COLUMN_CONFIGURATION_GUIDE.md`
- **Partition Optimization**: `PARTITION_OPTIMIZATION_SUMMARY.md`
- **Edge Case Testing**: `EDGE_CASE_TESTING_SUMMARY.md`
- **Reconciliation**: `RECON_IMPLEMENTATION_SUMMARY.md`

## 📞 Support

For additional help:
1. Check the detailed developer guide
2. Review test files for examples
3. Check edge case testing documentation
4. Monitor pipeline logs for error details
5. Use debug mode for troubleshooting

---

**Remember**: Always test changes thoroughly and monitor pipeline performance in production! 