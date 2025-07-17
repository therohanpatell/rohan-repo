# Partition Optimization Guide

## Overview

This document describes the optimizations implemented to speed up `{partition_info}` lookups in the metrics pipeline by reading only necessary partitions of metadata tables and source tables.

## Problem Statement

### Original Implementation Issues

1. **Individual Queries**: Each `{partition_info}` placeholder triggered a separate BigQuery query to the metadata table
2. **No Caching**: Repeated lookups for the same table resulted in duplicate queries
3. **No Partition Filtering**: Full table scans on metadata tables without partition optimization
4. **Inefficient Processing**: Multiple metrics referencing the same tables caused redundant work

### Performance Impact

- **High Query Count**: N metrics with partition_info placeholders = N individual queries
- **Increased Latency**: Each query has network overhead and BigQuery slot usage
- **Resource Waste**: Duplicate lookups for tables referenced by multiple metrics
- **Scalability Issues**: Performance degrades linearly with number of metrics

## Solution Architecture

### 1. Pre-Analysis Phase

```python
def _preload_partition_cache(self, json_data, dependencies, partition_info_table):
    # Extract all unique table references from all SQL queries
    # Batch query partition info for all tables in a single query
    # Cache results for subsequent lookups
```

**Benefits:**
- Single query instead of N queries
- Eliminates duplicate lookups
- Reduces BigQuery slot usage

### 2. Batch Querying

```python
def _batch_get_partition_dts(self, table_references, partition_info_table):
    # Build WHERE clause for all tables: (dataset='A' AND table='B') OR (dataset='C' AND table='D')
    # Use QUALIFY for efficient row selection
    # Return mapping of (dataset, table) -> partition_dt
```

**Query Example:**
```sql
SELECT project_dataset, table_name, partition_dt
FROM `metadata_table`
WHERE (project_dataset = 'dataset1' AND table_name = 'table1') 
   OR (project_dataset = 'dataset2' AND table_name = 'table2')
   OR (project_dataset = 'dataset3' AND table_name = 'table3')
QUALIFY ROW_NUMBER() OVER (PARTITION BY project_dataset, table_name ORDER BY partition_dt DESC) = 1
```

### 3. Intelligent Caching

```python
def get_partition_dt(self, project_dataset, table_name, partition_info_table):
    # Check cache first
    if cache_key in self._partition_cache:
        return self._partition_cache[cache_key]
    
    # Fallback to individual query if cache miss
    # Cache result for future use
```

**Cache Benefits:**
- O(1) lookup time for cached entries
- Eliminates repeated queries for same table
- Memory-efficient storage

### 4. Partition Filtering

```python
# Add partition filter to reduce data scanned
partition_filter = f"AND _PARTITIONDATE >= '{start_date}'"
```

**Optimization:**
- Reduces data scanned from metadata table
- Assumes metadata table is partitioned by date
- Configurable lookback period (default: 30 days)

## Implementation Details

### Cache Management

```python
class MetricsPipeline:
    def __init__(self, spark, bq_client):
        self._partition_cache = {}  # Cache for partition lookups
    
    def clear_partition_cache(self):
        # Free memory after processing
    
    def get_partition_cache_stats(self):
        # Monitor cache performance
```

### Performance Monitoring

```python
# Log cache statistics
cache_stats = pipeline.get_partition_cache_stats()
logger.info(f"Partition cache statistics: {cache_stats}")

# Track processing time
processing_time = (datetime.utcnow() - start_time).total_seconds()
logger.info(f"Processing completed in {processing_time:.2f} seconds")
```

### Error Handling

- **Graceful Degradation**: Falls back to individual queries if batch query fails
- **Cache Miss Handling**: Individual queries for uncached tables
- **Partition Filter Fallback**: Continues without partition filtering if not supported

## Performance Improvements

### Before Optimization

```
Processing 10 metrics with partition_info placeholders:
- 10 individual BigQuery queries
- 10 network round trips
- 10 BigQuery slot allocations
- No caching benefits
```

### After Optimization

```
Processing 10 metrics with partition_info placeholders:
- 1 batch BigQuery query (if all tables unique)
- 1 network round trip
- 1 BigQuery slot allocation
- Full caching benefits
- Partition filtering reduces data scanned
```

### Expected Performance Gains

| Metric Count | Tables Referenced | Before (Queries) | After (Queries) | Improvement |
|--------------|-------------------|------------------|-----------------|-------------|
| 10 | 3 unique | 10 | 1 | 90% reduction |
| 50 | 8 unique | 50 | 1 | 98% reduction |
| 100 | 15 unique | 100 | 1 | 99% reduction |

## Configuration Options

### Partition Filter Lookback

```python
# Adjust lookback period based on your metadata table
start_date = (current_date - timedelta(days=30)).strftime('%Y-%m-%d')
```

**Recommendations:**
- **Daily metrics**: 7-14 days lookback
- **Weekly metrics**: 30 days lookback  
- **Monthly metrics**: 90 days lookback

### Cache Management

```python
# Clear cache after processing to free memory
pipeline.clear_partition_cache()

# Monitor cache size
stats = pipeline.get_partition_cache_stats()
print(f"Cache size: {stats['cache_size']}")
```

## Best Practices

### 1. Metadata Table Design

```sql
-- Recommended metadata table structure
CREATE TABLE `project.dataset.partition_info` (
    project_dataset STRING,
    table_name STRING,
    partition_dt DATE,
    created_at TIMESTAMP
)
PARTITION BY DATE(created_at)  -- Enable partition filtering
CLUSTER BY project_dataset, table_name  -- Optimize for lookups
```

### 2. SQL Query Optimization

```sql
-- Use partition decorators when possible
SELECT * FROM `project.dataset.table$20240101`

-- Add partition filters in WHERE clauses
WHERE partition_dt = '{partition_info}'
```

### 3. Monitoring and Alerting

```python
# Monitor cache hit rates
cache_stats = pipeline.get_partition_cache_stats()
if cache_stats['cache_size'] == 0:
    logger.warning("Partition cache is empty - check metadata table")

# Monitor query performance
if processing_time > threshold:
    logger.warning(f"Processing time {processing_time}s exceeds threshold")
```

## Troubleshooting

### Common Issues

1. **Cache Misses**
   - Check metadata table accessibility
   - Verify table references in SQL queries
   - Review partition filter configuration

2. **Batch Query Failures**
   - Check BigQuery quotas and limits
   - Verify metadata table schema
   - Review WHERE clause construction

3. **Performance Degradation**
   - Monitor cache statistics
   - Check metadata table partition structure
   - Review lookback period configuration

### Debug Commands

```python
# Enable debug logging
logging.getLogger().setLevel(logging.DEBUG)

# Check cache contents
for (dataset, table), partition_dt in pipeline._partition_cache.items():
    print(f"{dataset}.{table} -> {partition_dt}")

# Monitor query execution
logger.debug(f"Batch query: {query}")
```

## Future Enhancements

### 1. Advanced Partition Filtering

```python
# Dynamic partition filter based on table usage patterns
def get_optimal_partition_filter(table_references):
    # Analyze historical partition usage
    # Determine optimal lookback period per table
    # Implement adaptive filtering
```

### 2. Query Optimization

```python
# Add partition hints to source table queries
def optimize_source_queries(sql, partition_dates):
    # Add partition decorators
    # Optimize WHERE clauses
    # Implement query hints
```

### 3. Cache Persistence

```python
# Persist cache across pipeline runs
def save_partition_cache():
    # Store cache in temporary table
    # Load cache on pipeline start
    # Implement cache invalidation
```

## Conclusion

The partition optimization implementation provides significant performance improvements by:

1. **Reducing Query Count**: From N queries to 1 batch query
2. **Eliminating Duplicates**: Caching prevents repeated lookups
3. **Optimizing Data Access**: Partition filtering reduces data scanned
4. **Improving Scalability**: Performance scales better with metric count

These optimizations are particularly beneficial for pipelines processing large numbers of metrics or frequently referenced tables. 