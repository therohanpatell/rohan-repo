# Metrics Pipeline Framework - API Reference

## Table of Contents
1. [MetricsPipeline Class](#metricspipeline-class)
2. [Context Managers](#context-managers)
3. [Exceptions](#exceptions)
4. [Utility Functions](#utility-functions)
5. [Command Line Interface](#command-line-interface)

## MetricsPipeline Class

The main orchestrator class for processing metrics using PySpark and BigQuery.

### Constructor

```python
def __init__(self, spark: SparkSession, bq_client: bigquery.Client, 
             column_config: Optional[Dict[str, str]] = None)
```

**Parameters:**
- `spark` (SparkSession): PySpark session for data processing
- `bq_client` (bigquery.Client): BigQuery client for database operations
- `column_config` (Optional[Dict[str, str]]): Custom column configuration mapping

**Default Column Configuration:**
```python
DEFAULT_COLUMN_CONFIG = {
    'metric_output': 'metric_output',
    'numerator_value': 'numerator_value', 
    'denominator_value': 'denominator_value',
    'business_data_date': 'business_data_date'
}
```

### Column Configuration Methods

#### update_column_config
```python
def update_column_config(self, new_config: Dict[str, str]) -> None
```
Updates the column configuration at runtime.

**Parameters:**
- `new_config` (Dict[str, str]): New column configuration mapping

**Example:**
```python
pipeline.update_column_config({
    'metric_output': 'output',
    'numerator_value': 'numerator'
})
```

#### add_column_mapping
```python
def add_column_mapping(self, internal_name: str, sql_column_name: str) -> None
```
Adds a new column mapping.

**Parameters:**
- `internal_name` (str): Internal name used in the pipeline
- `sql_column_name` (str): Column name expected in SQL results

**Example:**
```python
pipeline.add_column_mapping('custom_metric', 'custom_value')
```

#### get_sql_column_name
```python
def get_sql_column_name(self, internal_name: str) -> str
```
Gets SQL column name from internal name.

**Parameters:**
- `internal_name` (str): Internal column name

**Returns:**
- `str`: SQL column name

**Example:**
```python
sql_col = pipeline.get_sql_column_name('metric_output')  # Returns 'metric_output' or mapped value
```

#### get_internal_name
```python
def get_internal_name(self, sql_column_name: str) -> str
```
Gets internal name from SQL column name.

**Parameters:**
- `sql_column_name` (str): SQL column name

**Returns:**
- `str`: Internal column name

**Example:**
```python
internal_name = pipeline.get_internal_name('output')  # Returns 'metric_output' if mapped
```

### Data Reading & Validation Methods

#### read_json_from_gcs
```python
def read_json_from_gcs(self, gcs_path: str) -> List[Dict]
```
Reads JSON file from GCS and returns as list of dictionaries.

**Parameters:**
- `gcs_path` (str): GCS path to JSON file

**Returns:**
- `List[Dict]`: List of metric definitions

**Raises:**
- `MetricsPipelineError`: If file cannot be read or parsed

**Example:**
```python
json_data = pipeline.read_json_from_gcs("gs://bucket/config.json")
```

#### validate_json
```python
def validate_json(self, json_data: List[Dict]) -> List[Dict]
```
Validates JSON data for required fields and duplicates.

**Parameters:**
- `json_data` (List[Dict]): List of metric definitions

**Returns:**
- `List[Dict]`: List of validated metric definitions

**Raises:**
- `MetricsPipelineError`: If validation fails

**Required Fields:**
- `metric_id`: Unique identifier
- `metric_name`: Human readable name
- `metric_type`: Calculation type
- `sql`: SQL query with placeholders
- `dependency`: Dependency group
- `target_table`: Target BigQuery table (project.dataset.table format)

**Example:**
```python
validated_data = pipeline.validate_json(json_data)
```

### SQL Processing Methods

#### execute_sql
```python
def execute_sql(self, sql: str, run_date: str, partition_info_table: str, 
                metric_id: Optional[str] = None) -> Dict
```
Executes SQL query with dynamic placeholder replacement.

**Parameters:**
- `sql` (str): SQL query string with placeholders
- `run_date` (str): CLI provided run date
- `partition_info_table` (str): Metadata table name
- `metric_id` (Optional[str]): Optional metric ID for better error reporting

**Returns:**
- `Dict`: Dictionary with query results

**Raises:**
- `MetricsPipelineError`: If SQL execution fails

**Supported Placeholders:**
- `{currently}`: Replaced with run_date
- `{partition_info}`: Replaced with partition_dt from metadata table

**Example:**
```python
results = pipeline.execute_sql(
    "SELECT {currently} as business_data_date, 100 as metric_output",
    "2024-01-15",
    "project.dataset.partition_info",
    "metric_001"
)
```

#### replace_sql_placeholders
```python
def replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str) -> str
```
Replaces placeholders in SQL with appropriate dates.

**Parameters:**
- `sql` (str): SQL query string with placeholders
- `run_date` (str): CLI provided run date
- `partition_info_table` (str): Metadata table name

**Returns:**
- `str`: SQL with all placeholders replaced

**Example:**
```python
final_sql = pipeline.replace_sql_placeholders(
    "SELECT {currently} as date, {partition_info} as partition_date",
    "2024-01-15",
    "project.dataset.partition_info"
)
```

### Partition Optimization Methods

#### _batch_get_partition_dts
```python
def _batch_get_partition_dts(self, table_references: List[Tuple[str, str]], 
                            partition_info_table: str) -> Dict[Tuple[str, str], str]
```
Batch queries partition_dt for multiple tables in a single BigQuery query.

**Parameters:**
- `table_references` (List[Tuple[str, str]]): List of (dataset, table_name) tuples
- `partition_info_table` (str): Metadata table name

**Returns:**
- `Dict[Tuple[str, str], str]`: Dictionary mapping (dataset, table_name) to partition_dt

**Example:**
```python
table_refs = [("dataset1", "table1"), ("dataset2", "table2")]
partition_map = pipeline._batch_get_partition_dts(table_refs, "project.dataset.partition_info")
```

#### _preload_partition_cache
```python
def _preload_partition_cache(self, json_data: List[Dict], dependencies: List[str], 
                            partition_info_table: str) -> None
```
Preloads partition cache by analyzing all SQL queries and batching partition lookups.

**Parameters:**
- `json_data` (List[Dict]): List of metric definitions
- `dependencies` (List[str]): List of dependencies to process
- `partition_info_table` (str): Metadata table name

**Example:**
```python
pipeline._preload_partition_cache(json_data, ["dep1", "dep2"], "project.dataset.partition_info")
```

#### get_partition_dt
```python
def get_partition_dt(self, project_dataset: str, table_name: str, partition_info_table: str) -> Optional[str]
```
Gets latest partition_dt from metadata table (optimized with caching).

**Parameters:**
- `project_dataset` (str): Dataset name
- `table_name` (str): Table name
- `partition_info_table` (str): Metadata table name

**Returns:**
- `Optional[str]`: Latest partition date as string or None

**Example:**
```python
partition_dt = pipeline.get_partition_dt("dataset1", "table1", "project.dataset.partition_info")
```

#### clear_partition_cache
```python
def clear_partition_cache(self) -> None
```
Clears the partition cache to free memory.

**Example:**
```python
pipeline.clear_partition_cache()
```

#### get_partition_cache_stats
```python
def get_partition_cache_stats(self) -> Dict[str, int]
```
Gets statistics about the partition cache.

**Returns:**
- `Dict[str, int]`: Dictionary with cache statistics

**Example:**
```python
stats = pipeline.get_partition_cache_stats()
print(f"Cache size: {stats['cache_size']}")
print(f"Cached tables: {stats['cached_tables']}")
```

### BigQuery Integration Methods

#### get_bq_table_schema
```python
def get_bq_table_schema(self, table_name: str) -> List[bigquery.SchemaField]
```
Gets BigQuery table schema.

**Parameters:**
- `table_name` (str): Full table name (project.dataset.table)

**Returns:**
- `List[bigquery.SchemaField]`: List of schema fields

**Raises:**
- `MetricsPipelineError`: If table not found or schema retrieval fails

**Example:**
```python
schema = pipeline.get_bq_table_schema("project.dataset.table")
```

#### align_schema_with_bq
```python
def align_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame
```
Aligns Spark DataFrame with BigQuery table schema.

**Parameters:**
- `df` (DataFrame): Spark DataFrame
- `target_table` (str): BigQuery table name

**Returns:**
- `DataFrame`: Schema-aligned DataFrame

**Example:**
```python
aligned_df = pipeline.align_schema_with_bq(df, "project.dataset.table")
```

#### write_to_bq_with_overwrite
```python
def write_to_bq_with_overwrite(self, df: DataFrame, target_table: str) -> Tuple[List[str], List[Dict]]
```
Writes DataFrame to BigQuery table with overwrite capability for existing metrics.

**Parameters:**
- `df` (DataFrame): Spark DataFrame to write
- `target_table` (str): Target BigQuery table

**Returns:**
- `Tuple[List[str], List[Dict]]`: (successful_metric_ids list, failed_metrics list)

**Example:**
```python
successful_ids, failed_metrics = pipeline.write_to_bq_with_overwrite(df, "project.dataset.table")
```

#### check_existing_metrics
```python
def check_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]
```
Checks which metric IDs already exist in BigQuery table for the given partition date.

**Parameters:**
- `metric_ids` (List[str]): List of metric IDs to check
- `partition_dt` (str): Partition date to check
- `target_table` (str): Target BigQuery table

**Returns:**
- `List[str]`: List of existing metric IDs

**Example:**
```python
existing_metrics = pipeline.check_existing_metrics(
    ["metric1", "metric2"], 
    "2024-01-15", 
    "project.dataset.table"
)
```

#### delete_existing_metrics
```python
def delete_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> None
```
Deletes existing metrics from BigQuery table for the given partition date.

**Parameters:**
- `metric_ids` (List[str]): List of metric IDs to delete
- `partition_dt` (str): Partition date for deletion
- `target_table` (str): Target BigQuery table

**Example:**
```python
pipeline.delete_existing_metrics(
    ["metric1", "metric2"], 
    "2024-01-15", 
    "project.dataset.table"
)
```

### Reconciliation Methods

#### build_recon_record
```python
def build_recon_record(self, metric_record: Dict, sql: str, run_date: str, 
                      env: str, execution_status: str, partition_dt: str, 
                      error_message: Optional[str] = None) -> Dict
```
Builds a reconciliation record for a metric.

**Parameters:**
- `metric_record` (Dict): Original metric record from JSON
- `sql` (str): SQL query string
- `run_date` (str): Run date from CLI
- `env` (str): Environment from CLI
- `execution_status` (str): 'success' or 'failed'
- `partition_dt` (str): Partition date used in SQL
- `error_message` (Optional[str]): Optional error message if metric failed

**Returns:**
- `Dict`: Dictionary containing recon record

**Example:**
```python
recon_record = pipeline.build_recon_record(
    metric_record,
    "SELECT ...",
    "2024-01-15",
    "PRD",
    "success",
    "2024-01-15"
)
```

#### write_recon_to_bq
```python
def write_recon_to_bq(self, recon_records: List[Dict], recon_table: str) -> None
```
Writes reconciliation records to BigQuery recon table.

**Parameters:**
- `recon_records` (List[Dict]): List of recon records to write
- `recon_table` (str): Target recon table name

**Raises:**
- `MetricsPipelineError`: If write operation fails

**Example:**
```python
pipeline.write_recon_to_bq(recon_records, "project.dataset.reconciliation")
```

#### create_recon_records_from_write_results
```python
def create_recon_records_from_write_results(self, json_data: List[Dict], run_date: str, 
                                          dependencies: List[str], partition_info_table: str,
                                          env: str, successful_writes: Dict[str, List[str]],
                                          failed_execution_metrics: List[Dict], 
                                          failed_write_metrics: Dict[str, List[Dict]],
                                          partition_dt: str) -> List[Dict]
```
Creates recon records based on execution results and write success/failure to target tables.

**Parameters:**
- `json_data` (List[Dict]): List of metric definitions
- `run_date` (str): CLI provided run date
- `dependencies` (List[str]): List of dependencies processed
- `partition_info_table` (str): Metadata table name
- `env` (str): Environment name
- `successful_writes` (Dict[str, List[str]]): Dict mapping target_table to list of successfully written metric IDs
- `failed_execution_metrics` (List[Dict]): List of dicts with 'metric_record' and 'error_message' keys
- `failed_write_metrics` (Dict[str, List[Dict]]): Dict mapping target_table to list of failed write metrics with error messages
- `partition_dt` (str): Partition date used

**Returns:**
- `List[Dict]`: List of recon records

**Example:**
```python
recon_records = pipeline.create_recon_records_from_write_results(
    json_data,
    "2024-01-15",
    ["dep1"],
    "project.dataset.partition_info",
    "PRD",
    successful_writes,
    failed_execution_metrics,
    failed_write_metrics,
    "2024-01-15"
)
```

### Main Processing Method

#### process_metrics
```python
def process_metrics(self, json_data: List[Dict], run_date: str, 
                   dependencies: List[str], partition_info_table: str) -> Tuple[Dict[str, DataFrame], List[Dict], List[Dict]]
```
Processes metrics and creates Spark DataFrames grouped by target_table.

**Parameters:**
- `json_data` (List[Dict]): List of metric definitions
- `run_date` (str): CLI provided run date
- `dependencies` (List[str]): List of dependencies to process
- `partition_info_table` (str): Metadata table name

**Returns:**
- `Tuple[Dict[str, DataFrame], List[Dict], List[Dict]]`: (DataFrames dict, successful_metrics list, failed_metrics list)

**Example:**
```python
metrics_dfs, successful_metrics, failed_metrics = pipeline.process_metrics(
    json_data,
    "2024-01-15",
    ["dep1", "dep2"],
    "project.dataset.partition_info"
)
```

### Rollback Methods

#### rollback_metric
```python
def rollback_metric(self, metric_id: str, target_table: str, partition_dt: str) -> None
```
Rolls back a specific metric from the target table.

**Parameters:**
- `metric_id` (str): Metric ID to rollback
- `target_table` (str): Target BigQuery table
- `partition_dt` (str): Partition date for the metric

**Example:**
```python
pipeline.rollback_metric("metric_001", "project.dataset.table", "2024-01-15")
```

#### rollback_all_processed_metrics
```python
def rollback_all_processed_metrics(self, partition_dt: str) -> None
```
Rolls back all processed metrics from all target tables in case of failure.

**Parameters:**
- `partition_dt` (str): Partition date for rollback

**Example:**
```python
pipeline.rollback_all_processed_metrics("2024-01-15")
```

### Utility Methods

#### validate_gcs_path
```python
def validate_gcs_path(self, gcs_path: str) -> str
```
Validates GCS path format and accessibility.

**Parameters:**
- `gcs_path` (str): GCS path to validate

**Returns:**
- `str`: Validated GCS path

**Raises:**
- `MetricsPipelineError`: If path is invalid or inaccessible

**Example:**
```python
valid_path = pipeline.validate_gcs_path("gs://bucket/path/file.json")
```

#### check_dependencies_exist
```python
def check_dependencies_exist(self, json_data: List[Dict], dependencies: List[str]) -> None
```
Checks if all specified dependencies exist in the JSON data.

**Parameters:**
- `json_data` (List[Dict]): List of metric definitions
- `dependencies` (List[str]): List of dependencies to check

**Raises:**
- `MetricsPipelineError`: If any dependency is missing

**Example:**
```python
pipeline.check_dependencies_exist(json_data, ["dep1", "dep2"])
```

#### normalize_numeric_value
```python
def normalize_numeric_value(self, value: Union[int, float, Decimal, None]) -> Optional[str]
```
Normalizes numeric values to string representation to preserve precision.

**Parameters:**
- `value` (Union[int, float, Decimal, None]): Numeric value of any type

**Returns:**
- `Optional[str]`: String representation of the number or None

**Example:**
```python
normalized = pipeline.normalize_numeric_value(123.456)
```

#### safe_decimal_conversion
```python
def safe_decimal_conversion(self, value: Optional[str]) -> Optional[Decimal]
```
Safely converts string to Decimal for BigQuery.

**Parameters:**
- `value` (Optional[str]): String representation of number

**Returns:**
- `Optional[Decimal]`: Decimal value or None

**Example:**
```python
decimal_val = pipeline.safe_decimal_conversion("123.456")
```

## Context Managers

### managed_spark_session
```python
@contextmanager
def managed_spark_session(app_name: str = "MetricsPipeline")
```
Context manager for Spark session with proper cleanup.

**Parameters:**
- `app_name` (str): Spark application name

**Yields:**
- `SparkSession`: SparkSession instance

**Example:**
```python
with managed_spark_session("MyPipeline") as spark:
    # Use spark session
    pass
# Session automatically cleaned up
```

## Exceptions

### MetricsPipelineError
```python
class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass
```
Custom exception for pipeline-specific errors.

**Usage:**
```python
try:
    pipeline.process_metrics(...)
except MetricsPipelineError as e:
    logger.error(f"Pipeline failed: {e}")
```

## Utility Functions

### parse_arguments
```python
def parse_arguments() -> argparse.Namespace
```
Parses command line arguments.

**Returns:**
- `argparse.Namespace`: Parsed command line arguments

**Required Arguments:**
- `--gcs_path`: GCS path to JSON input file
- `--run_date`: Run date in YYYY-MM-DD format
- `--dependencies`: Comma-separated list of dependencies to process
- `--partition_info_table`: BigQuery table for partition info (project.dataset.table)
- `--env`: Environment name (e.g., BLD, PRD, DEV)
- `--recon_table`: BigQuery table for reconciliation data (project.dataset.table)

**Example:**
```python
args = parse_arguments()
print(f"GCS Path: {args.gcs_path}")
print(f"Run Date: {args.run_date}")
```

### validate_date_format
```python
def validate_date_format(date_str: str) -> None
```
Validates date format.

**Parameters:**
- `date_str` (str): Date string to validate

**Raises:**
- `MetricsPipelineError`: If date format is invalid

**Example:**
```python
validate_date_format("2024-01-15")  # Valid
validate_date_format("2024/01/15")  # Raises MetricsPipelineError
```

## Command Line Interface

### main
```python
def main() -> None
```
Main function with improved error handling and resource management.

**Execution Flow:**
1. Parse command line arguments
2. Validate date format
3. Initialize Spark session and BigQuery client
4. Create MetricsPipeline instance
5. Read and validate JSON configuration
6. Process metrics
7. Write results to BigQuery
8. Create reconciliation records
9. Clean up resources

**Error Handling:**
- Automatic rollback on failure
- Detailed error logging
- Graceful resource cleanup

**Example Usage:**
```bash
python pysaprk_copy.py \
    --gcs_path gs://bucket/config.json \
    --run_date 2024-01-15 \
    --dependencies "dep1,dep2" \
    --partition_info_table project.dataset.partition_info \
    --env PRD \
    --recon_table project.dataset.reconciliation
```

## Data Types

### Column Types
```python
COLUMN_TYPES = {
    'numeric_columns': ['metric_output', 'numerator_value', 'denominator_value'],
    'date_columns': ['business_data_date'],
    'required_columns': ['business_data_date']
}
```

### Recon Record Schema
```python
recon_schema = StructType([
    StructField("module_id", StringType(), False),
    StructField("module_type_nm", StringType(), False),
    StructField("source_databs_nm", StringType(), True),
    StructField("source_table_nm", StringType(), True),
    StructField("source_column_nm", StringType(), True),
    StructField("source_file_nm", StringType(), True),
    StructField("source_contrl_file_nm", StringType(), True),
    StructField("source_server_nm", StringType(), False),
    StructField("target_databs_nm", StringType(), True),
    StructField("target_table_nm", StringType(), True),
    StructField("target_column_nm", StringType(), True),
    StructField("target_file_nm", StringType(), True),
    StructField("target_contrl_file_nm", StringType(), True),
    StructField("target_server_nm", StringType(), False),
    StructField("source_vl", StringType(), False),
    StructField("target_vl", StringType(), False),
    StructField("clcltn_ds", StringType(), True),
    StructField("excldd_vl", StringType(), True),
    StructField("excldd_reason_tx", StringType(), True),
    StructField("tolrnc_pc", StringType(), True),
    StructField("rcncln_exact_pass_in", StringType(), False),
    StructField("rcncln_tolrnc_pass_in", StringType(), True),
    StructField("latest_source_parttn_dt", StringType(), False),
    StructField("latest_target_parttn_dt", StringType(), False),
    StructField("load_ts", StringType(), False),
    StructField("schdld_dt", DateType(), False),
    StructField("source_system_id", StringType(), False),
    StructField("schdld_yr", IntegerType(), False),
    StructField("Job_Name", StringType(), False)
])
```

---

This API reference provides comprehensive documentation for all classes, methods, and their parameters in the Metrics Pipeline Framework. For usage examples and best practices, refer to the Developer Guide and Quick Reference documents. 