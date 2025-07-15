"""Constants for the metrics pipeline"""

import re
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    TimestampType, DecimalType, IntegerType, DoubleType
)

# Required fields for JSON validation
REQUIRED_JSON_FIELDS = [
    'metric_id', 'metric_name', 'metric_type', 
    'sql', 'dependency', 'target_table'
]

# Regex patterns for SQL placeholders
PLACEHOLDER_PATTERNS = {
    'currently': re.compile(r'\{currently\}'),
    'partition_info': re.compile(r'\{partition_info\}'),
    'table_reference': re.compile(r'`([^.]+)\.([^.]+)\.([^`]+)`')
}

# Schema definitions
SCHEMA_DEFINITIONS = {
    'metrics': StructType([
        StructField("metric_id", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("metric_type", StringType(), False),
        StructField("numerator_value", DecimalType(38, 9), True),
        StructField("denominator_value", DecimalType(38, 9), True),
        StructField("metric_output", DecimalType(38, 9), True),
        StructField("business_data_date", StringType(), False),
        StructField("partition_dt", StringType(), False),
        StructField("pipeline_execution_ts", TimestampType(), False)
    ]),
    
    'recon': StructType([
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
}

# BigQuery write configurations
BIGQUERY_WRITE_CONFIG = {
    'format': 'bigquery',
    'writeMethod': 'direct',
    'mode': 'append'
}

# Spark configurations
SPARK_CONFIG = {
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true'
}

# Default values
DEFAULT_VALUES = {
    'recon_module_id': '103',
    'recon_module_type': 'Metrics',
    'recon_source_vl': '0',
    'recon_na_value': 'NA',
    'precision_threshold': '0.0000001'
}

# Error messages
ERROR_MESSAGES = {
    'invalid_gcs_path': "Invalid GCS path format: {path}. Must start with 'gs://'",
    'invalid_path_structure': "Invalid GCS path structure: {path}",
    'file_not_accessible': "GCS path inaccessible: {path}. Error: {error}",
    'missing_field': "Record {index}: Missing required field '{field}'",
    'empty_field': "Record {index}: Field '{field}' is null, empty, or contains only whitespace",
    'duplicate_metric': "Record {index}: Duplicate metric_id '{metric_id}' found",
    'invalid_table_format': "Record {index}: target_table '{table}' must be in format 'project.dataset.table'",
    'empty_table_part': "Record {index}: target_table '{table}' has empty {part} part",
    'zero_denominator': "Metric '{metric_id}': Invalid denominator value: denominator_value is 0",
    'negative_denominator': "Metric '{metric_id}': Invalid denominator value: denominator_value is negative ({value})",
    'small_denominator': "Metric '{metric_id}': Very small denominator value detected: {value}",
    'missing_business_date': "business_data_date is required but was not returned by the SQL query",
    'sql_execution_failed': "Metric '{metric_id}': Failed to execute SQL: {error}",
    'table_not_found': "Table not found: {table}",
    'rollback_failed': "Failed to rollback metric {metric_id}: {error}"
}