"""
Configuration module for Metrics Pipeline
Contains all constants, configurations, and setup utilities
"""

import logging
from typing import Dict, List
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    TimestampType, DecimalType, IntegerType
)


# Configure logging
def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(  # Set up standard logging format for entire pipeline
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)  # Return logger for this module


# Pipeline Constants
class PipelineConfig:
    """Pipeline configuration constants"""
    
    # Schema configurations for output tables
    METRICS_SCHEMA = StructType([  # Schema for metrics output table
        StructField("metric_id", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("metric_type", StringType(), False),
        StructField("frequency", StringType(), True),
        StructField("numerator_value", DecimalType(38, 9), True),
        StructField("denominator_value", DecimalType(38, 9), True),
        StructField("metric_output", DecimalType(38, 9), True),
        StructField("business_data_date", StringType(), False),
        StructField("partition_dt", StringType(), False),
        StructField("pipeline_execution_ts", TimestampType(), False)
    ])
    
    RECON_SCHEMA = StructType([  # Schema for reconciliation tracking table
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
    
    # Required JSON fields for metric configuration validation
    REQUIRED_JSON_FIELDS = [  # Fields that must be present in each metric JSON
        'metric_id', 'metric_name', 'metric_type', 
        'sql', 'dependency', 'target_table'
    ]
    
    # Spark configurations for optimization
    SPARK_CONFIGS = {  # Adaptive query execution settings
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
    
    QUERY_TIMEOUT = 180  # Maximum seconds for BigQuery operations
    
    # Recon tracking constants
    RECON_MODULE_ID = '103'  # Module identifier for recon records
    RECON_MODULE_TYPE = 'Metrics'  # Module type for recon records
    
    MAX_ERROR_MESSAGE_LENGTH = 500  # Truncate long error messages for storage
    
    # Error categories for debugging and monitoring
    ERROR_CATEGORIES = {  # Standardized error types for classification
        'PARTITION_VALIDATION_ERROR': 'Partition info table validation failed',
        'GCS_READ_ERROR': 'Failed to read JSON from GCS',
        'JSON_VALIDATION_ERROR': 'JSON data validation failed',
        'SQL_EXECUTION_ERROR': 'SQL query execution failed',
        'BIGQUERY_WRITE_ERROR': 'BigQuery write operation failed',
        'RECON_CREATION_ERROR': 'Recon record creation failed',
        'PIPELINE_EXECUTION_ERROR': 'General pipeline execution error',
        'PIPELINE_VALIDATION_ERROR': 'Pipeline validation error',
        'UNKNOWN_ERROR': 'Unknown error occurred'
    }


# Validation configurations
class ValidationConfig:
    """Validation rules and configurations"""
    
    @staticmethod
    def get_default_recon_values() -> Dict[str, str]:
        """Get default values for recon records"""
        return {  # Default 'NA' values for optional recon fields
            'source_column_nm': 'NA',
            'source_file_nm': 'NA',
            'source_contrl_file_nm': 'NA',
            'target_column_nm': 'NA',
            'target_file_nm': 'NA',
            'target_contrl_file_nm': 'NA',
            'tolrnc_pc': 'NA',
            'rcncln_tolrnc_pass_in': 'NA'
        }
