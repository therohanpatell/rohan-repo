"""Configuration settings for the metrics pipeline"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from .constants import SPARK_CONFIG, BIGQUERY_WRITE_CONFIG, DEFAULT_VALUES


@dataclass
class SparkConfig:
    """Configuration for Spark session"""
    app_name: str = "OptimizedMetricsPipeline"
    config: Dict[str, str] = None
    
    def __post_init__(self):
        if self.config is None:
            self.config = SPARK_CONFIG.copy()
    
    def get_builder_config(self) -> Dict[str, str]:
        """Get configuration for Spark builder"""
        return self.config


@dataclass
class BigQueryConfig:
    """Configuration for BigQuery operations"""
    write_config: Dict[str, str] = None
    
    def __post_init__(self):
        if self.write_config is None:
            self.write_config = BIGQUERY_WRITE_CONFIG.copy()
    
    def get_write_options(self) -> Dict[str, str]:
        """Get write options for BigQuery"""
        return self.write_config


@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    
    # Core settings
    spark_config: SparkConfig = None
    bigquery_config: BigQueryConfig = None
    
    # Pipeline settings
    precision_threshold: str = DEFAULT_VALUES['precision_threshold']
    recon_module_id: str = DEFAULT_VALUES['recon_module_id']
    recon_module_type: str = DEFAULT_VALUES['recon_module_type']
    recon_source_vl: str = DEFAULT_VALUES['recon_source_vl']
    recon_na_value: str = DEFAULT_VALUES['recon_na_value']
    
    # Validation settings
    validate_denominator: bool = True
    allow_negative_denominator: bool = False
    log_small_denominator: bool = True
    
    # Rollback settings
    enable_rollback: bool = True
    rollback_timeout_hours: int = 1
    
    # Logging settings
    log_level: str = "INFO"
    log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    def __post_init__(self):
        if self.spark_config is None:
            self.spark_config = SparkConfig()
        if self.bigquery_config is None:
            self.bigquery_config = BigQueryConfig()
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'PipelineConfig':
        """Create configuration from dictionary"""
        return cls(**config_dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'spark_config': self.spark_config,
            'bigquery_config': self.bigquery_config,
            'precision_threshold': self.precision_threshold,
            'recon_module_id': self.recon_module_id,
            'recon_module_type': self.recon_module_type,
            'recon_source_vl': self.recon_source_vl,
            'recon_na_value': self.recon_na_value,
            'validate_denominator': self.validate_denominator,
            'allow_negative_denominator': self.allow_negative_denominator,
            'log_small_denominator': self.log_small_denominator,
            'enable_rollback': self.enable_rollback,
            'rollback_timeout_hours': self.rollback_timeout_hours,
            'log_level': self.log_level,
            'log_format': self.log_format
        }


def get_default_config() -> PipelineConfig:
    """Get default pipeline configuration"""
    return PipelineConfig()