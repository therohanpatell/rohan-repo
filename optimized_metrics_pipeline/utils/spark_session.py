"""Spark session utilities for the metrics pipeline"""

from contextlib import contextmanager
from typing import Optional
from pyspark.sql import SparkSession
from ..config.settings import SparkConfig
from .logging import get_logger

logger = get_logger(__name__)


def create_spark_session(config: SparkConfig) -> SparkSession:
    """
    Create a Spark session with the given configuration
    
    Args:
        config: Spark configuration
        
    Returns:
        SparkSession instance
    """
    try:
        builder = SparkSession.builder.appName(config.app_name)
        
        # Apply configuration
        for key, value in config.get_builder_config().items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        logger.info(f"Spark session created successfully: {config.app_name}")
        
        return spark
        
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise


@contextmanager
def managed_spark_session(config: SparkConfig):
    """
    Context manager for Spark session with proper cleanup
    
    Args:
        config: Spark configuration
        
    Yields:
        SparkSession instance
    """
    spark = None
    try:
        spark = create_spark_session(config)
        yield spark
        
    except Exception as e:
        logger.error(f"Error in Spark session: {str(e)}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")


def get_or_create_spark_session(config: SparkConfig) -> SparkSession:
    """
    Get existing Spark session or create a new one
    
    Args:
        config: Spark configuration
        
    Returns:
        SparkSession instance
    """
    try:
        # Try to get existing session
        spark = SparkSession.getActiveSession()
        if spark:
            logger.info("Using existing Spark session")
            return spark
        
        # Create new session if none exists
        return create_spark_session(config)
        
    except Exception as e:
        logger.error(f"Failed to get or create Spark session: {str(e)}")
        raise