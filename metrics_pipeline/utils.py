# metrics_pipeline/utils.py
"""Utility functions for the metrics pipeline."""

import logging
import sys
from datetime import datetime
from typing import Optional, Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession

def configure_logging() -> logging.Logger:
    """Configure logging for the pipeline.

    Returns:
        Configured logger instance.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def validate_date_format(date_str: str) -> None:
    """Validate that a date string is in YYYY-MM-DD format.

    Args:
        date_str: Date string to validate

    Raises:
        ValueError: If the date format is invalid
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD") from e

@contextmanager
def managed_spark_session(app_name: str = "MetricsPipeline") -> Iterator[SparkSession]:
    """Context manager for Spark session with proper cleanup.

    Args:
        app_name: Name for the Spark application

    Yields:
        SparkSession instance

    Raises:
        Exception: If there's an error creating/stopping the Spark session
    """
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        logging.getLogger(__name__).info(f"Spark session created successfully: {app_name}")
        yield spark
    except Exception as e:
        logging.getLogger(__name__).error(f"Error in Spark session: {str(e)}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logging.getLogger(__name__).info("Spark session stopped successfully")
            except Exception as e:
                logging.getLogger(__name__).error(f"Error stopping Spark session: {str(e)}")
