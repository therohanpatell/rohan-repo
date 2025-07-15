from contextlib import contextmanager

from pyspark.sql import SparkSession

from .logging_utils import get_logger

logger = get_logger(__name__)


@contextmanager
def managed_spark_session(app_name: str = "OptimizedMetricsPipeline"):
    """Context manager that returns a configured :class:`pyspark.sql.SparkSession`."""
    spark = None
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        logger.info("Spark session [%s] created", app_name)
        yield spark
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")