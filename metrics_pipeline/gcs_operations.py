# metrics_pipeline/gcs_operations.py
"""Google Cloud Storage operations for the metrics pipeline."""

import logging
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame

from .validation import validate_gcs_path
from .exceptions import GCSOperationError

logger = logging.getLogger(__name__)

def read_json_from_gcs(spark: SparkSession, gcs_path: str) -> List[Dict[str, Any]]:
    """Read JSON file from GCS and return as list of dictionaries.

    Args:
        spark: SparkSession instance
        gcs_path: GCS path to JSON file

    Returns:
        List of metric definitions

    Raises:
        GCSOperationError: If file cannot be read or parsed
    """
    try:
        # Validate GCS path first
        validated_path = validate_gcs_path(spark, gcs_path)
        logger.info("Reading JSON from GCS: %s", validated_path)

        # Read JSON file using Spark
        df = spark.read.option("multiline", "true").json(validated_path)

        if df.count() == 0:
            raise GCSOperationError(f"No data found in JSON file: {validated_path}")

        # Convert to list of dictionaries
        json_data = [row.asDict() for row in df.collect()]
        logger.info("Successfully read %d records from JSON", len(json_data))
        return json_data

    except Exception as e:
        logger.error("Failed to read JSON from GCS: %s", str(e))
        raise GCSOperationError(f"Failed to read JSON from GCS: {str(e)}") from e
