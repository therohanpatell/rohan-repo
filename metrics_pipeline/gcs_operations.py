# metrics_pipeline/gcs_operations.py
"""Google Cloud Storage operations for the metrics pipeline."""

import logging
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame

from .exceptions import GCSOperationError

logger = logging.getLogger(__name__)

def validate_gcs_path(spark: SparkSession, gcs_path: str) -> str:
    """Validate GCS path format and accessibility.

    Args:
        spark: SparkSession instance
        gcs_path: GCS path to validate

    Returns:
        Validated GCS path

    Raises:
        GCSOperationError: If path is invalid or inaccessible
    """
    logger.info("Validating GCS path: %s", gcs_path)

    # Check basic format
    if not gcs_path.startswith('gs://'):
        raise GCSOperationError(f"Invalid GCS path format: {gcs_path}. Must start with 'gs://'")

    # Check path structure
    path_parts = gcs_path.replace('gs://', '').split('/')
    if len(path_parts) < 2:
        raise GCSOperationError(f"Invalid GCS path structure: {gcs_path}")

    try:
        # Try to read just the schema/structure without loading data
        test_df = spark.read.option("multiline", "true").json(gcs_path).limit(0)
        test_df.printSchema()  # This will fail if file doesn't exist
        logger.info("GCS path validated successfully: %s", gcs_path)
        return gcs_path
    except Exception as e:
        raise GCSOperationError(f"GCS path inaccessible: {gcs_path}. Error: {str(e)}") from e

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
