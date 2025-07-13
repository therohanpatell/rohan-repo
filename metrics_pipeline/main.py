# metrics_pipeline/main.py
"""Main entry point for the metrics pipeline."""

import argparse
import logging
import sys
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession
from google.cloud import bigquery

from .utils import configure_logging, validate_date_format, managed_spark_session
from .gcs_operations import read_json_from_gcs
from .validation import validate_json, validate_gcs_path
from .pipeline import MetricsPipeline
from .exceptions import MetricsPipelineError

logger = logging.getLogger(__name__)

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='PySpark BigQuery Metrics Pipeline'
    )

    parser.add_argument(
        '--gcs_path',
        required=True,
        help='GCS path to JSON input file'
    )
    parser.add_argument(
        '--run_date',
        required=True,
        help='Run date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--dependencies',
        required=True,
        help='Comma-separated list of dependencies to process'
    )
    parser.add_argument(
        '--partition_info_table',
        required=True,
        help='BigQuery table for partition info (project.dataset.table)'
    )
    parser.add_argument(
        '--target_table',
        required=True,
        help='Target BigQuery table (project.dataset.table)'
    )

    return parser.parse_args()

def main() -> None:
    """Main function with improved error handling and resource management."""
    # Configure logging at the start
    configure_logging()

    pipeline = None
    partition_dt = None

    try:
        # Parse arguments
        args = parse_arguments()

        # Validate date format
        validate_date_format(args.run_date)

        # Parse dependencies (strip whitespace)
        dependencies = [dep.strip() for dep in args.dependencies.split(',') if dep.strip()]

        if not dependencies:
            raise MetricsPipelineError("No valid dependencies provided")

        logger.info("Starting Metrics Pipeline")
        logger.info("GCS Path: %s", args.gcs_path)
        logger.info("Run Date: %s", args.run_date)
        logger.info("Dependencies: %s", dependencies)
        logger.info("Partition Info Table: %s", args.partition_info_table)
        logger.info("Target Table: %s", args.target_table)

        # Use managed Spark session
        with managed_spark_session("MetricsPipeline") as spark:
            # Initialize BigQuery client
            bq_client = bigquery.Client()

            # Initialize pipeline
            pipeline = MetricsPipeline(spark, bq_client)

            # Execute pipeline steps
            logger.info("Step 1: Reading JSON from GCS")
            json_data = read_json_from_gcs(spark, args.gcs_path)

            logger.info("Step 2: Validating JSON data")
            validated_data = validate_json(json_data)

            logger.info("Step 3: Processing metrics")
            metrics_df = pipeline.process_metrics(
                validated_data,
                args.run_date,
                dependencies,
                args.partition_info_table
            )

            # Store partition_dt for potential rollback
            partition_dt = datetime.now().strftime('%Y-%m-%d')

            logger.info("Step 4: Writing to BigQuery")
            pipeline.write_to_bq(metrics_df, args.target_table)

            logger.info("Pipeline completed successfully!")

    except MetricsPipelineError as e:
        logger.error("Pipeline failed: %s", str(e))

        # Attempt rollback if we have processed metrics
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("Attempting to rollback processed metrics")
                pipeline.rollback_processed_metrics(args.target_table, partition_dt)
            except Exception as rollback_error:
                logger.error("Rollback failed: %s", str(rollback_error))

        sys.exit(1)

    except Exception as e:
        logger.error("Unexpected error: %s", str(e))

        # Attempt rollback if we have processed metrics
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("Attempting to rollback processed metrics")
                pipeline.rollback_processed_metrics(args.target_table, partition_dt)
            except Exception as rollback_error:
                logger.error("Rollback failed: %s", str(rollback_error))

        sys.exit(1)

if __name__ == "__main__":
    main()
