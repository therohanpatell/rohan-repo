import argparse
from datetime import datetime
from typing import List

from google.cloud import bigquery

from .logging_utils import get_logger
from .spark_utils import managed_spark_session
from .pipeline import MetricsPipeline
from .validation import validate_date_format
from .exceptions import MetricsPipelineError

logger = get_logger("OptimizedPipelineCLI")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optimized PySpark BigQuery Metrics Pipeline")
    parser.add_argument("--gcs_path", required=True, help="GCS path to the JSON configuration file")
    parser.add_argument("--run_date", required=True, help="Run date in YYYY-MM-DD format")
    parser.add_argument(
        "--dependencies",
        required=True,
        help="Comma-separated list of dependency identifiers to process",
    )
    parser.add_argument(
        "--partition_info_table",
        required=True,
        help="BigQuery table containing partition information (project.dataset.table)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    validate_date_format(args.run_date)
    dependencies: List[str] = [d.strip() for d in args.dependencies.split(",") if d.strip()]
    if not dependencies:
        raise MetricsPipelineError("At least one dependency must be provided")

    with managed_spark_session() as spark:
        bq_client = bigquery.Client()
        pipeline = MetricsPipeline(spark, bq_client)

        definitions = pipeline.read_json_from_gcs(args.gcs_path)
        pipeline.validate_json(definitions)
        metrics_dfs = pipeline.process_metrics(
            definitions,
            run_date=args.run_date,
            dependencies=dependencies,
            partition_info_table=args.partition_info_table,
        )
        if metrics_dfs:
            pipeline.write_results(metrics_dfs)
        else:
            logger.warning("No metrics were generated based on the provided configuration")

        logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    try:
        main()
    except MetricsPipelineError as exc:
        logger.error("Pipeline failed: %s", exc)
        raise