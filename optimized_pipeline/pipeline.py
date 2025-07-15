import json
import re
from datetime import datetime
from typing import Dict, List, Tuple, Union, Optional

from google.cloud import bigquery
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    TimestampType,
)

from .exceptions import MetricsPipelineError
from .logging_utils import get_logger
from .number_utils import normalize_numeric_value, safe_decimal_conversion
from .sql_placeholder import replace_sql_placeholders
from .bq_utils import (
    align_schema_with_bq,
    write_to_bq_with_overwrite,
)

logger = get_logger(__name__)


class MetricsPipeline:
    """Refactored implementation of the original *pysaprk.MetricsPipeline*."""

    def __init__(self, spark: SparkSession, bq_client: bigquery.Client):
        self.spark = spark
        self.bq_client = bq_client
        self.processed_metrics: List[Dict] = []

    # ---------------------------------------------------------------------
    # ---------------------------- I/O helpers ----------------------------
    # ---------------------------------------------------------------------
    def _validate_gcs_path(self, gcs_path: str) -> str:
        if not gcs_path.startswith("gs://"):
            raise MetricsPipelineError("GCS path must start with 'gs://'")
        parts = gcs_path.replace("gs://", "").split("/")
        if len(parts) < 2:
            raise MetricsPipelineError("GCS path appears malformed: %s" % gcs_path)
        return gcs_path

    def read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """Load JSON definitions from *gcs_path* into a list of dicts."""
        gcs_path = self._validate_gcs_path(gcs_path)
        try:
            df = self.spark.read.option("multiline", "true").json(gcs_path)
            if df.count() == 0:
                raise MetricsPipelineError("No data found at %s" % gcs_path)
            return [row.asDict() for row in df.collect()]
        except Exception as exc:  # pragma: no cover
            logger.error("Unable to read JSON from %s: %s", gcs_path, exc)
            raise MetricsPipelineError("Failed to read JSON from GCS") from exc

    # ------------------------------------------------------------------
    # ----------------------- Validation helpers ----------------------
    # ------------------------------------------------------------------
    def validate_json(self, json_data: List[Dict]) -> List[Dict]:
        """Basic schema validation to ensure required fields are present."""
        required = {
            "metric_id",
            "metric_name",
            "metric_type",
            "sql",
            "dependency",
            "target_table",
        }
        seen_ids = set()
        for idx, record in enumerate(json_data):
            missing = required - record.keys()
            if missing:
                raise MetricsPipelineError(f"Record {idx} missing fields: {missing}")
            metric_id = record["metric_id"]
            if metric_id in seen_ids:
                raise MetricsPipelineError(f"Duplicate metric_id found: {metric_id}")
            seen_ids.add(metric_id)
        logger.info("Validated %d metric definitions", len(json_data))
        return json_data

    # ------------------------------------------------------------------
    # --------------------- Execution & processing ---------------------
    # ------------------------------------------------------------------
    def execute_sql(
        self,
        sql: str,
        *,
        run_date: str,
        partition_info_table: str,
    ) -> Dict[str, Union[str, None]]:
        """Apply placeholder substitution then run *sql* using BigQuery."""
        final_sql = replace_sql_placeholders(
            sql,
            run_date=run_date,
            partition_info_table=partition_info_table,
            bq_client=self.bq_client,
        )
        logger.debug("Executing SQL: %s", final_sql)
        query_job = self.bq_client.query(final_sql)
        results = query_job.result()

        # Build a deterministic result dict
        output = {
            "metric_output": None,
            "numerator_value": None,
            "denominator_value": None,
            "business_data_date": None,
        }
        for row in results:
            for key in output.keys():
                if key in row:
                    value = row[key]
                    if key in ("metric_output", "numerator_value", "denominator_value"):
                        output[key] = normalize_numeric_value(value)
                    else:
                        output[key] = value
            break  # only first row is consumed
        return output

    def _build_spark_schema(self) -> StructType:
        return StructType(
            [
                StructField("metric_id", StringType(), False),
                StructField("metric_name", StringType(), False),
                StructField("metric_type", StringType(), False),
                StructField("numerator_value", DecimalType(38, 9), True),
                StructField("denominator_value", DecimalType(38, 9), True),
                StructField("metric_output", DecimalType(38, 9), True),
                StructField("business_data_date", StringType(), False),
                StructField("partition_dt", StringType(), False),
                StructField("pipeline_execution_ts", TimestampType(), False),
            ]
        )

    def process_metrics(
        self,
        json_data: List[Dict],
        *,
        run_date: str,
        dependencies: List[str],
        partition_info_table: str,
    ) -> Dict[str, DataFrame]:
        """Execute metrics grouped by *target_table* and return Spark DataFrames."""
        partition_dt = datetime.utcnow().strftime("%Y-%m-%d")
        filtered = [rec for rec in json_data if rec["dependency"] in dependencies]
        if not filtered:
            raise MetricsPipelineError("No metric definitions matched the provided dependencies")

        grouped: Dict[str, List[Dict]] = {}
        for rec in filtered:
            grouped.setdefault(rec["target_table"].strip(), []).append(rec)

        results: Dict[str, DataFrame] = {}
        for table, records in grouped.items():
            processed_records: List[Dict] = []
            for record in records:
                try:
                    sql_res = self.execute_sql(
                        record["sql"],
                        run_date=run_date,
                        partition_info_table=partition_info_table,
                    )
                    processed_records.append(
                        {
                            "metric_id": record["metric_id"],
                            "metric_name": record["metric_name"],
                            "metric_type": record["metric_type"],
                            "numerator_value": safe_decimal_conversion(sql_res["numerator_value"]),
                            "denominator_value": safe_decimal_conversion(sql_res["denominator_value"]),
                            "metric_output": safe_decimal_conversion(sql_res["metric_output"]),
                            "business_data_date": sql_res["business_data_date"],
                            "partition_dt": partition_dt,
                            "pipeline_execution_ts": datetime.utcnow(),
                        }
                    )
                except Exception as exc:  # pragma: no cover
                    logger.error("Metric %s failed: %s", record["metric_id"], exc)
                    continue
            if processed_records:
                schema = self._build_spark_schema()
                df = self.spark.createDataFrame(processed_records, schema)
                results[table] = df
                self.processed_metrics.extend(processed_records)
                logger.info("Processed %d metrics for table %s", len(processed_records), table)
        return results

    # ------------------------------------------------------------------
    # ------------------------ BigQuery writers ------------------------
    # ------------------------------------------------------------------
    def write_results(self, metrics_dfs: Dict[str, DataFrame]) -> None:
        """Align schemas & write each DataFrame to its destination table."""
        partition_dt = datetime.utcnow().strftime("%Y-%m-%d")
        for table, df in metrics_dfs.items():
            aligned = align_schema_with_bq(self.spark, df, table, self.bq_client)
            write_to_bq_with_overwrite(aligned, table, partition_dt, self.bq_client)