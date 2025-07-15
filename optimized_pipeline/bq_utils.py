from typing import List, Dict, Optional

from google.cloud import bigquery
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType

from .exceptions import MetricsPipelineError
from .logging_utils import get_logger

logger = get_logger(__name__)


def get_bq_table_schema(table_name: str, bq_client: bigquery.Client) -> List[bigquery.SchemaField]:
    """Fetch and return the BigQuery table schema."""
    try:
        table = bq_client.get_table(table_name)
        return list(table.schema)
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to fetch schema for %s: %s", table_name, exc)
        raise MetricsPipelineError(f"Unable to obtain BigQuery schema for {table_name}") from exc


def align_schema_with_bq(spark: SparkSession, df: DataFrame, table_name: str, bq_client: bigquery.Client) -> DataFrame:
    """Align *df*'s schema with the destination BigQuery table.

    Currently this only handles widening decimal precision as required.
    """
    bq_schema = get_bq_table_schema(table_name, bq_client)
    spark_schema = df.schema

    # Iterate over BQ schema and adjust relevant Spark columns
    for field in bq_schema:
        if field.name not in spark_schema.names:
            continue
        spark_field = spark_schema[field.name]
        if isinstance(spark_field.dataType, DecimalType):
            bq_precision = getattr(field, "precision", 38)
            bq_scale = getattr(field, "scale", 9)
            if (spark_field.dataType.precision, spark_field.dataType.scale) != (bq_precision, bq_scale):
                df = df.withColumn(field.name, df[field.name].cast(DecimalType(bq_precision, bq_scale)))
    return df


def check_existing_metrics(metric_ids: List[str], partition_dt: str, table_name: str, bq_client: bigquery.Client) -> List[str]:
    """Return a list of *metric_id* values already present in *table_name* for *partition_dt*."""
    param_ids = ",".join([f"'{m_id}'" for m_id in metric_ids])
    query = (
        f"""
        SELECT DISTINCT metric_id
        FROM `{table_name}`
        WHERE partition_dt = '{partition_dt}'
        AND metric_id IN ({param_ids})
        """
    )
    try:
        return [row.metric_id for row in bq_client.query(query).result()]
    except Exception as exc:
        logger.error("Error checking existing metrics on %s: %s", table_name, exc)
        raise MetricsPipelineError("Failed to check existing metrics") from exc


def delete_existing_metrics(metric_ids: List[str], partition_dt: str, table_name: str, bq_client: bigquery.Client) -> None:
    """Delete duplicate metric records prior to overwriting."""
    if not metric_ids:
        return
    ids = ",".join([f"'{m_id}'" for m_id in metric_ids])
    query = (
        f"""
        DELETE FROM `{table_name}`
        WHERE partition_dt = '{partition_dt}'
        AND metric_id IN ({ids})
        """
    )
    try:
        bq_client.query(query).result()
        logger.info("Deleted %d duplicate metrics from %s", len(metric_ids), table_name)
    except Exception as exc:
        logger.error("Failed to delete existing metrics from %s: %s", table_name, exc)
        raise MetricsPipelineError("Failed to delete existing metrics for overwrite") from exc


def write_to_bq_with_overwrite(df: DataFrame, table_name: str, partition_dt: str, bq_client: bigquery.Client) -> List[str]:
    """Write *df* to *table_name*, overwriting duplicate *metric_id* rows for *partition_dt*."""
    metric_ids = [row["metric_id"] for row in df.select("metric_id").collect()]
    existing = check_existing_metrics(metric_ids, partition_dt, table_name, bq_client)
    delete_existing_metrics(existing, partition_dt, table_name, bq_client)

    df.write.format("bigquery").option("table", table_name).option("writeMethod", "direct").mode("append").save()
    logger.info("Written %d metrics to %s", df.count(), table_name)
    return metric_ids