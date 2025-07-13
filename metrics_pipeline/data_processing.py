# metrics_pipeline/data_processing.py
"""Data processing functions for the metrics pipeline."""

import logging
import re
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Tuple, Dict, Any, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    TimestampType, DecimalType, DoubleType
)

from google.cloud import bigquery

from .exceptions import MetricsPipelineError

logger = logging.getLogger(__name__)

def parse_tables_from_sql(sql: str) -> List[Tuple[str, str]]:
    """Parse all project_dataset and table_name from SQL query.

    Args:
        sql: SQL query string

    Returns:
        List of tuples (project_dataset, table_name)
    """
    # Pattern to match BigQuery table references like `project.dataset.table`
    pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
    matches = re.findall(pattern, sql)

    if matches:
        # Return dataset and table name for each match
        tables = []
        for project, dataset, table in matches:
            tables.append((dataset, table))
        return tables

    return []

def replace_run_dates_in_sql(sql: str, replacement_dates: List[str]) -> str:
    """Replace {run_date} placeholders in SQL with corresponding dates.

    Args:
        sql: SQL query string with {run_date} placeholders
        replacement_dates: List of dates to replace placeholders

    Returns:
        SQL with all {run_date} placeholders replaced

    Raises:
        MetricsPipelineError: If number of placeholders doesn't match replacement dates
    """
    # Find all {run_date} occurrences
    run_date_pattern = r'\{run_date\}'
    matches = list(re.finditer(run_date_pattern, sql))

    if len(matches) != len(replacement_dates):
        raise MetricsPipelineError(
            f"Number of {{run_date}} placeholders ({len(matches)}) doesn't match "
            f"number of replacement dates ({len(replacement_dates)})"
        )

    # Replace from end to beginning to preserve indices
    final_sql = sql
    for i, match in enumerate(reversed(matches)):
        replacement_index = len(matches) - 1 - i
        replacement_date = replacement_dates[replacement_index]
        start, end = match.span()
        final_sql = final_sql[:start] + f"'{replacement_date}'" + final_sql[end:]

    logger.info("Replaced %d {run_date} placeholders", len(matches))
    logger.debug("Final SQL: %s", final_sql)

    return final_sql

def normalize_numeric_value(value: Union[int, float, Decimal, str, None]) -> Optional[str]:
    """Normalize numeric values to string representation to preserve precision.

    Args:
        value: Numeric value of any type

    Returns:
        String representation of the number or None
    """
    if value is None:
        return None

    try:
        # Handle different numeric types with precision preservation
        if isinstance(value, Decimal):
            # Keep as string to preserve precision
            return str(value)
        elif isinstance(value, (int, float)):
            # Convert to Decimal first to handle large numbers properly
            decimal_val = Decimal(str(value))
            return str(decimal_val)
        elif isinstance(value, str):
            # Try to parse as Decimal to validate it's a valid number
            try:
                decimal_val = Decimal(value)
                return str(decimal_val)
            except:
                logger.warning("Could not parse string as number: %s", value)
                return None
        else:
            # Try to convert to string and then to Decimal
            decimal_val = Decimal(str(value))
            return str(decimal_val)

    except (ValueError, TypeError, OverflowError, Exception) as e:
        logger.warning("Could not normalize numeric value: %s, error: %s", value, e)
        return None

def safe_decimal_conversion(value: Optional[str]) -> Optional[Decimal]:
    """Safely convert string to Decimal for BigQuery.

    Args:
        value: String representation of number

    Returns:
        Decimal value or None
    """
    if value is None:
        return None

    try:
        return Decimal(value)
    except (ValueError, TypeError, OverflowError):
        logger.warning("Could not convert to Decimal: %s", value)
        return None

def align_schema_with_bq(df: DataFrame, bq_client: bigquery.Client,
                         target_table: str) -> DataFrame:
    """Align Spark DataFrame with BigQuery table schema.

    Args:
        df: Spark DataFrame
        bq_client: BigQuery client
        target_table: BigQuery table name

    Returns:
        Schema-aligned DataFrame

    Raises:
        MetricsPipelineError: If schema alignment fails
    """
    logger.info("Aligning DataFrame schema with BigQuery table: %s", target_table)

    try:
        # Get BigQuery schema
        table = bq_client.get_table(target_table)
        bq_schema = table.schema
        bq_columns = [field.name for field in bq_schema]

        # Get current DataFrame columns
        current_columns = df.columns

        # Drop extra columns not in BigQuery schema
        columns_to_keep = [col for col in current_columns if col in bq_columns]
        columns_to_drop = [col for col in current_columns if col not in bq_columns]

        if columns_to_drop:
            logger.info("Dropping extra columns: %s", columns_to_drop)
            df = df.drop(*columns_to_drop)

        # Reorder columns to match BigQuery schema
        df = df.select(*[col(c) for c in bq_columns if c in columns_to_keep])

        # Handle type conversions for BigQuery compatibility
        for field in bq_schema:
            if field.name in df.columns:
                if field.field_type == 'DATE':
                    df = df.withColumn(field.name, to_date(col(field.name)))
                elif field.field_type == 'TIMESTAMP':
                    df = df.withColumn(field.name, col(field.name).cast(TimestampType()))
                elif field.field_type == 'NUMERIC':
                    df = df.withColumn(field.name, col(field.name).cast(DecimalType(38, 9)))
                elif field.field_type == 'FLOAT':
                    df = df.withColumn(field.name, col(field.name).cast(DoubleType()))

        logger.info("Schema alignment complete. Final columns: %s", df.columns)
        return df

    except Exception as e:
        logger.error("Schema alignment failed: %s", str(e))
        raise MetricsPipelineError(f"Schema alignment failed: {str(e)}") from e
