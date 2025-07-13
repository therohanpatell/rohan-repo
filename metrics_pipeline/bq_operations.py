# metrics_pipeline/bq_operations.py
"""BigQuery operations for the metrics pipeline."""

import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Any, Dict

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError

from .exceptions import BigQueryOperationError

logger = logging.getLogger(__name__)

def get_partition_dt(
    bq_client: bigquery.Client,
    project_dataset: str,
    table_name: str,
    partition_info_table: str
) -> Optional[str]:
    """Get latest partition_dt from metadata table.

    Args:
        bq_client: BigQuery client
        project_dataset: Dataset name
        table_name: Table name
        partition_info_table: Metadata table name

    Returns:
        Latest partition date as string or None
    """
    try:
        query = f"""
        SELECT partition_dt
        FROM `{partition_info_table}`
        WHERE project_dataset = '{project_dataset}'
        AND table_name = '{table_name}'
        ORDER BY partition_dt DESC
        LIMIT 1
        """

        logger.info("Querying partition info for %s.%s", project_dataset, table_name)

        query_job = bq_client.query(query)
        results = query_job.result()

        for row in results:
            partition_dt = row.partition_dt
            if isinstance(partition_dt, datetime):
                return partition_dt.strftime('%Y-%m-%d')
            return str(partition_dt)

        logger.warning("No partition info found for %s.%s", project_dataset, table_name)
        return None

    except Exception as e:
        logger.error("Failed to get partition_dt for %s.%s: %s",
                   project_dataset, table_name, str(e))
        return None

def get_bq_table_schema(
    bq_client: bigquery.Client,
    table_name: str
) -> List[bigquery.SchemaField]:
    """Get BigQuery table schema.

    Args:
        bq_client: BigQuery client
        table_name: Full table name (project.dataset.table)

    Returns:
        List of schema fields

    Raises:
        BigQueryOperationError: If table is not found or schema cannot be retrieved
    """
    try:
        logger.info("Getting schema for table: %s", table_name)
        table = bq_client.get_table(table_name)
        return table.schema

    except NotFound as e:
        raise BigQueryOperationError(f"Table not found: {table_name}") from e
    except Exception as e:
        logger.error("Failed to get table schema: %s", str(e))
        raise BigQueryOperationError(f"Failed to get table schema: {str(e)}") from e

def execute_bq_query(
    bq_client: bigquery.Client,
    query: str
) -> List[Dict[str, Any]]:
    """Execute a BigQuery query and return results.

    Args:
        bq_client: BigQuery client
        query: SQL query to execute

    Returns:
        List of dictionaries representing query results
    """
    try:
        logger.info("Executing BigQuery query")
        query_job = bq_client.query(query)
        return [dict(row) for row in query_job.result()]

    except Exception as e:
        logger.error("Failed to execute query: %s", str(e))
        raise BigQueryOperationError(f"Failed to execute query: {str(e)}") from e
