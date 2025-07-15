import re
from datetime import datetime
from typing import List, Tuple, Optional

from google.cloud import bigquery

from .exceptions import MetricsPipelineError
from .logging_utils import get_logger

logger = get_logger(__name__)


_PLACEHOLDER_CURRENTLY = r"\{currently\}"
_PLACEHOLDER_PARTITION_INFO = r"\{partition_info\}"
_TABLE_REGEX = r"`([^.]+)\.([^.]+)\.([^`]+)`"  # project.dataset.table enclosed in backticks


def find_placeholder_positions(sql: str) -> List[Tuple[str, int, int]]:
    """Return a list of placeholders in *sql* alongside their positions.

    Each tuple contains *(placeholder_type, start_index, end_index)*.
    """
    placeholders: List[Tuple[str, int, int]] = []
    for match in re.finditer(_PLACEHOLDER_CURRENTLY, sql):
        placeholders.append(("currently", match.start(), match.end()))
    for match in re.finditer(_PLACEHOLDER_PARTITION_INFO, sql):
        placeholders.append(("partition_info", match.start(), match.end()))
    # Preserve deterministic order
    placeholders.sort(key=lambda x: x[1])
    return placeholders


def _get_table_before_placeholder(sql: str, placeholder_pos: int) -> Optional[Tuple[str, str]]:
    """Return *(dataset, table)* that appears immediately before *placeholder_pos*."""
    closest_distance = float("inf")
    chosen: Optional[Tuple[str, str]] = None
    for match in re.finditer(_TABLE_REGEX, sql):
        if match.end() < placeholder_pos:
            distance = placeholder_pos - match.end()
            if distance < closest_distance:
                closest_distance = distance
                _, dataset, table = match.groups()
                chosen = (dataset, table)
    return chosen


def _get_partition_dt(dataset: str, table: str, partition_info_table: str, bq_client: bigquery.Client) -> Optional[str]:
    """Query *partition_info_table* for the latest ``partition_dt`` value."""
    query = (
        f"""
        SELECT partition_dt
        FROM `{partition_info_table}`
        WHERE project_dataset = '{dataset}' AND table_name = '{table}'
        ORDER BY partition_dt DESC
        LIMIT 1
        """
    )
    try:
        results = bq_client.query(query).result()
        for row in results:
            partition_dt = row.partition_dt
            if isinstance(partition_dt, datetime):
                return partition_dt.strftime("%Y-%m-%d")
            return str(partition_dt)
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to fetch partition_dt for %s.%s: %s", dataset, table, exc)
    return None


def replace_sql_placeholders(sql: str, *, run_date: str, partition_info_table: str, bq_client: bigquery.Client) -> str:
    """Replace ``{currently}`` and ``{partition_info}`` placeholders in *sql*."""
    placeholders = find_placeholder_positions(sql)
    if not placeholders:
        return sql

    final_sql = sql
    # Iterate from the back to keep indices stable
    for placeholder_type, start, end in reversed(placeholders):
        if placeholder_type == "currently":
            replacement = f"'{run_date}'"
        else:  # partition_info
            table_info = _get_table_before_placeholder(sql, start)
            if not table_info:
                raise MetricsPipelineError(
                    f"Could not resolve table reference for {{partition_info}} at position {start}"
                )
            dataset, table = table_info
            replacement_dt = _get_partition_dt(dataset, table, partition_info_table, bq_client)
            if not replacement_dt:
                raise MetricsPipelineError(
                    f"Could not determine partition_dt for table {dataset}.{table}"
                )
            replacement = f"'{replacement_dt}'"
        final_sql = final_sql[:start] + replacement + final_sql[end:]
    return final_sql