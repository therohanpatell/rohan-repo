#!/usr/bin/env python3
"""backup_and_validate.py  (hardened)

Create a genuine BigQuery TABLE SNAPSHOT of a production table and PROVE that
the snapshot is an exact copy of the original as of the snapshot instant.

This script NEVER creates, alters, drops or truncates the production table.
It issues exactly one write: CREATE SNAPSHOT TABLE ... CLONE ...
Everything else is read-only.

Security / safety guardrails
----------------------------
  * every identifier is regex-validated before it reaches SQL (no injection)
  * the original is read via FOR SYSTEM_TIME AS OF <snapshot_time> so concurrent
    writes cannot produce a false PASS or a spurious FAIL
  * streaming buffer is detected and blocks the run (snapshots exclude it)
  * column policy tags (PII) are captured and verified to survive into the
    snapshot; loss or alteration of a tag is a hard failure
  * query cache is disabled for every validation query
  * maximum_bytes_billed caps runaway scans
  * values of policy-tagged columns are redacted from logs and reports
  * the created object is asserted to be table_type == SNAPSHOT

Exit code 0 -> snapshot validated, safe to use for migration
Exit code 1 -> blocked or FAILED. The snapshot, if created, is retained.

Usage
-----
python backup_and_validate.py \
    --project PROJECT_ID \
    --dataset DATASET \
    --table TABLE_NAME \
    --snapshot-dataset SNAPSHOT_DATASET \
    --retention-days 7 \
    [--primary-key customer_id,transaction_id] \
    [--location asia-south1] \
    [--max-bytes-billed 1099511627776] \
    [--partition-filter "event_date >= '2024-01-01'"] \
    [--json-report backup_report.json]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Sequence

from google.api_core import exceptions as gexc
from google.cloud import bigquery

LOG = logging.getLogger("bq.backup")

HASH_SALTS = ("", "s1|", "s2|", "s3|")  # 4 independent 64-bit fingerprints
JOB_LABELS = {"tool": "bq-terraform-migration", "phase": "backup"}

# --------------------------------------------------------------------------- #
# Identifier / expression validation (SQL injection guardrail)
# --------------------------------------------------------------------------- #

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$")
PROJECT_RE = re.compile(r"^(?:[a-z][a-z0-9.-]{0,62}:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$")
# Table names may contain dashes; dataset and column names may not.
TABLE_RE = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")
FORBIDDEN_IN_EXPR = (";", "--", "/*", "*/", "\x00")


class BackupError(RuntimeError):
    """Fatal condition. Stop and return a non-zero exit code."""


class ValidationFailure(BackupError):
    """A configured validation check did not pass."""


def valid_ident(name: str, kind: str) -> str:
    """Accept only plain BigQuery identifiers. Rejects quotes, dots, newlines."""
    if not IDENT_RE.match(name or ""):
        raise BackupError(
            f"Invalid {kind} {name!r}: must match [A-Za-z_][A-Za-z0-9_]* "
            "(no quotes, dots, spaces or newlines)"
        )
    return name


def valid_table_name(name: str, kind: str = "table") -> str:
    """Table names allow dashes; still no quotes, dots, spaces or newlines."""
    if not TABLE_RE.match(name or ""):
        raise BackupError(
            f"Invalid {kind} {name!r}: only letters, digits, underscore and dash are allowed"
        )
    return name


def valid_project(name: str) -> str:
    if not PROJECT_RE.match(name or ""):
        raise BackupError(f"Invalid project id {name!r}")
    return name


def valid_expression(expr: str, kind: str) -> str:
    """Operator-supplied SQL fragment. Blocks statement chaining and comments."""
    if not expr or not expr.strip():
        raise BackupError(f"Empty {kind}")
    for bad in FORBIDDEN_IN_EXPR:
        if bad in expr:
            raise BackupError(f"{kind} contains forbidden sequence {bad!r}: {expr!r}")
    if expr.count("(") != expr.count(")"):
        raise BackupError(f"{kind} has unbalanced parentheses: {expr!r}")
    if "`" in expr:
        raise BackupError(f"{kind} must not contain backticks: {expr!r}")
    return expr.strip()


def q(identifier: str) -> str:
    return f"`{valid_ident(identifier, 'identifier')}`"


def fq(project: str, dataset: str, table: str) -> str:
    return f"{valid_project(project)}.{valid_ident(dataset, 'dataset')}.{valid_table_name(table)}"


def bq_ref(table_id: str) -> str:
    project, dataset, table = table_id.split(".", 2)
    valid_project(project)
    valid_ident(dataset, "dataset")
    valid_table_name(table)
    return f"`{table_id}`"


def col_ref(alias: str, name: str) -> str:
    return f"{alias}.{q(name)}"


# --------------------------------------------------------------------------- #
# Config / results
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ValidationConfig:
    schema: bool = True
    row_count: bool = True
    statistics: bool = True
    distinct_count: bool = True
    hash_check: bool = True
    policy_tag_check: bool = True
    approx_distinct: bool = False
    float_tolerance: float = 1e-9

    def executed(self) -> list[str]:
        keys = ("schema", "row_count", "statistics", "distinct_count", "hash_check", "policy_tag_check")
        return [f"{k}={'ON' if getattr(self, k) else 'OFF'}" for k in keys]


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""
    critical: bool = True


@dataclass
class Report:
    original: str = ""
    snapshot: str = ""
    checks: list[CheckResult] = field(default_factory=list)
    facts: dict[str, Any] = field(default_factory=dict)

    def add(self, name: str, passed: bool, detail: str = "", critical: bool = True) -> None:
        self.checks.append(CheckResult(name, passed, detail, critical))
        LOG.log(
            logging.INFO if passed else logging.ERROR,
            "check=%s result=%s %s", name, "PASS" if passed else "FAIL", detail,
        )

    def status(self, name: str) -> str:
        for c in self.checks:
            if c.name == name:
                return "PASS" if c.passed else "FAIL"
        return "SKIPPED"

    @property
    def ok(self) -> bool:
        return all(c.passed for c in self.checks if c.critical)


# --------------------------------------------------------------------------- #
# Query execution guardrails
# --------------------------------------------------------------------------- #


class QueryRunner:
    """Wraps client.query with cost caps, cache disabled and audit labels."""

    def __init__(
        self,
        client: bigquery.Client,
        max_bytes_billed: int | None,
        timeout_s: float | None,
        labels: dict[str, str],
    ) -> None:
        self.client = client
        self.max_bytes_billed = max_bytes_billed
        self.timeout_s = timeout_s
        self.labels = labels
        self.total_bytes = 0

    def run(self, sql: str, label: str, dry_run: bool = False) -> tuple[list[dict[str, Any]], Any]:
        LOG.debug("query[%s]:\n%s", label, sql)
        cfg = bigquery.QueryJobConfig(
            dry_run=dry_run,
            use_query_cache=False,  # never trust cached results when proving equality
            maximum_bytes_billed=self.max_bytes_billed,
            labels={**self.labels, "step": re.sub(r"[^a-z0-9_-]", "_", label.lower())[:63]},
        )
        try:
            job = self.client.query(sql, job_config=cfg)
            if dry_run:
                LOG.info("dry_run=%s bytes_estimated=%s", label, job.total_bytes_processed)
                return [], job
            rows = [dict(r) for r in job.result(timeout=self.timeout_s)]
        except gexc.Forbidden as exc:
            raise BackupError(
                f"Access denied running '{label}'. If this table has policy tags, the service "
                "account also needs roles/datacatalog.categoryFineGrainedReader on the taxonomy. "
                f"Underlying error: {exc}"
            ) from exc
        except gexc.BadRequest as exc:
            if "bytes billed" in str(exc).lower():
                raise BackupError(
                    f"Query '{label}' would exceed --max-bytes-billed ({self.max_bytes_billed:,}). "
                    "Raise the cap or narrow the scan with --partition-filter."
                ) from exc
            raise
        self.total_bytes += job.total_bytes_processed or 0
        LOG.info("query=%s job_id=%s bytes=%s", label, job.job_id, job.total_bytes_processed)
        return rows, job


# --------------------------------------------------------------------------- #
# Scan helper: time travel + partition filter
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Scan:
    """Renders a consistent FROM/WHERE for every validation query."""

    table_id: str
    alias: str = "src"
    as_of: datetime | None = None
    partition_filter: str | None = None

    def from_clause(self) -> str:
        ref = bq_ref(self.table_id)
        if self.as_of:
            stamp = self.as_of.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
            ref += f' FOR SYSTEM_TIME AS OF TIMESTAMP "{stamp} UTC"'
        return f"FROM {ref} AS {self.alias}"

    def where_clause(self) -> str:
        return f"\nWHERE {self.partition_filter}" if self.partition_filter else ""

    def tail(self) -> str:
        return f"\n{self.from_clause()}{self.where_clause()}"


# --------------------------------------------------------------------------- #
# Schema / policy tags
# --------------------------------------------------------------------------- #


def normalise(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def standard_sql_type(f: bigquery.SchemaField) -> str:
    base = f.field_type.upper()
    alias = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL", "RECORD": "STRUCT"}.get(base, base)
    if alias == "STRUCT":
        inner = ", ".join(f"{sub.name} {standard_sql_type(sub)}" for sub in f.fields)
        alias = f"STRUCT<{inner}>"
    if f.mode == "REPEATED":
        return f"ARRAY<{alias}>"
    return alias


def schema_signature(fields: Sequence[bigquery.SchemaField]) -> list[dict[str, Any]]:
    out = []
    for f in fields:
        out.append(
            {
                "name": f.name,
                "type": f.field_type.upper(),
                "mode": (f.mode or "NULLABLE").upper(),
                "fields": schema_signature(f.fields) if f.fields else [],
            }
        )
    return out


def diff_schemas(a: Sequence[bigquery.SchemaField], b: Sequence[bigquery.SchemaField]) -> list[str]:
    diffs: list[str] = []

    def walk(x: list[dict[str, Any]], y: list[dict[str, Any]], path: str) -> None:
        if len(x) != len(y):
            diffs.append(f"{path or '<root>'}: column count {len(x)} vs {len(y)}")
        for i in range(max(len(x), len(y))):
            xi = x[i] if i < len(x) else None
            yi = y[i] if i < len(y) else None
            if xi is None:
                diffs.append(f"[{i}]: extra column in snapshot ({yi['name']})")
                continue
            if yi is None:
                diffs.append(f"[{i}]: column missing from snapshot ({xi['name']})")
                continue
            label = f"{path}{'.' if path else ''}{xi['name']}"
            if xi["name"] != yi["name"]:
                diffs.append(f"[{i}]: name {xi['name']} vs {yi['name']} (ordinal mismatch)")
                continue
            if xi["type"] != yi["type"]:
                diffs.append(f"{label}: type {xi['type']} vs {yi['type']}")
            if xi["mode"] != yi["mode"]:
                diffs.append(f"{label}: mode {xi['mode']} vs {yi['mode']}")
            if xi["fields"] or yi["fields"]:
                walk(xi["fields"], yi["fields"], label)

    walk(schema_signature(a), schema_signature(b), "")
    return diffs


def describe_partitioning(table: bigquery.Table) -> dict[str, Any] | None:
    """Normalised partition spec, or None for an unpartitioned table."""
    tp = table.time_partitioning
    if tp:
        return {
            "kind": "time",
            "granularity": tp.type_,
            "field": tp.field,  # None means ingestion-time (_PARTITIONTIME)
            "expiration_ms": tp.expiration_ms,
            "require_filter": bool(getattr(tp, "require_partition_filter", False)),
        }
    rp = table.range_partitioning
    if rp:
        return {
            "kind": "range",
            "field": rp.field,
            "start": rp.range_.start,
            "end": rp.range_.end,
            "interval": rp.range_.interval,
        }
    return None


# --------------------------------------------------------------------------- #
# require_partition_filter
# --------------------------------------------------------------------------- #


def partition_column(table: bigquery.Table) -> tuple[str | None, bool]:
    """Return (partition_column, is_ingestion_time).

    An ingestion-time partitioned table has no partition column of its own; it
    partitions on the _PARTITIONTIME pseudo-column.
    """
    tp = table.time_partitioning
    if tp:
        return tp.field, tp.field is None
    rp = table.range_partitioning
    if rp:
        return rp.field, False
    return None, False


def requires_partition_filter(table: bigquery.Table) -> bool:
    tp = table.time_partitioning
    return bool(
        getattr(table, "require_partition_filter", False)
        or (tp is not None and getattr(tp, "require_partition_filter", False))
    )


def resolve_partition_filter(
    runner: "QueryRunner", table: bigquery.Table, supplied: str | None, table_id: str
) -> str | None:
    """Build a filter that satisfies require_partition_filter and keeps every partition.

    `<partition_column> IS NOT NULL` is a predicate on the partition column, so
    BigQuery accepts it, and it matches every partition that holds a value - the
    whole table, not a slice of it.

    The one gap is the __NULL__ partition, holding rows whose partition column
    is NULL. IS NOT NULL would exclude those, so they are counted first (a cheap
    query touching only that partition) and the filter is widened when any exist.
    """
    if not requires_partition_filter(table):
        return supplied
    if supplied:
        LOG.warning(
            "%s requires a partition filter and one was supplied explicitly: only rows matching "
            "%s will be processed and validated",
            table_id, supplied,
        )
        return supplied

    column, ingestion_time = partition_column(table)
    ref = "_PARTITIONTIME" if ingestion_time else (q(column) if column else None)
    if not ref:
        raise BackupError(
            f"{table_id} has require_partition_filter=true but no partition column could be "
            'read from its metadata. Supply --partition-filter "<predicate>" manually.'
        )

    # Count rows in the __NULL__ partition. `IS NULL` is itself a valid partition
    # filter, and it prunes to that single partition, so this is cheap.
    null_rows = runner.run(
        f"SELECT COUNT(*) AS n\nFROM {bq_ref(table_id)}\nWHERE {ref} IS NULL",
        "null_partition_probe",
    )[0][0]["n"]

    if null_rows:
        widened = f"({ref} IS NOT NULL OR {ref} IS NULL)"
        LOG.warning(
            "%s holds %s row(s) in the __NULL__ partition; widening the filter to %s so they are "
            "not silently excluded",
            table_id, f"{null_rows:,}", widened,
        )
        return widened

    LOG.info(
        "%s has require_partition_filter=true; using %s IS NOT NULL to cover every partition",
        table_id, ref,
    )
    return f"{ref} IS NOT NULL"


def collect_policy_tags(
    fields: Sequence[bigquery.SchemaField], prefix: str = ""
) -> dict[str, tuple[str, ...]]:
    """Recursively map column path -> sorted policy tag resource names."""
    out: dict[str, tuple[str, ...]] = {}
    for f in fields:
        path = f"{prefix}{f.name}"
        tags = getattr(f, "policy_tags", None)
        names = tuple(sorted(tags.names)) if tags and tags.names else ()
        if names:
            out[path] = names
        if f.fields:
            out.update(collect_policy_tags(f.fields, prefix=f"{path}."))
    return out


def assert_unique_column_names(fields: Sequence[bigquery.SchemaField], where: str) -> None:
    """BigQuery resolves names case-insensitively; near-duplicates break matching."""
    seen: dict[str, str] = {}
    for f in fields:
        key = f.name.lower()
        if key in seen:
            raise BackupError(f"{where} has columns differing only by case: {seen[key]} / {f.name}")
        seen[key] = f.name


# --------------------------------------------------------------------------- #
# Column statistics
# --------------------------------------------------------------------------- #

INT_TYPES = {"INTEGER", "INT64"}
FLOAT_TYPES = {"FLOAT", "FLOAT64"}
EXACT_NUMERIC_TYPES = {"NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"}
TEMPORAL_TYPES = {"DATE", "DATETIME", "TIMESTAMP", "TIME"}
STRINGY_TYPES = {"STRING", "BYTES"}
BOOL_TYPES = {"BOOL", "BOOLEAN"}
VALUE_SUFFIXES = {"min", "max"}  # these can leak raw column values


def classify(f: bigquery.SchemaField) -> str:
    if (f.mode or "").upper() == "REPEATED":
        return "repeated"
    t = f.field_type.upper()
    if t in ("RECORD", "STRUCT"):
        return "struct"
    if t in INT_TYPES:
        return "int"
    if t in FLOAT_TYPES:
        return "float"
    if t in EXACT_NUMERIC_TYPES:
        return "exact_numeric"
    if t in TEMPORAL_TYPES:
        return "temporal"
    if t in STRINGY_TYPES:
        return "string"
    if t in BOOL_TYPES:
        return "bool"
    return "opaque"


def build_stats_query(
    scan: Scan, fields: Sequence[bigquery.SchemaField], cfg: ValidationConfig
) -> tuple[str, dict[str, str], list[str]]:
    alias_kind: dict[str, str] = {"row_count": "exact"}
    notes: list[str] = []
    select = ["COUNT(*) AS row_count"]
    t = scan.alias

    for i, f in enumerate(fields):
        c = col_ref(t, f.name)
        kind = classify(f)
        p = f"c{i}"

        def add(suffix: str, expr: str, compare: str = "exact") -> None:
            alias = f"{p}_{suffix}"
            select.append(f"{expr} AS {alias}")
            alias_kind[alias] = compare

        if kind == "repeated":
            add("empty", f"COUNTIF(ARRAY_LENGTH({c}) = 0)")
            add("elems", f"CAST(SUM(ARRAY_LENGTH({c})) AS STRING)")
            add("xor", f"BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING({c})))")
            add("fsum", f"CAST(SUM(CAST(FARM_FINGERPRINT(TO_JSON_STRING({c})) AS BIGNUMERIC)) AS STRING)")
            notes.append(f"{f.name}: REPEATED -> element count + order-independent content hash")
            continue

        if kind in ("struct", "opaque"):
            add("nulls", f"COUNTIF({c} IS NULL)")
            add("xor", f"BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING({c})))")
            add("fsum", f"CAST(SUM(CAST(FARM_FINGERPRINT(TO_JSON_STRING({c})) AS BIGNUMERIC)) AS STRING)")
            notes.append(f"{f.name}: {f.field_type} -> MIN/MAX/SUM not applicable, deterministic hash used")
            continue

        add("nulls", f"COUNTIF({c} IS NULL)")

        if kind == "bool":
            add("true", f"COUNTIF({c} IS TRUE)")
            add("false", f"COUNTIF({c} IS FALSE)")
            notes.append(f"{f.name}: BOOL -> COUNT/NULL/TRUE/FALSE")
            continue

        if cfg.distinct_count:
            add("distinct", f"APPROX_COUNT_DISTINCT({c})" if cfg.approx_distinct else f"COUNT(DISTINCT {c})")

        if kind == "string":
            continue

        add("min", f"CAST(MIN({c}) AS STRING)")
        add("max", f"CAST(MAX({c}) AS STRING)")

        if kind in ("int", "exact_numeric"):
            add("sum", f"CAST(SUM(CAST({c} AS BIGNUMERIC)) AS STRING)")
        elif kind == "float":
            add("sum", f"SUM({c})", compare="float")
            notes.append(f"{f.name}: FLOAT64 -> SUM compared with tolerance {cfg.float_tolerance}")

    sql = "SELECT\n  " + ",\n  ".join(select) + scan.tail()
    return sql, alias_kind, notes


def compare_stats(
    left: dict[str, Any],
    right: dict[str, Any],
    alias_kind: dict[str, str],
    fields: Sequence[bigquery.SchemaField],
    tolerance: float,
    sensitive: set[str],
) -> list[str]:
    """Return mismatches. Values of policy-tagged columns are redacted."""
    col_of = {f"c{i}": f.name for i, f in enumerate(fields)}
    mismatches: list[str] = []
    for alias, kind in alias_kind.items():
        # A missing alias on either side must never be read as "equal".
        if alias not in left or alias not in right:
            mismatches.append(
                f"{alias}: aggregate missing from one side (left={alias in left}, right={alias in right})"
            )
            continue
        a, b = normalise(left[alias]), normalise(right[alias])
        bad = False
        if kind == "float":
            if (a is None) != (b is None):
                bad = True
            elif a is not None:
                delta = abs(float(a) - float(b))
                scale = max(1.0, abs(float(a)), abs(float(b)))
                bad = delta > tolerance and delta / scale > tolerance
        else:
            bad = a != b
        if bad:
            prefix, _, suffix = alias.partition("_")
            col = col_of.get(prefix, prefix)
            label = "row_count" if alias == "row_count" else f"{col}.{suffix}"
            if col in sensitive and suffix in VALUE_SUFFIXES:
                mismatches.append(f"{label}: values differ (redacted: column carries a policy tag)")
            else:
                mismatches.append(f"{label}: {a!r} vs {b!r}")
    return mismatches


# --------------------------------------------------------------------------- #
# Deterministic order-independent hash
# --------------------------------------------------------------------------- #


def build_hash_query(scan: Scan, column_names: Sequence[str]) -> str:
    """Order-independent content fingerprint.

    Row order is undefined in BigQuery, so row fingerprints are combined with
    commutative aggregates only (COUNT / BIT_XOR / exact BIGNUMERIC SUM).
    Four independently salted 64-bit fingerprints make accidental collision
    negligible. Columns are serialised name-sorted so both sides canonicalise
    identically regardless of ordinal position.
    """
    cols = ", ".join(col_ref(scan.alias, n) for n in sorted(column_names))
    inner = f"SELECT TO_JSON_STRING(STRUCT({cols})) AS rj{scan.tail()}"
    parts = ["COUNT(*) AS n"]
    for i, salt in enumerate(HASH_SALTS):
        expr = "rj" if not salt else f"CONCAT('{salt}', rj)"
        parts.append(f"BIT_XOR(FARM_FINGERPRINT({expr})) AS x{i}")
    parts.append("CAST(SUM(CAST(FARM_FINGERPRINT(rj) AS BIGNUMERIC)) AS STRING) AS s0")
    return "SELECT\n  " + ",\n  ".join(parts) + f"\nFROM (\n  {inner}\n)"


def digest(row: dict[str, Any]) -> str:
    payload = json.dumps({k: normalise(v) for k, v in row.items()}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


# --------------------------------------------------------------------------- #
# Metadata access
# --------------------------------------------------------------------------- #


def get_table(client: bigquery.Client, table_id: str) -> bigquery.Table:
    try:
        return client.get_table(table_id)
    except gexc.NotFound as exc:
        raise BackupError(f"Not found: {table_id}") from exc
    except gexc.Forbidden as exc:
        raise BackupError(f"Access denied for {table_id}: {exc}") from exc


def assert_dataset(client: bigquery.Client, project: str, dataset: str) -> bigquery.Dataset:
    try:
        return client.get_dataset(f"{project}.{dataset}")
    except gexc.NotFound as exc:
        raise BackupError(f"Dataset not found: {project}.{dataset}") from exc
    except gexc.Forbidden as exc:
        raise BackupError(f"Access denied for dataset {project}.{dataset}: {exc}") from exc


def validate_source(
    runner: QueryRunner,
    client: bigquery.Client,
    table_id: str,
    partition_filter: str | None,
    allow_streaming: bool,
    report: Report,
) -> tuple[bigquery.Table, str | None]:
    table = get_table(client, table_id)

    if table.table_type != "TABLE":
        raise BackupError(
            f"{table_id} is a {table.table_type}, not a base TABLE. "
            "Views and external tables cannot be snapshotted."
        )
    assert_unique_column_names(table.schema, table_id)

    # Snapshots do not include the streaming buffer -> silent data loss risk.
    buf = getattr(table, "streaming_buffer", None)
    if buf and getattr(buf, "estimated_rows", 0):
        msg = (
            f"{table_id} has ~{buf.estimated_rows:,} rows in the streaming buffer. "
            "A table snapshot does NOT capture the streaming buffer, so those rows would "
            "be lost. Stop the writers and wait for the buffer to flush (up to ~90 minutes)."
        )
        if not allow_streaming:
            raise BackupError(msg + " Override with --allow-streaming-buffer only if you accept the loss.")
        LOG.warning("%s (overridden)", msg)

    partition_filter = resolve_partition_filter(runner, table, partition_filter, table_id)

    scan = Scan(table_id, partition_filter=partition_filter)
    runner.run(f"SELECT 1{scan.tail()}\nLIMIT 0", "source_probe")

    part = describe_partitioning(table)

    tags = collect_policy_tags(table.schema)
    report.facts["original"] = {
        "table_id": table_id,
        "num_rows_metadata": table.num_rows,
        "columns": [f.name for f in table.schema],
        "types": {f.name: standard_sql_type(f) for f in table.schema},
        "modes": {f.name: (f.mode or "NULLABLE") for f in table.schema},
        "partitioning": part,
        "clustering": list(table.clustering_fields or []),
        "location": table.location,
        "last_modified": table.modified.isoformat() if table.modified else None,
        "policy_tagged_columns": {k: list(v) for k, v in tags.items()},
    }
    LOG.info(
        "source ok table=%s columns=%d policy_tagged=%d partitioning=%s",
        table_id, len(table.schema), len(tags), part,
    )
    if tags:
        LOG.info("policy tags present on: %s", ", ".join(sorted(tags)))
    report.facts["original"]["partition_filter"] = partition_filter
    return table, partition_filter


def make_snapshot_id(table: str, stamp: str, suffix: str = "_vw") -> str:
    """<table>__snapshot__YYYYMMDD_HHMMSS<suffix>, e.g. customer__snapshot__20260821_010530_vw

    One `stamp` is generated per run, so every table in a batch shares the same
    timestamp and the whole batch is identifiable as one migration.
    """
    return f"{table}__snapshot__{stamp}{suffix}"


def create_snapshot(
    runner: QueryRunner,
    client: bigquery.Client,
    source_id: str,
    snapshot_id: str,
    retention_days: int,
) -> bigquery.Table:
    """Create a genuine BigQuery table snapshot.

    Uses CREATE SNAPSHOT TABLE ... CLONE, a zero-copy metadata-only snapshot that
    preserves schema, column descriptions, policy tags, partitioning and
    clustering. It is NOT a CTAS or a temporary table, and it does not read or
    duplicate the underlying storage.
    """
    try:
        client.get_table(snapshot_id)
        raise BackupError(f"Snapshot already exists, refusing to overwrite: {snapshot_id}")
    except gexc.NotFound:
        pass

    expiry = datetime.now(timezone.utc) + timedelta(days=retention_days)
    sql = (
        f"CREATE SNAPSHOT TABLE {bq_ref(snapshot_id)}\n"
        f"CLONE {bq_ref(source_id)}\n"
        f'OPTIONS (expiration_timestamp = TIMESTAMP "{expiry.strftime("%Y-%m-%d %H:%M:%S")} UTC")'
    )
    runner.run(sql, "create_snapshot")

    snapshot = get_table(client, snapshot_id)
    # Guardrail: prove this is a real snapshot, not a copy or a CTAS table.
    if snapshot.table_type != "SNAPSHOT" or not getattr(snapshot, "snapshot_definition", None):
        raise BackupError(
            f"{snapshot_id} was created as table_type={snapshot.table_type}, not SNAPSHOT. "
            "Aborting rather than proceeding with a non-snapshot copy."
        )
    LOG.info(
        "snapshot created id=%s type=%s base=%s snapshot_time=%s expires=%s",
        snapshot_id,
        snapshot.table_type,
        snapshot.snapshot_definition.base_table_reference.path,
        snapshot.snapshot_definition.snapshot_time,
        snapshot.expires,
    )
    return snapshot


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #

LINE = "=" * 50
THIN = "-" * 50


def render(report: Report, cfg: ValidationConfig, notes: list[str]) -> str:
    def row(label: str, value: Any) -> str:
        return f"{label:<29}{value}"

    def num(v: Any) -> str:
        return f"{v:,}" if isinstance(v, int) else "n/a"

    o = report.facts.get("original", {})
    s = report.facts.get("snapshot", {})
    tagged = o.get("policy_tagged_columns", {})

    out = [
        LINE, "BIGQUERY SNAPSHOT VALIDATION", LINE, "",
        "Original:", report.original, "",
        "Snapshot:", report.snapshot, "",
        row("Snapshot type", s.get("table_type", "n/a")),
        row("Snapshot time", s.get("snapshot_time", "n/a")),
        row("Expires", s.get("expires", "n/a")),
        "", THIN, "SCHEMA", THIN, "",
        row("Schema match", report.status("schema_match")),
        "", THIN, "PARTITIONING", THIN, "",
        row("Source spec", o.get("partitioning") or "not partitioned"),
        row("Clustering", ", ".join(o.get("clustering", [])) or "none"),
        row("Partition filter", o.get("partition_filter") or "none required"),
        "", THIN, "PII / POLICY TAGS", THIN, "",
        row("Tagged columns", len(tagged)),
        row("Tags preserved", report.status("policy_tags_preserved")),
    ]
    for name in sorted(tagged):
        out.append(row(f"  {name}", "tagged"))

    out += [
        "", THIN, "ROW COUNT", THIN, "",
        row("Original (as of snapshot)", num(o.get("row_count"))),
        row("Snapshot", num(s.get("row_count"))),
        row("Result", report.status("row_count")),
        "", THIN, "DATA STATISTICS", THIN, "",
        row("Column statistics", report.status("column_statistics")),
        "", THIN, "DATA HASH", THIN, "",
        row("Original hash", report.facts.get("original_hash", "skipped")),
        row("Snapshot hash", report.facts.get("snapshot_hash", "skipped")),
        row("Result", report.status("data_hash")),
        "", THIN, "VALIDATIONS EXECUTED", THIN, "",
        "  " + "  ".join(cfg.executed()),
        row("  bytes processed", num(report.facts.get("bytes_processed"))),
    ]

    if notes:
        out += ["", THIN, "NOTES", THIN, ""] + [f"  - {n}" for n in notes]

    failures = [c for c in report.checks if not c.passed]
    if failures:
        out += ["", THIN, "FAILURE DETAIL", THIN, ""] + [f"  {c.name}: {c.detail}" for c in failures]

    out += ["", LINE, f"FINAL RESULT: {'PASS' if report.ok else 'FAIL'}", ""]
    out.append(
        "Snapshot is safe to use for migration."
        if report.ok
        else "Snapshot must NOT be used. Do not run Terraform until this is resolved."
    )
    out.append(LINE)
    return "\n".join(out)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# CLI / multi-table orchestration
# --------------------------------------------------------------------------- #


def split_table_ref(ref: str) -> tuple[str, str, str]:
    """Parse 'project.dataset.table' into validated parts."""
    parts = ref.strip().split(".")
    if len(parts) != 3:
        raise BackupError(
            f"Invalid table reference {ref!r}: expected project_id.dataset_id.table_name"
        )
    project, dataset, table = parts
    return valid_project(project), valid_ident(dataset, "dataset"), valid_table_name(table)


def parse_per_table(values: Sequence[str], kind: str) -> tuple[str | None, dict[str, str]]:
    """Parse repeatable 'TABLE=VALUE' options, with a bare value as the default.

    --primary-key customer_id                      -> applies to every table
    --primary-key customer=customer_id             -> applies to that table only
    """
    default: str | None = None
    per_table: dict[str, str] = {}
    for item in values or []:
        name, sep, value = item.partition("=")
        # A bare expression may legitimately contain '=' (e.g. "dt = '2024-01-01'"),
        # so only treat it as scoped when the left side is a plain table name.
        if sep and TABLE_RE.match(name.strip()) and " " not in name.strip():
            per_table[name.strip().lower()] = value.strip()
        elif default is None:
            default = item.strip()
        else:
            raise BackupError(f"More than one unscoped {kind} supplied: {default!r} and {item!r}")
    return default, per_table


def resolve_for(table: str, default: str | None, per_table: dict[str, str]) -> str | None:
    return per_table.get(table.lower(), default)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Snapshot one or more BigQuery tables and prove each snapshot is an exact copy.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=(
            "Snapshots are created in the same dataset as each source table, named\n"
            "<table>__snapshot__YYYYMMDD_HHMMSS_vw\n\n"
            "Examples:\n"
            "  # several tables at once\n"
            "  %(prog)s --tables proj.ds.customer proj.ds.orders proj.ds.payments \\\n"
            "           --retention-days 7\n\n"
            "  # optional per-table business key (adds duplicate + sample checks)\n"
            "  %(prog)s --tables proj.ds.customer proj.ds.orders \\\n"
            "           --primary-key customer=customer_id\n"
        ),
    )
    p.add_argument(
        "--tables",
        nargs="+",
        metavar="PROJECT.DATASET.TABLE",
        help="One or more fully qualified source tables",
    )
    # Legacy single-table form, still supported.
    p.add_argument("--project")
    p.add_argument("--dataset")
    p.add_argument("--table")

    p.add_argument("--snapshot-suffix", default="_vw", help="Appended after the timestamp")
    p.add_argument("--retention-days", type=int, default=7)
    p.add_argument(
        "--primary-key",
        action="append",
        default=[],
        metavar="[TABLE=]COLS",
        help="Business key; repeatable, optionally scoped to a table",
    )
    p.add_argument(
        "--partition-filter",
        action="append",
        default=[],
        metavar="[TABLE=]PREDICATE",
        help="Scan predicate; repeatable, optionally scoped to a table",
    )
    p.add_argument("--location")
    p.add_argument("--billing-project")
    p.add_argument("--max-bytes-billed", type=int, help="Hard cost cap per query, in bytes")
    p.add_argument("--query-timeout", type=float, default=3600.0)
    p.add_argument("--float-tolerance", type=float, default=1e-9)
    p.add_argument("--approx-distinct", action="store_true")
    p.add_argument("--allow-streaming-buffer", action="store_true")
    p.add_argument("--fail-fast", action="store_true", help="Stop at the first table that fails")
    p.add_argument("--no-statistics", action="store_true")
    p.add_argument("--no-distinct-count", action="store_true")
    p.add_argument("--no-hash-check", action="store_true")
    p.add_argument("--no-policy-tag-check", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")

    args = p.parse_args(argv)
    if not 1 <= args.retention_days <= 3650:
        p.error("--retention-days must be between 1 and 3650")
    if not args.tables and not (args.project and args.dataset and args.table):
        p.error("supply --tables PROJECT.DATASET.TABLE [...] (or the legacy --project/--dataset/--table)")
    if args.tables and (args.project or args.dataset or args.table):
        p.error("--tables cannot be combined with --project/--dataset/--table")
    if args.snapshot_suffix and not re.match(r"^[A-Za-z0-9_-]*$", args.snapshot_suffix):
        p.error("--snapshot-suffix may contain only letters, digits, underscore and dash")
    return args


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        stream=sys.stderr,
    )


# --------------------------------------------------------------------------- #
# One table
# --------------------------------------------------------------------------- #


def process_table(  # noqa: C901 - linear pipeline, kept together deliberately
    client: bigquery.Client,
    source_ref: tuple[str, str, str],
    args: argparse.Namespace,
    cfg: ValidationConfig,
    stamp: str,
    primary_key: str | None,
    partition_filter: str | None,
) -> Report:
    """Snapshot and validate a single table. Never raises; failures land in the Report."""
    report = Report()
    notes: list[str] = []
    snapshot_id = ""
    project, dataset, table = source_ref

    runner = QueryRunner(client, args.max_bytes_billed, args.query_timeout, JOB_LABELS)

    try:
        pfilter = valid_expression(partition_filter, "--partition-filter") if partition_filter else None

        source_id = fq(project, dataset, table)
        # The snapshot is created alongside the source table, in the same dataset.
        snap_project, snap_dataset = project, dataset
        report.original = source_id

        # ---- Step 1: original ---------------------------------------------- #
        assert_dataset(client, project, dataset)
        source, pfilter = validate_source(
            runner, client, source_id, pfilter, args.allow_streaming_buffer, report
        )
        if pfilter:
            notes.append(f"Partition filter in effect: {pfilter}")

        snap_ds = assert_dataset(client, snap_project, snap_dataset)
        LOG.info(
            "snapshot will be created in the source dataset %s.%s - make sure Terraform does not "
            "manage that dataset with delete_contents_on_destroy",
            snap_project, snap_dataset,
        )
        if snap_ds.default_table_expiration_ms:
            ds_days = snap_ds.default_table_expiration_ms / 86_400_000
            if ds_days < args.retention_days:
                raise BackupError(
                    f"Snapshot dataset {snap_project}.{snap_dataset} has a default table expiration "
                    f"of {ds_days:.1f} days, shorter than --retention-days {args.retention_days}. "
                    "The snapshot could vanish mid-migration."
                )

        pk = [
            valid_ident(c.strip(), "primary key column")
            for c in (primary_key or "").split(",")
            if c.strip()
        ]
        if pk:
            names = {f.name.lower() for f in source.schema}
            missing = [c for c in pk if c.lower() not in names]
            if missing:
                raise BackupError(f"primary key columns not in {source_id}: {', '.join(missing)}")
            report.facts["primary_key"] = pk

        original_tags = collect_policy_tags(source.schema)
        sensitive = {p.split(".", 1)[0] for p in original_tags}

        # ---- Steps 2 + 3: snapshot ----------------------------------------- #
        snapshot_id = (
            f"{snap_project}.{snap_dataset}."
            f"{make_snapshot_id(table, stamp, args.snapshot_suffix)}"
        )
        report.snapshot = snapshot_id
        snapshot = create_snapshot(runner, client, source_id, snapshot_id, args.retention_days)

        snap_time = snapshot.snapshot_definition.snapshot_time
        report.facts["snapshot"] = {
            "table_id": snapshot_id,
            "table_type": snapshot.table_type,
            "snapshot_time": snap_time.isoformat() if snap_time else None,
            "base_table": snapshot.snapshot_definition.base_table_reference.path,
            "expires": snapshot.expires.isoformat() if snapshot.expires else None,
        }

        # All original-side reads pin to the snapshot instant, so concurrent
        # writers cannot cause a false PASS or a spurious FAIL.
        src_scan = Scan(source_id, as_of=snap_time, partition_filter=pfilter)
        snap_scan = Scan(snapshot_id, partition_filter=pfilter)
        notes.append(f"Original read via time travel FOR SYSTEM_TIME AS OF {snap_time}")

        runner.run(f"SELECT 1{snap_scan.tail()}\nLIMIT 0", "snapshot_probe")

        # ---- Step 4: schema ------------------------------------------------ #
        if cfg.schema:
            diffs = diff_schemas(source.schema, snapshot.schema)
            report.add("schema_match", not diffs, "identical" if not diffs else "; ".join(diffs[:10]))
            if diffs:
                raise ValidationFailure("snapshot schema differs from original")

        # ---- Policy tags (PII) --------------------------------------------- #
        if cfg.policy_tag_check:
            snap_tags = collect_policy_tags(snapshot.schema)
            if not original_tags:
                report.add("policy_tags_preserved", True, "source has no policy tags", critical=False)
            else:
                lost = {k: v for k, v in original_tags.items() if snap_tags.get(k) != v}
                extra = {k for k in snap_tags if k not in original_tags}
                detail = f"{len(original_tags)} tagged columns carried into the snapshot"
                if lost or extra:
                    detail = f"tags lost or altered on: {', '.join(sorted(lost))}" + (
                        f"; unexpected tags on: {', '.join(sorted(extra))}" if extra else ""
                    )
                report.add("policy_tags_preserved", not lost and not extra, detail)
                if lost:
                    raise ValidationFailure(
                        "PII policy tags did not survive into the snapshot - the snapshot would "
                        "expose tagged data to principals who cannot see the original."
                    )

        # ---- Steps 5-7: counts + statistics -------------------------------- #
        if cfg.statistics:
            sql_src, alias_kind, stat_notes = build_stats_query(src_scan, source.schema, cfg)
            sql_snap, _, _ = build_stats_query(snap_scan, snapshot.schema, cfg)
            notes.extend(stat_notes)
        else:
            alias_kind = {"row_count": "exact"}
            sql_src = f"SELECT COUNT(*) AS row_count{src_scan.tail()}"
            sql_snap = f"SELECT COUNT(*) AS row_count{snap_scan.tail()}"

        src_stats = runner.run(sql_src, "stats_original")[0][0]
        snap_stats = runner.run(sql_snap, "stats_snapshot")[0][0]
        report.facts["original"]["row_count"] = src_stats["row_count"]
        report.facts["snapshot"]["row_count"] = snap_stats["row_count"]

        if cfg.row_count:
            report.add(
                "row_count",
                src_stats["row_count"] == snap_stats["row_count"],
                f"{src_stats['row_count']:,} vs {snap_stats['row_count']:,}",
            )

        if cfg.statistics:
            mismatches = compare_stats(
                src_stats, snap_stats, alias_kind, source.schema, cfg.float_tolerance, sensitive
            )
            report.add(
                "column_statistics",
                not mismatches,
                f"{len(alias_kind)} aggregates compared" if not mismatches else "; ".join(mismatches[:10]),
            )

        # ---- Step 8: deterministic hash ------------------------------------ #
        if cfg.hash_check:
            names = [f.name for f in source.schema]
            h1 = digest(runner.run(build_hash_query(src_scan, names), "hash_original")[0][0])
            h2 = digest(runner.run(build_hash_query(snap_scan, names), "hash_snapshot")[0][0])
            report.facts["original_hash"], report.facts["snapshot_hash"] = h1, h2
            report.add("data_hash", h1 == h2, f"{h1} vs {h2}")

    except BackupError as exc:
        LOG.error("[%s] %s", f"{project}.{dataset}.{table}", exc)
        report.add("fatal", False, str(exc))
    except gexc.GoogleAPIError as exc:  # pragma: no cover
        LOG.exception("[%s] BigQuery API error", f"{project}.{dataset}.{table}")
        report.add("fatal", False, f"BigQuery API error: {exc}")

    report.facts["bytes_processed"] = runner.total_bytes
    report.facts["notes"] = notes
    print(render(report, cfg, notes))

    if not report.ok and snapshot_id:
        LOG.error(
            "[%s] validation failed - snapshot %s retained for investigation, NOT safe to migrate from",
            f"{project}.{dataset}.{table}", snapshot_id,
        )
    return report


# --------------------------------------------------------------------------- #
# Batch summary
# --------------------------------------------------------------------------- #


def render_summary(results: list[tuple[str, Report]], skipped: list[str]) -> str:
    out = ["", LINE, "BATCH SUMMARY", LINE, ""]
    width = max([len(src) for src, _ in results] + [len(s) for s in skipped] + [12])
    for src, rep in results:
        rows = rep.facts.get("snapshot", {}).get("row_count")
        suffix = f"  rows={rows:,}" if isinstance(rows, int) else ""
        out.append(f"{'PASS' if rep.ok else 'FAIL'}  {src:<{width}}{suffix}")
        if rep.snapshot:
            out.append(f"      -> {rep.snapshot}")
        if not rep.ok:
            for c in rep.checks:
                if not c.passed:
                    out.append(f"      !! {c.name}: {c.detail[:160]}")
    for src in skipped:
        out.append(f"SKIP  {src:<{width}}  (--fail-fast: an earlier table failed)")

    passed = sum(1 for _, r in results if r.ok)
    out += [
        "",
        f"Tables requested   {len(results) + len(skipped)}",
        f"Passed             {passed}",
        f"Failed             {len(results) - passed}",
        f"Skipped            {len(skipped)}",
        "",
        LINE,
        f"BATCH RESULT: {'PASS' if passed == len(results) and not skipped else 'FAIL'}",
        LINE,
    ]
    return "\n".join(out)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)

    try:
        if args.tables:
            refs = [split_table_ref(t) for t in args.tables]
        else:
            refs = [(valid_project(args.project), valid_ident(args.dataset, "dataset"),
                     valid_table_name(args.table))]

        seen: set[str] = set()
        for r in refs:
            key = ".".join(r).lower()
            if key in seen:
                raise BackupError(f"Duplicate table in --tables: {'.'.join(r)}")
            seen.add(key)

        pk_default, pk_map = parse_per_table(args.primary_key, "--primary-key")
        pf_default, pf_map = parse_per_table(args.partition_filter, "--partition-filter")
        for scope in (set(pk_map) | set(pf_map)) - {r[2].lower() for r in refs}:
            raise BackupError(f"Option scoped to table {scope!r}, which is not in --tables")

        cfg = ValidationConfig(
            statistics=not args.no_statistics,
            distinct_count=not args.no_distinct_count,
            hash_check=not args.no_hash_check,
            policy_tag_check=not args.no_policy_tag_check,
            approx_distinct=args.approx_distinct,
            float_tolerance=args.float_tolerance,
        )
    except BackupError as exc:
        LOG.error("%s", exc)
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return 1

    if args.approx_distinct:
        LOG.warning("APPROX_COUNT_DISTINCT enabled - this weakens the equality proof")

    # One timestamp for the whole batch, so all snapshots share a name stamp.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOG.info("batch start tables=%d stamp=%s suffix=%s", len(refs), stamp, args.snapshot_suffix)

    results: list[tuple[str, Report]] = []
    skipped: list[str] = []

    for i, ref in enumerate(refs, start=1):
        src = ".".join(ref)
        LOG.info("[%d/%d] processing %s", i, len(refs), src)
        # A per-table client keeps a bad location on one table from poisoning the rest.
        client = bigquery.Client(project=args.billing_project or ref[0], location=args.location)
        report = process_table(
            client, ref, args, cfg, stamp,
            resolve_for(ref[2], pk_default, pk_map),
            resolve_for(ref[2], pf_default, pf_map),
        )
        results.append((src, report))
        if not report.ok and args.fail_fast:
            skipped = [".".join(r) for r in refs[i:]]
            LOG.error("--fail-fast: stopping, %d table(s) not attempted", len(skipped))
            break

    print(render_summary(results, skipped))

    all_ok = bool(results) and all(r.ok for _, r in results) and not skipped
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
