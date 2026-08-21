#!/usr/bin/env python3
"""restore_and_validate.py  (hardened)

After Terraform has recreated the production table, restore data from a genuine
BigQuery table snapshot into the new table and PROVE that nothing was lost,
altered or exposed.

This script NEVER creates, alters, drops or truncates the target table, and it
never deletes the snapshot. It issues exactly one write: INSERT ... SELECT.

Security / safety guardrails
----------------------------
  * every identifier is regex-validated; operator SQL fragments are screened
    for statement chaining, comments and backticks
  * the snapshot is asserted to be table_type == SNAPSHOT (a plain table copy
    requires an explicit override)
  * POLICY TAG PARITY: if a snapshot column carries a PII policy tag and the
    Terraform-recreated column does not, the restore is BLOCKED - otherwise the
    migration would silently strip access control from PII
  * COLUMN MASKING PROBE: if the caller only has masked read access to a tagged
    column, INSERT ... SELECT would write masked values into production. The
    restore is blocked unless fine-grained access is demonstrated
  * concurrent-writer detection on the target before and after the insert
  * num_dml_affected_rows is reconciled against the snapshot row count
  * query cache disabled, maximum_bytes_billed cap, audit labels on every job
  * values of policy-tagged columns are redacted from logs and reports

Exit code 0 -> restore executed and every configured validation passed
Exit code 1 -> blocked before the insert, or the insert ran but validation
               FAILED. The snapshot is always retained.

Usage
-----
python restore_and_validate.py \
    --project PROJECT_ID \
    --dataset DATASET \
    --table TABLE_NAME \
    --snapshot-project PROJECT_ID \
    --snapshot-dataset SNAPSHOT_DATASET \
    --snapshot-table SNAPSHOT_TABLE \
    [--primary-key customer_id] \
    [--allow-non-empty-target] \
    [--default-expression city="'UNKNOWN'"] \
    [--sample-size 10000] [--dry-run] \
    [--max-bytes-billed 1099511627776] \
    [--json-report restore_report.json]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Sequence

from google.api_core import exceptions as gexc
from google.cloud import bigquery

__version__ = "2026.08.21.3"
LOG = logging.getLogger("bq.restore")

HASH_SALTS = ("", "s1|", "s2|", "s3|")
JOB_LABELS = {"tool": "bq-terraform-migration", "phase": "restore"}

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$")
PROJECT_RE = re.compile(r"^(?:[a-z][a-z0-9.-]{0,62}:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$")
# Table names may contain dashes; dataset and column names may not.
TABLE_RE = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")
FORBIDDEN_IN_EXPR = (";", "--", "/*", "*/", "\x00")


class RestoreError(RuntimeError):
    """Fatal condition. Stop and return a non-zero exit code."""


class RestoreBlocked(RestoreError):
    """Pre-flight check failed. Nothing was written."""


class ValidationFailure(RestoreError):
    """A configured post-restore validation did not pass."""


# --------------------------------------------------------------------------- #
# Identifier / expression validation
# --------------------------------------------------------------------------- #


def valid_ident(name: str, kind: str) -> str:
    if not IDENT_RE.match(name or ""):
        raise RestoreError(
            f"Invalid {kind} {name!r}: must match [A-Za-z_][A-Za-z0-9_]* "
            "(no quotes, dots, spaces or newlines)"
        )
    return name


def valid_table_name(name: str, kind: str = "table") -> str:
    """Table names allow dashes; still no quotes, dots, spaces or newlines."""
    if not TABLE_RE.match(name or ""):
        raise RestoreError(
            f"Invalid {kind} {name!r}: only letters, digits, underscore and dash are allowed"
        )
    return name


def valid_project(name: str) -> str:
    if not PROJECT_RE.match(name or ""):
        raise RestoreError(f"Invalid project id {name!r}")
    return name


def valid_expression(expr: str, kind: str) -> str:
    """Operator-supplied SQL. Blocks chaining, comments and identifier escapes."""
    if not expr or not expr.strip():
        raise RestoreError(f"Empty {kind}")
    for bad in FORBIDDEN_IN_EXPR:
        if bad in expr:
            raise RestoreError(f"{kind} contains forbidden sequence {bad!r}: {expr!r}")
    if expr.count("(") != expr.count(")"):
        raise RestoreError(f"{kind} has unbalanced parentheses: {expr!r}")
    if "`" in expr:
        raise RestoreError(f"{kind} must not contain backticks: {expr!r}")
    if re.search(r"\b(select|insert|update|delete|merge|drop|create|alter|truncate)\b", expr, re.I):
        raise RestoreError(
            f"{kind} looks like a statement or subquery, which is not allowed here: {expr!r}"
        )
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


@dataclass
class ValidationConfig:
    schema: bool = True
    row_count: bool = True
    statistics: bool = True
    distinct_count: bool = True
    duplicate_check: bool = True
    new_column_check: bool = True
    policy_tag_check: bool = True
    hash_check: bool = True
    sample_check: bool = True
    sample_size: int = 10000
    approx_distinct: bool = False
    float_tolerance: float = 1e-9

    def merge(self, blob: dict[str, Any]) -> None:
        for key, value in blob.items():
            if not hasattr(self, key):
                raise RestoreError(f"Unknown validation key in config: {key}")
            setattr(self, key, value)

    def executed(self) -> list[str]:
        keys = (
            "schema", "row_count", "statistics", "distinct_count", "duplicate_check",
            "new_column_check", "policy_tag_check", "hash_check", "sample_check",
        )
        return [f"{k}={'ON' if getattr(self, k) else 'OFF'}" for k in keys]


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""
    critical: bool = True


@dataclass
class Report:
    snapshot: str = ""
    target: str = ""
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


class QueryRunner:
    """Cost caps, cache disabled, audit labels on every job."""

    def __init__(self, client, max_bytes_billed, timeout_s, labels) -> None:
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
            labels={**self.labels, "step": re.sub(r"[^a-z0-9_-]", "_", label.lower())[:63]},
        )
        # Only set the cap when one was given: the client library stringifies this
        # property unconditionally, so passing None sends the literal "None" and
        # BigQuery rejects the job with an INT64 type error.
        if self.max_bytes_billed is not None:
            cfg.maximum_bytes_billed = int(self.max_bytes_billed)

        # Belt and braces: some client versions stringify this property even when
        # it is None, which BigQuery rejects as an invalid INT64. Drop anything
        # that is not a numeric string before the job is submitted.
        _qcfg = cfg._properties.setdefault("query", {})
        if not str(_qcfg.get("maximumBytesBilled", "")).isdigit():
            _qcfg.pop("maximumBytesBilled", None)
        try:
            job = self.client.query(sql, job_config=cfg)
            if dry_run:
                LOG.info("dry_run=%s bytes_estimated=%s", label, job.total_bytes_processed)
                return [], job
            rows = [dict(r) for r in job.result(timeout=self.timeout_s)]
        except gexc.Forbidden as exc:
            raise RestoreError(
                f"Access denied running '{label}'. For policy-tagged columns the service account "
                "needs roles/datacatalog.categoryFineGrainedReader on the taxonomy. "
                f"Underlying error: {exc}"
            ) from exc
        except gexc.BadRequest as exc:
            if "bytes billed" in str(exc).lower():
                raise RestoreError(
                    f"Query '{label}' would exceed --max-bytes-billed "
                    f"({self.max_bytes_billed:,} bytes). "
                    "Raise the cap or narrow the scan with --partition-filter."
                ) from exc
            raise
        self.total_bytes += job.total_bytes_processed or 0
        LOG.info("query=%s job_id=%s bytes=%s", label, job.job_id, job.total_bytes_processed)
        return rows, job


@dataclass(frozen=True)
class Scan:
    table_id: str
    alias: str = "src"
    partition_filter: str | None = None

    def from_clause(self) -> str:
        return f"FROM {bq_ref(self.table_id)} AS {self.alias}"

    def where_clause(self) -> str:
        return f"\nWHERE {self.partition_filter}" if self.partition_filter else ""

    def tail(self) -> str:
        return f"\n{self.from_clause()}{self.where_clause()}"


# --------------------------------------------------------------------------- #
# Schema helpers
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


def type_signature(f: bigquery.SchemaField) -> str:
    return f"{standard_sql_type(f)}|{(f.mode or 'NULLABLE').upper()}"


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
        raise RestoreBlocked(
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


def collect_policy_tags(fields: Sequence[bigquery.SchemaField], prefix: str = "") -> dict[str, tuple[str, ...]]:
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
    seen: dict[str, str] = {}
    for f in fields:
        key = f.name.lower()
        if key in seen:
            raise RestoreBlocked(f"{where} has columns differing only by case: {seen[key]} / {f.name}")
        seen[key] = f.name


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #

INT_TYPES = {"INTEGER", "INT64"}
FLOAT_TYPES = {"FLOAT", "FLOAT64"}
EXACT_NUMERIC_TYPES = {"NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"}
TEMPORAL_TYPES = {"DATE", "DATETIME", "TIMESTAMP", "TIME"}
STRINGY_TYPES = {"STRING", "BYTES"}
BOOL_TYPES = {"BOOL", "BOOLEAN"}
VALUE_SUFFIXES = {"min", "max"}


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
    skip_row_count: bool = False,
) -> dict[str, list[str]]:
    """Group mismatches by family. Values of tagged columns are redacted."""
    col_of = {f"c{i}": f.name for i, f in enumerate(fields)}
    grouped: dict[str, list[str]] = {}
    for alias, kind in alias_kind.items():
        if alias == "row_count" and skip_row_count:
            continue
        family = alias.partition("_")[2] or alias
        if alias not in left or alias not in right:
            grouped.setdefault(family, []).append(f"{alias}: aggregate missing from one side")
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
                grouped.setdefault(family, []).append(f"{label}: values differ (redacted, policy-tagged)")
            else:
                grouped.setdefault(family, []).append(f"{label}: {a!r} vs {b!r}")
    return grouped


def build_hash_query(scan: Scan, column_names: Sequence[str]) -> str:
    """Order-independent content fingerprint over name-sorted columns."""
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


def key_json(alias: str, keys: Sequence[str]) -> str:
    cols = ", ".join(col_ref(alias, k) for k in keys)
    return f"TO_JSON_STRING(STRUCT({cols}))"


# --------------------------------------------------------------------------- #
# Schema comparison
# --------------------------------------------------------------------------- #


@dataclass
class SchemaDiff:
    common: list[str]
    new: list[str]
    removed: list[str]
    type_changed: list[tuple[str, str, str]]
    snapshot_fields: dict[str, bigquery.SchemaField]
    target_fields: dict[str, bigquery.SchemaField]


def compare_schemas(
    snapshot: Sequence[bigquery.SchemaField], target: Sequence[bigquery.SchemaField]
) -> SchemaDiff:
    snap = {f.name.lower(): f for f in snapshot}
    tgt = {f.name.lower(): f for f in target}

    common, type_changed = [], []
    for k in snap:
        if k not in tgt:
            continue
        s, t = snap[k], tgt[k]
        if type_signature(s) != type_signature(t):
            type_changed.append((t.name, type_signature(s), type_signature(t)))
        else:
            common.append(t.name)

    return SchemaDiff(
        common=common,
        new=[tgt[k].name for k in tgt if k not in snap],
        removed=[snap[k].name for k in snap if k not in tgt],
        type_changed=type_changed,
        snapshot_fields=snap,
        target_fields=tgt,
    )


def build_restore_sql(
    target_id: str,
    scan: Scan,
    diff: SchemaDiff,
    defaults: dict[str, str],
) -> str:
    """Fully dynamic INSERT. Column names are never hardcoded."""
    insert_cols, select_exprs = [], []

    for name in diff.common:
        insert_cols.append(q(name))
        select_exprs.append(col_ref(scan.alias, name))

    for name in diff.new:
        f = diff.target_fields[name.lower()]
        expr = defaults.get(name.lower())
        insert_cols.append(q(name))
        if expr:
            select_exprs.append(f"({expr}) AS {q(name)}")
        else:
            select_exprs.append(f"CAST(NULL AS {standard_sql_type(f)}) AS {q(name)}")

    return (
        f"INSERT INTO {bq_ref(target_id)}\n(\n    "
        + ",\n    ".join(insert_cols)
        + "\n)\nSELECT\n    "
        + ",\n    ".join(select_exprs)
        + scan.tail()
    )


# --------------------------------------------------------------------------- #
# PII guardrails
# --------------------------------------------------------------------------- #


def check_policy_tag_parity(
    diff: SchemaDiff,
    snapshot_tags: dict[str, tuple[str, ...]],
    target_tags: dict[str, tuple[str, ...]],
    allow_loss: bool,
    report: Report,
) -> None:
    """A tagged column must stay tagged in the Terraform-recreated table.

    If Terraform dropped the policy tag, restoring would move PII from an
    access-controlled column into an open one. That is a data exposure, not a
    data-loss issue, so it blocks by default.
    """
    if not snapshot_tags:
        report.add("policy_tag_parity", True, "snapshot has no policy tags", critical=False)
        return

    common_lower = {c.lower() for c in diff.common}
    lost, changed = [], []
    for path, tags in snapshot_tags.items():
        root = path.split(".", 1)[0]
        if root.lower() not in common_lower:
            continue
        tgt = target_tags.get(path, ())
        if not tgt:
            lost.append(path)
        elif tgt != tags:
            changed.append(f"{path}: {tags} -> {tgt}")

    detail = f"{len(snapshot_tags)} tagged columns, parity intact"
    if lost or changed:
        detail = (
            (f"tag REMOVED in target: {', '.join(sorted(lost))}; " if lost else "")
            + ("tag changed: " + "; ".join(changed) if changed else "")
        ).strip("; ")

    ok = not lost and not changed
    report.add("policy_tag_parity", ok or allow_loss, detail, critical=not allow_loss)
    if not ok and not allow_loss:
        raise RestoreBlocked(
            "POLICY TAG MISMATCH - restore blocked.\n  "
            + detail
            + "\nRestoring would write PII into a column that no longer carries its access "
            "control. Fix the Terraform schema to re-apply the policy tags, or pass "
            "--allow-policy-tag-loss if this de-classification is intentional and approved."
        )
    if not ok:
        LOG.warning("policy tag loss accepted via --allow-policy-tag-loss: %s", detail)


def check_column_masking(
    runner: QueryRunner,
    scan: Scan,
    diff: SchemaDiff,
    snapshot_tags: dict[str, tuple[str, ...]],
    row_count: int,
    acknowledge: bool,
    report: Report,
) -> None:
    """Detect data masking on tagged columns before writing them to production.

    BigQuery column-level masking is applied at read time. If this service
    account has Masked Reader rather than Fine-Grained Reader, INSERT ... SELECT
    would happily copy NULLs or hashes into the new production table and every
    aggregate comparison would still agree, because both sides would be masked.
    This probe catches nullify and default-value masking, which are the common
    policies. It cannot detect SHA256 masking, hence --acknowledge-masking-risk
    remains an explicit operator decision.
    """
    tagged_top = sorted({p.split(".", 1)[0] for p in snapshot_tags} & set(diff.common))
    if not tagged_top or not row_count:
        report.add("no_column_masking", True, "no tagged columns to probe", critical=False)
        return

    parts = []
    for i, name in enumerate(tagged_top):
        c = col_ref(scan.alias, name)
        f = diff.snapshot_fields[name.lower()]
        if classify(f) in ("repeated", "struct", "opaque"):
            parts.append(f"COUNT(*) AS n{i}, 2 AS d{i}")
        else:
            parts.append(f"COUNTIF({c} IS NULL) AS n{i}, COUNT(DISTINCT {c}) AS d{i}")

    rows, _ = runner.run("SELECT\n  " + ",\n  ".join(parts) + scan.tail(), "masking_probe")
    r = rows[0]

    suspicious = []
    for i, name in enumerate(tagged_top):
        nulls, distinct = r[f"n{i}"], r[f"d{i}"]
        if nulls == row_count:
            suspicious.append(f"{name}: 100% NULL across {row_count:,} rows")
        elif distinct <= 1 and row_count > 1:
            suspicious.append(f"{name}: only {distinct} distinct value across {row_count:,} rows")

    if not suspicious:
        report.add("no_column_masking", True, f"{len(tagged_top)} tagged columns look unmasked")
        return

    detail = "; ".join(suspicious)
    report.add("no_column_masking", acknowledge, detail, critical=not acknowledge)
    if not acknowledge:
        raise RestoreBlocked(
            "POSSIBLE COLUMN MASKING - restore blocked.\n  "
            + detail
            + "\nThese columns carry policy tags and read back as constant or all-NULL, which is "
            "what data masking looks like. Restoring now could write masked values into "
            "production. Grant roles/datacatalog.categoryFineGrainedReader to this service "
            "account and re-run. If the column genuinely is all-NULL or constant, pass "
            "--acknowledge-masking-risk."
        )
    LOG.warning("masking risk accepted via --acknowledge-masking-risk: %s", detail)


# --------------------------------------------------------------------------- #
# Pre-flight
# --------------------------------------------------------------------------- #


def get_table(client: bigquery.Client, table_id: str) -> bigquery.Table:
    try:
        return client.get_table(table_id)
    except gexc.NotFound as exc:
        raise RestoreBlocked(f"Not found: {table_id}") from exc
    except gexc.Forbidden as exc:
        raise RestoreBlocked(f"Access denied for {table_id}: {exc}") from exc


def validate_snapshot(
    runner: QueryRunner,
    client: bigquery.Client,
    snapshot_id: str,
    partition_filter: str | None,
    allow_plain_table: bool,
    report: Report,
) -> tuple[bigquery.Table, str | None]:
    table = get_table(client, snapshot_id)

    if table.table_type != "SNAPSHOT":
        msg = (
            f"{snapshot_id} is table_type={table.table_type}, not a BigQuery SNAPSHOT. "
            "This script is designed around real table snapshots."
        )
        if not allow_plain_table or table.table_type != "TABLE":
            raise RestoreBlocked(msg + " Pass --allow-plain-table-source to restore from a plain table.")
        LOG.warning("%s (overridden)", msg)

    if table.expires and table.expires <= datetime.now(timezone.utc):
        raise RestoreBlocked(f"Snapshot {snapshot_id} expired at {table.expires.isoformat()}")
    if table.expires and (table.expires - datetime.now(timezone.utc)).total_seconds() < 3600:
        LOG.warning("snapshot %s expires in under an hour (%s)", snapshot_id, table.expires)

    partition_filter = resolve_partition_filter(runner, table, partition_filter, snapshot_id)

    assert_unique_column_names(table.schema, snapshot_id)
    scan = Scan(snapshot_id, partition_filter=partition_filter)
    runner.run(f"SELECT 1{scan.tail()}\nLIMIT 0", "snapshot_probe")

    snap_def = getattr(table, "snapshot_definition", None)
    report.facts["snapshot_meta"] = {
        "table_id": snapshot_id,
        "table_type": table.table_type,
        "snapshot_time": snap_def.snapshot_time.isoformat() if snap_def and snap_def.snapshot_time else None,
        "base_table": snap_def.base_table_reference.path if snap_def else None,
        "expires": table.expires.isoformat() if table.expires else None,
        "policy_tagged_columns": {k: list(v) for k, v in collect_policy_tags(table.schema).items()},
        "partition_filter": partition_filter,
    }
    LOG.info("snapshot ok id=%s type=%s columns=%d", snapshot_id, table.table_type, len(table.schema))
    return table, partition_filter


def validate_target(
    runner: QueryRunner,
    client: bigquery.Client,
    target_id: str,
    partition_filter: str | None,
    allow_non_empty: bool,
    report: Report,
) -> tuple[bigquery.Table, int, str | None]:
    table = get_table(client, target_id)
    if table.table_type != "TABLE":
        raise RestoreBlocked(f"{target_id} is a {table.table_type}, not a base TABLE")
    partition_filter = resolve_partition_filter(runner, table, partition_filter, target_id)

    assert_unique_column_names(table.schema, target_id)
    scan = Scan(target_id, alias="t", partition_filter=partition_filter)
    rows, _ = runner.run(f"SELECT COUNT(*) AS n{scan.tail()}", "target_count")
    existing = rows[0]["n"]

    if existing and not allow_non_empty:
        raise RestoreBlocked(
            f"Target {target_id} already contains {existing:,} rows. Refusing to append - "
            "re-running this script would duplicate the restore. Pass "
            "--allow-non-empty-target only if appending is intended. This script never "
            "truncates or deletes data."
        )
    if existing:
        LOG.warning("target already holds %d rows and --allow-non-empty-target was supplied", existing)

    report.facts["target_meta"] = {
        "table_id": target_id,
        "pre_existing_rows": existing,
        "columns": [f.name for f in table.schema],
        "last_modified": table.modified.isoformat() if table.modified else None,
        "policy_tagged_columns": {k: list(v) for k, v in collect_policy_tags(table.schema).items()},
        "partition_filter": partition_filter,
    }
    return table, existing, partition_filter


# --------------------------------------------------------------------------- #
# Post-restore checks
# --------------------------------------------------------------------------- #


def run_sample_check(
    runner: QueryRunner,
    snap_scan: Scan,
    tgt_scan: Scan,
    keys: Sequence[str],
    common: Sequence[str],
    snapshot_rows: int,
    sample_size: int,
    report: Report,
) -> None:
    """Deterministic key-bucket sample compared column-by-column via row hash."""
    buckets = max(1, round(snapshot_rows / max(1, sample_size))) if snapshot_rows else 1
    key_list = ", ".join(q(k) for k in keys)

    def side(scan: Scan) -> str:
        payload = ", ".join(col_ref(scan.alias, n) for n in sorted(common))
        keycols = ", ".join(col_ref(scan.alias, k) for k in keys)
        bucket = f"MOD(FARM_FINGERPRINT({key_json(scan.alias, keys)}) & 0x7FFFFFFFFFFFFFFF, {buckets}) = 0"
        where = f"{scan.partition_filter} AND {bucket}" if scan.partition_filter else bucket
        return (
            f"SELECT {keycols}, FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({payload}))) AS h\n"
            f"    {scan.from_clause()}\n    WHERE {where}"
        )

    sql = (
        f"WITH s AS (\n    {side(snap_scan)}\n),\n"
        f"t AS (\n    {side(tgt_scan)}\n)\n"
        "SELECT\n"
        "  COUNT(*) AS pairs,\n"
        "  COUNTIF(s.h IS NULL) AS missing_in_snapshot,\n"
        "  COUNTIF(t.h IS NULL) AS missing_in_target,\n"
        "  COUNTIF(s.h IS NOT NULL AND t.h IS NOT NULL AND s.h != t.h) AS mismatches\n"
        f"FROM s FULL OUTER JOIN t USING ({key_list})"
    )
    r = runner.run(sql, "sample_compare")[0][0]
    report.facts.update(
        {
            "sample_compared": r["pairs"],
            "sample_mismatches": r["mismatches"],
            "sample_missing_target": r["missing_in_target"],
        }
    )
    ok = r["mismatches"] == 0 and r["missing_in_target"] == 0 and r["missing_in_snapshot"] == 0
    report.add(
        "sample_check",
        ok,
        f"pairs={r['pairs']:,} mismatches={r['mismatches']:,} "
        f"missing_in_target={r['missing_in_target']:,} "
        f"missing_in_snapshot={r['missing_in_snapshot']:,} (1 in {buckets} rows)",
    )


def run_new_column_check(
    runner: QueryRunner, scan: Scan, diff: SchemaDiff, defaults: dict[str, str], report: Report
) -> None:
    """Every new column must be NULL everywhere, unless a default was configured."""
    if not diff.new:
        report.facts["new_column_detail"] = {}
        report.add("new_column_check", True, "no new columns")
        return

    parts, mode_of = [], {}
    for i, name in enumerate(diff.new):
        c = col_ref(scan.alias, name)
        expr = defaults.get(name.lower())
        if expr:
            parts.append(f"COUNTIF(NOT ({c} IS NOT DISTINCT FROM ({expr}))) AS n{i}")
            mode_of[name] = ("default", expr)
        else:
            parts.append(f"COUNTIF({c} IS NOT NULL) AS n{i}")
            mode_of[name] = ("null", None)

    r = runner.run("SELECT\n  " + ",\n  ".join(parts) + scan.tail(), "new_columns")[0][0]

    bad, rendered = [], {}
    for i, name in enumerate(diff.new):
        offenders = r[f"n{i}"]
        mode, expr = mode_of[name]
        if offenders:
            bad.append(f"{name}: {offenders:,} rows deviate from configured {mode}")
            rendered[name] = f"{offenders:,} unexpected values  FAIL"
        else:
            rendered[name] = "NULL (ok)" if mode == "null" else f"default {expr} (ok)"
    report.facts["new_column_detail"] = rendered
    report.add("new_column_check", not bad, "; ".join(bad) if bad else f"{len(diff.new)} columns verified")


# --------------------------------------------------------------------------- #
# Report rendering
# --------------------------------------------------------------------------- #

LINE = "=" * 50
THIN = "-" * 50


def render(report: Report, cfg: ValidationConfig, diff: SchemaDiff | None, notes: list[str]) -> str:
    def row(label: str, value: Any) -> str:
        return f"{label:<31}{value}"

    def num(v: Any) -> str:
        return f"{v:,}" if isinstance(v, int) else "n/a"

    f = report.facts
    snap_meta = f.get("snapshot_meta", {})
    tagged = snap_meta.get("policy_tagged_columns", {})

    out = [
        LINE, "BIGQUERY RESTORE VALIDATION", LINE, "",
        "Snapshot:", report.snapshot, "",
        "Target:", report.target, "",
        row("Snapshot type", snap_meta.get("table_type", "n/a")),
        row("Snapshot time", snap_meta.get("snapshot_time", "n/a")),
        "", THIN, "SCHEMA", THIN, "",
        row("Compatible columns", report.status("schema_compatible")),
        row("New columns", len(diff.new) if diff else "n/a"),
        row("Removed columns", len(diff.removed) if diff else "n/a"),
        row("Type compatibility", report.status("no_type_changes")),
        "", THIN, "PII / POLICY TAGS", THIN, "",
        row("Tagged columns in snapshot", len(tagged)),
        row("Tag parity in target", report.status("policy_tag_parity")),
        row("Masking probe", report.status("no_column_masking")),
    ]
    for name in sorted(tagged):
        out.append(row(f"  {name}", "tagged"))

    out += [
        "", THIN, "ROW COUNTS", THIN, "",
        row("Snapshot", num(f.get("snapshot_rows"))),
        row("Target", num(f.get("target_rows"))),
        row("Rows inserted (DML)", num(f.get("rows_inserted"))),
        row("Result", report.status("row_count")),
        "", THIN, "DATA RECONCILIATION", THIN, "",
        row("Distinct key count", report.status("distinct_key_count")),
        row("NULL counts", report.status("stats_nulls")),
        row("MIN/MAX", report.status("stats_minmax")),
        row("SUM", report.status("stats_sum")),
        row("COUNT DISTINCT", report.status("stats_distinct")),
        row("Complex column hashes", report.status("stats_complex")),
        row("Full row hash", report.status("data_hash")),
        "", THIN, "DUPLICATES", THIN, "",
        row("Duplicate keys", f.get("duplicate_keys", "n/a")),
        row("Result", report.status("duplicate_check")),
        "", THIN, "NEW COLUMNS", THIN, "",
    ]

    new_cols = f.get("new_column_detail", {})
    out += [row(f"  {n}", d) for n, d in new_cols.items()] or ["  (none)"]

    out += [
        "", THIN, "SAMPLE VALIDATION", THIN, "",
        row("Sample size (requested)", num(cfg.sample_size) if cfg.sample_check else "skipped"),
        row("Records compared", num(f.get("sample_compared"))),
        row("Mismatches", num(f.get("sample_mismatches"))),
        row("Missing in target", num(f.get("sample_missing_target"))),
        row("Result", report.status("sample_check")),
        "", THIN, "RESTORE JOB", THIN, "",
        row("Job ID", f.get("job_id", "n/a")),
        row("Bytes processed", num(f.get("bytes_processed"))),
        row("Duration (s)", f.get("duration_s", "n/a")),
        row("Concurrent writers", report.status("no_concurrent_writes")),
        "", THIN, "VALIDATIONS EXECUTED", THIN, "",
        "  " + "  ".join(cfg.executed()),
    ]

    if diff and diff.removed:
        out += ["", THIN, "NOT RESTORED", THIN, ""]
        out += [f"  {n}: not present in target schema" for n in diff.removed]

    if notes:
        out += ["", THIN, "NOTES", THIN, ""] + [f"  - {n}" for n in notes]

    failures = [c for c in report.checks if not c.passed]
    if failures:
        out += ["", THIN, "FAILURE DETAIL", THIN, ""] + [f"  {c.name}: {c.detail}" for c in failures]

    out += ["", LINE, f"FINAL RESULT: {'PASS' if report.ok else 'FAIL'}", LINE]
    out.append(
        "Restored data reconciles with the snapshot on every configured check."
        if not failures
        else "Snapshot retained and target left untouched by this script. "
             "Investigate before declaring the migration complete."
    )
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
        raise RestoreError(
            f"Invalid table reference {ref!r}: expected project_id.dataset_id.table_name"
        )
    project, dataset, table = parts
    return valid_project(project), valid_ident(dataset, "dataset"), valid_table_name(table)


def parse_per_table(values: Sequence[str], kind: str) -> tuple[str | None, dict[str, str]]:
    """Parse repeatable 'TABLE=VALUE' options, with a bare value as the default."""
    default: str | None = None
    per_table: dict[str, str] = {}
    for item in values or []:
        name, sep, value = item.partition("=")
        if sep and TABLE_RE.match(name.strip()) and " " not in name.strip():
            per_table[name.strip().lower()] = value.strip()
        elif default is None:
            default = item.strip()
        else:
            raise RestoreError(f"More than one unscoped {kind} supplied: {default!r} and {item!r}")
    return default, per_table


def resolve_for(table: str, default: str | None, per_table: dict[str, str]) -> str | None:
    return per_table.get(table.lower(), default)


def parse_defaults(pairs: Sequence[str], table: str) -> dict[str, str]:
    """Parse --default-expression [TABLE.]COLUMN=SQL_EXPRESSION for one table.

    Unscoped entries apply to every table; scoped entries only to the named one.
    """
    out: dict[str, str] = {}
    for item in pairs or []:
        if "=" not in item:
            raise RestoreError(
                f"--default-expression must be [TABLE.]COLUMN=SQL_EXPRESSION, got {item!r}"
            )
        lhs, _, expr = item.partition("=")
        lhs = lhs.strip()
        if "." in lhs:
            scope, _, column = lhs.rpartition(".")
            if scope.lower() != table.lower():
                continue
        else:
            column = lhs
        out[valid_ident(column, "default expression column").lower()] = valid_expression(
            expr, f"--default-expression for {lhs}"
        )
    return out


def discover_snapshot(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    suffix: str,
    stamp: str | None,
) -> str:
    """Find the snapshot for a table, in that table's own dataset.

    Matches <table>__snapshot__<stamp><suffix> and accepts only objects whose
    table_type is SNAPSHOT, so a similarly named ordinary table can never be
    mistaken for a snapshot. With --snapshot-stamp the match must be exact;
    otherwise the most recent snapshot wins and ambiguity is reported rather
    than silently resolved.
    """
    prefix = f"{table}__snapshot__"
    dataset_ref = f"{project}.{dataset}"
    try:
        listed = list(client.list_tables(dataset_ref))
    except gexc.NotFound as exc:
        raise RestoreBlocked(f"Dataset not found: {dataset_ref}") from exc
    except gexc.Forbidden as exc:
        raise RestoreBlocked(f"Access denied listing {dataset_ref}: {exc}") from exc

    candidates = [
        t for t in listed
        if t.table_id.startswith(prefix)
        and t.table_id.endswith(suffix)
        and getattr(t, "table_type", "SNAPSHOT") == "SNAPSHOT"
    ]

    if stamp:
        want = f"{prefix}{stamp}{suffix}"
        exact = [t for t in candidates if t.table_id == want]
        if not exact:
            raise RestoreBlocked(f"No snapshot named {dataset_ref}.{want}")
        return f"{dataset_ref}.{exact[0].table_id}"

    if not candidates:
        near = [t.table_id for t in listed if t.table_id.startswith(prefix)]
        hint = (
            f" Found {len(near)} name match(es) that are not table_type=SNAPSHOT: "
            f"{', '.join(near[:3])}." if near else ""
        )
        raise RestoreBlocked(
            f"No snapshot matching {dataset_ref}.{prefix}*{suffix}.{hint} "
            "Check --snapshot-suffix, or pass --snapshot-table explicitly."
        )

    # The name stamp is lexicographically sortable (YYYYMMDD_HHMMSS).
    latest = sorted(candidates, key=lambda t: t.table_id)[-1]
    if len(candidates) > 1:
        LOG.warning(
            "%d snapshots match %s*%s; selecting the most recent: %s. "
            "Use --snapshot-stamp to pin an exact one.",
            len(candidates), prefix, suffix, latest.table_id,
        )
    return f"{dataset_ref}.{latest.table_id}"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Restore BigQuery snapshots into Terraform-recreated tables and prove accuracy.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=(
            "The snapshot is found automatically in each target table's own dataset,\n"
            "by the naming convention <table>__snapshot__YYYYMMDD_HHMMSS_vw\n\n"
            "Examples:\n"
            "  # check everything without writing\n"
            "  %(prog)s --tables proj.ds.customer proj.ds.orders --dry-run\n\n"
            "  # restore, pinning an exact snapshot stamp\n"
            "  %(prog)s --tables proj.ds.customer proj.ds.orders \\\n"
            "           --snapshot-stamp 20260821_010530\n"
        ),
    )
    p.add_argument(
        "--tables",
        nargs="+",
        metavar="PROJECT.DATASET.TABLE",
        help="One or more fully qualified target tables",
    )
    # Legacy single-table form, still supported.
    p.add_argument("--project")
    p.add_argument("--dataset")
    p.add_argument("--table")
    p.add_argument("--snapshot-table", help="Exact snapshot table name (single-table mode only)")

    p.add_argument("--snapshot-suffix", default="_vw", help="Suffix used when the snapshot was created")
    p.add_argument("--snapshot-stamp", help="Pin an exact YYYYMMDD_HHMMSS stamp")

    p.add_argument("--primary-key", action="append", default=[], metavar="[TABLE=]COLS")
    p.add_argument("--partition-filter", action="append", default=[], metavar="[TABLE=]PREDICATE")
    p.add_argument("--default-expression", action="append", default=[], metavar="[TABLE.]COL=EXPR")

    p.add_argument("--allow-non-empty-target", action="store_true")
    p.add_argument("--allow-plain-table-source", action="store_true")
    p.add_argument("--allow-policy-tag-loss", action="store_true")
    p.add_argument("--acknowledge-masking-risk", action="store_true")
    p.add_argument("--fail-fast", action="store_true", help="Stop at the first table that fails")

    p.add_argument("--sample-size", type=int, default=10000)
    p.add_argument("--float-tolerance", type=float, default=1e-9)
    p.add_argument("--approx-distinct", action="store_true")
    p.add_argument("--max-bytes-billed", type=int)
    p.add_argument("--query-timeout", type=float, default=7200.0)
    p.add_argument("--config", help="JSON file overriding the validation toggles")
    p.add_argument("--no-statistics", action="store_true")
    p.add_argument("--no-distinct-count", action="store_true")
    p.add_argument("--no-duplicate-check", action="store_true")
    p.add_argument("--no-hash-check", action="store_true")
    p.add_argument("--no-sample-check", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="Run every pre-flight check, skip the INSERT")
    p.add_argument("--location")
    p.add_argument("--billing-project")
    p.add_argument("-v", "--verbose", action="store_true")

    args = p.parse_args(argv)
    if args.sample_size < 1:
        p.error("--sample-size must be >= 1")
    if not args.tables and not (args.project and args.dataset and args.table):
        p.error("supply --tables PROJECT.DATASET.TABLE [...] (or the legacy --project/--dataset/--table)")
    if args.tables and (args.project or args.dataset or args.table):
        p.error("--tables cannot be combined with --project/--dataset/--table")
    if args.snapshot_table and args.tables and len(args.tables) > 1:
        p.error("--snapshot-table is only valid when restoring a single table")
    return args


def build_config(args: argparse.Namespace) -> ValidationConfig:
    cfg = ValidationConfig(
        statistics=not args.no_statistics,
        distinct_count=not args.no_distinct_count,
        duplicate_check=not args.no_duplicate_check,
        hash_check=not args.no_hash_check,
        sample_check=not args.no_sample_check,
        sample_size=args.sample_size,
        approx_distinct=args.approx_distinct,
        float_tolerance=args.float_tolerance,
    )
    if args.config:
        with open(args.config, encoding="utf-8") as fh:
            blob = json.load(fh)
        cfg.merge(blob.get("validation", blob))
    return cfg


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
    target_ref: tuple[str, str, str],
    snapshot_id: str,
    args: argparse.Namespace,
    cfg: ValidationConfig,
    primary_key: str | None,
    partition_filter: str | None,
) -> Report:
    """Restore and validate a single table. Never raises; failures land in the Report."""
    report = Report()
    notes: list[str] = []
    diff: SchemaDiff | None = None
    project, dataset, table = target_ref
    target_id = fq(project, dataset, table)
    report.snapshot, report.target = snapshot_id, target_id

    runner = QueryRunner(client, args.max_bytes_billed, args.query_timeout, JOB_LABELS)

    try:
        defaults = parse_defaults(args.default_expression, table)
        keys = [
            valid_ident(c.strip(), "primary key column")
            for c in (primary_key or "").split(",")
            if c.strip()
        ]
        pfilter = valid_expression(partition_filter, "--partition-filter") if partition_filter else None

        # ---- Steps 1 + 2: pre-flight --------------------------------------- #
        snapshot, snap_filter = validate_snapshot(
            runner, client, snapshot_id, pfilter, args.allow_plain_table_source, report
        )
        target, pre_existing, tgt_filter = validate_target(
            runner, client, target_id, pfilter, args.allow_non_empty_target, report
        )
        target_modified_before = target.modified

        # Snapshot and target can partition on different columns, so each gets
        # its own predicate rather than sharing one.
        snap_scan = Scan(snapshot_id, alias="src", partition_filter=snap_filter)
        tgt_scan = Scan(target_id, alias="t", partition_filter=tgt_filter)
        for which, expr in (("snapshot", snap_filter), ("target", tgt_filter)):
            if expr:
                notes.append(f"Partition filter on {which}: {expr}")

        # ---- Step 3: schema comparison ------------------------------------- #
        diff = compare_schemas(snapshot.schema, target.schema)
        LOG.info(
            "[%s] schema diff common=%d new=%s removed=%s type_changed=%s",
            target_id, len(diff.common), diff.new, diff.removed, [c[0] for c in diff.type_changed],
        )

        if diff.type_changed:
            lines = "\n".join(
                f"  column={n}\n    snapshot: {s}\n    target:   {t}" for n, s, t in diff.type_changed
            )
            raise RestoreBlocked(
                "TYPE CHANGE DETECTED - restore blocked.\n" + lines + "\n"
                "This script deliberately does not guess conversions. Align the Terraform type "
                "with the snapshot, or handle the conversion as a separate, reviewed step."
            )
        report.add("no_type_changes", True, "no type or mode changes on common columns")

        if not diff.common:
            raise RestoreBlocked("No common columns between snapshot and target. Nothing to restore.")
        report.add("schema_compatible", True, f"{len(diff.common)} common columns")

        for name in diff.removed:
            LOG.warning("[%s] %s will not be restored: absent from target schema", target_id, name)
            notes.append(f"{name}: present in snapshot, absent in target - not restored")

        unknown = set(defaults) - {n.lower() for n in diff.new}
        if unknown:
            raise RestoreBlocked(
                "--default-expression given for columns that are not NEW target columns: "
                + ", ".join(sorted(unknown))
            )

        required_new = [
            n for n in diff.new
            if (diff.target_fields[n.lower()].mode or "NULLABLE").upper() == "REQUIRED"
            and n.lower() not in defaults
        ]
        if required_new:
            raise RestoreBlocked(
                "New target columns are REQUIRED but have no value in the snapshot: "
                + ", ".join(required_new)
                + ". Supply --default-expression COL=EXPR or relax the Terraform schema."
            )

        tightened = [
            n for n in diff.common
            if (diff.target_fields[n.lower()].mode or "NULLABLE").upper() == "REQUIRED"
            and (diff.snapshot_fields[n.lower()].mode or "NULLABLE").upper() != "REQUIRED"
        ]
        if tightened:
            checks = ", ".join(
                f"COUNTIF({col_ref('src', n)} IS NULL) AS n{i}" for i, n in enumerate(tightened)
            )
            r = runner.run(f"SELECT {checks}{snap_scan.tail()}", "required_nulls")[0][0]
            offenders = [n for i, n in enumerate(tightened) if r[f"n{i}"]]
            if offenders:
                raise RestoreBlocked(
                    "Target marks these columns REQUIRED but the snapshot contains NULLs: "
                    + ", ".join(offenders)
                )

        if keys:
            missing = [k for k in keys if k.lower() not in {c.lower() for c in diff.common}]
            if missing:
                raise RestoreBlocked(
                    f"primary key columns are not common to both schemas: {', '.join(missing)}"
                )
        else:
            notes.append(
                "No primary key supplied: duplicate, distinct-key and sample checks are skipped. "
                "The order-independent full-row hash still runs."
            )

        # ---- PII guardrails ------------------------------------------------ #
        snapshot_tags = collect_policy_tags(snapshot.schema)
        target_tags = collect_policy_tags(target.schema)
        sensitive = {p.split(".", 1)[0] for p in snapshot_tags}

        if cfg.policy_tag_check:
            check_policy_tag_parity(diff, snapshot_tags, target_tags, args.allow_policy_tag_loss, report)

        snap_rows_pre = runner.run(f"SELECT COUNT(*) AS n{snap_scan.tail()}", "snapshot_count")[0][0]["n"]
        if snapshot_tags:
            check_column_masking(
                runner, snap_scan, diff, snapshot_tags, snap_rows_pre,
                args.acknowledge_masking_risk, report,
            )

        # ---- Step 4: dynamic restore SQL ----------------------------------- #
        sql = build_restore_sql(target_id, snap_scan, diff, defaults)
        report.facts["restore_sql"] = sql
        LOG.info("[%s] generated restore SQL:\n%s", target_id, sql)

        runner.run(sql, "restore_dry_run", dry_run=True)
        report.add("restore_sql_valid", True, "dry run accepted by BigQuery")

        if args.dry_run:
            LOG.warning("[%s] --dry-run supplied: stopping before the INSERT", target_id)
            notes.append("--dry-run: no data was written")
            report.facts["dry_run"] = True
            print(render(report, cfg, diff, notes))
            return report

        # ---- Step 7: execute ----------------------------------------------- #
        started = time.monotonic()
        _, job = runner.run(sql, "restore_insert")
        duration = round(time.monotonic() - started, 2)
        report.facts.update(
            {
                "job_id": job.job_id,
                "duration_s": duration,
                "rows_inserted": job.num_dml_affected_rows,
                "destination": target_id,
            }
        )
        LOG.info(
            "[%s] restore complete job_id=%s rows=%s duration_s=%s",
            target_id, job.job_id, job.num_dml_affected_rows, duration,
        )

        if job.num_dml_affected_rows is not None:
            report.add(
                "dml_rows_match_snapshot",
                job.num_dml_affected_rows == snap_rows_pre,
                f"inserted={job.num_dml_affected_rows:,} snapshot={snap_rows_pre:,}",
            )

        # ---- Step 8: post-restore validation ------------------------------- #
        common_snapshot_fields = [diff.snapshot_fields[n.lower()] for n in diff.common]
        common_target_fields = [diff.target_fields[n.lower()] for n in diff.common]

        if cfg.statistics:
            sql_s, alias_kind, stat_notes = build_stats_query(snap_scan, common_snapshot_fields, cfg)
            sql_t, _, _ = build_stats_query(tgt_scan, common_target_fields, cfg)
            notes.extend(stat_notes)
            s_stats = runner.run(sql_s, "stats_snapshot")[0][0]
            t_stats = runner.run(sql_t, "stats_target")[0][0]
        else:
            alias_kind = {"row_count": "exact"}
            s_stats = runner.run(f"SELECT COUNT(*) AS row_count{snap_scan.tail()}", "count_snapshot")[0][0]
            t_stats = runner.run(f"SELECT COUNT(*) AS row_count{tgt_scan.tail()}", "count_target")[0][0]

        snapshot_rows, target_rows = s_stats["row_count"], t_stats["row_count"]
        report.facts["snapshot_rows"] = snapshot_rows
        report.facts["target_rows"] = target_rows

        expected = snapshot_rows + pre_existing
        report.add(
            "row_count",
            target_rows == expected,
            f"snapshot={snapshot_rows:,} + pre_existing={pre_existing:,} "
            f"expected={expected:,} target={target_rows:,}",
        )
        if pre_existing:
            notes.append(
                f"Target already held {pre_existing:,} rows; statistic, hash and duplicate "
                "comparisons include those rows and may legitimately differ."
            )

        if cfg.statistics:
            grouped = compare_stats(
                s_stats, t_stats, alias_kind, common_target_fields,
                cfg.float_tolerance, sensitive, skip_row_count=True,
            )
            families = {
                "stats_nulls": ["nulls", "empty", "elems", "true", "false"],
                "stats_minmax": ["min", "max"],
                "stats_sum": ["sum"],
                "stats_distinct": ["distinct"],
                "stats_complex": ["xor", "fsum"],
            }
            for check_name, fams in families.items():
                bad = [m for k in fams for m in grouped.get(k, [])]
                report.add(check_name, not bad, "match" if not bad else "; ".join(bad[:8]))

        if keys:
            if cfg.distinct_count:
                sd = runner.run(
                    f"SELECT COUNT(DISTINCT {key_json('src', keys)}) AS d{snap_scan.tail()}",
                    "distinct_key_snapshot",
                )[0][0]["d"]
                td = runner.run(
                    f"SELECT COUNT(DISTINCT {key_json('t', keys)}) AS d{tgt_scan.tail()}",
                    "distinct_key_target",
                )[0][0]["d"]
                report.add("distinct_key_count", sd == td, f"{sd:,} vs {td:,}")

            if cfg.duplicate_check:
                key_list = ", ".join(q(k) for k in keys)
                dup_sql = (
                    "SELECT COUNT(*) AS dup_keys FROM (\n"
                    f"  SELECT {key_list}, COUNT(*) AS c{tgt_scan.tail()}\n"
                    f"  GROUP BY {key_list}\n  HAVING c > 1\n)"
                )
                dups = runner.run(dup_sql, "duplicate_check")[0][0]["dup_keys"]
                report.facts["duplicate_keys"] = dups
                report.add("duplicate_check", dups == 0, f"{dups:,} duplicated key values")

        if cfg.new_column_check:
            run_new_column_check(runner, tgt_scan, diff, defaults, report)

        if cfg.hash_check:
            h1 = digest(runner.run(build_hash_query(snap_scan, diff.common), "hash_snapshot")[0][0])
            h2 = digest(runner.run(build_hash_query(tgt_scan, diff.common), "hash_target")[0][0])
            report.facts["snapshot_hash"], report.facts["target_hash"] = h1, h2
            report.add("data_hash", h1 == h2, f"{h1} vs {h2}")

        if cfg.sample_check and keys:
            run_sample_check(
                runner, snap_scan, tgt_scan, keys, diff.common, snapshot_rows, cfg.sample_size, report
            )

        # Concurrent writer detection: anything other than our own job touching
        # the target would invalidate every comparison above.
        after = client.get_table(target_id)
        report.add(
            "no_concurrent_writes",
            after.num_rows in (None, expected) or target_rows == expected,
            f"metadata rows={after.num_rows} expected={expected} "
            f"modified_before={target_modified_before} modified_after={after.modified}",
            critical=False,
        )

    except RestoreError as exc:
        LOG.error("[%s] %s", target_id, exc)
        report.add("fatal", False, str(exc))
    except gexc.GoogleAPIError as exc:  # pragma: no cover
        LOG.exception("[%s] BigQuery API error", target_id)
        report.add("fatal", False, f"BigQuery API error: {exc}")

    report.facts["bytes_processed"] = runner.total_bytes
    report.facts["notes"] = notes
    if not report.facts.get("dry_run"):
        print(render(report, cfg, diff, notes))

    if not report.ok:
        LOG.error(
            "[%s] restore validation FAILED - snapshot %s retained, target left untouched by this "
            "script. No automatic rollback was attempted.",
            target_id, snapshot_id,
        )
    return report


# --------------------------------------------------------------------------- #
# Batch summary
# --------------------------------------------------------------------------- #


def render_summary(results: list[tuple[str, Report]], skipped: list[str]) -> str:
    out = ["", LINE, "BATCH SUMMARY", LINE, ""]
    width = max([len(t) for t, _ in results] + [len(s) for s in skipped] + [12])
    for target, rep in results:
        rows = rep.facts.get("target_rows")
        suffix = f"  rows={rows:,}" if isinstance(rows, int) else ""
        out.append(f"{'PASS' if rep.ok else 'FAIL'}  {target:<{width}}{suffix}")
        if rep.snapshot:
            out.append(f"      <- {rep.snapshot}")
        if not rep.ok:
            for c in rep.checks:
                if not c.passed:
                    out.append(f"      !! {c.name}: {c.detail[:160]}")
    for target in skipped:
        out.append(f"SKIP  {target:<{width}}  (--fail-fast: an earlier table failed)")

    passed = sum(1 for _, r in results if r.ok)
    out += [
        "",
        f"Tables requested   {len(results) + len(skipped)}",
        f"Passed             {passed}",
        f"Failed             {len(results) - passed}",
        f"Skipped            {len(skipped)}",
        "",
        LINE,
        f"BATCH RESULT: {'PASS' if passed == len(results) and results and not skipped else 'FAIL'}",
        LINE,
    ]
    return "\n".join(out)


def resolve_plan(args: argparse.Namespace) -> list[tuple[str, str, str]]:
    """Build the list of target table refs to restore."""
    if args.tables:
        refs = [split_table_ref(t) for t in args.tables]
    else:
        refs = [
            (valid_project(args.project), valid_ident(args.dataset, "dataset"),
             valid_table_name(args.table))
        ]

    seen: set[str] = set()
    for ref in refs:
        key = ".".join(ref).lower()
        if key in seen:
            raise RestoreError(f"Duplicate table requested: {'.'.join(ref)}")
        seen.add(key)
    return refs


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)
    LOG.info("%s version=%s python=%s", __file__, __version__, sys.version.split()[0])

    try:
        refs = resolve_plan(args)
        pk_default, pk_map = parse_per_table(args.primary_key, "--primary-key")
        pf_default, pf_map = parse_per_table(args.partition_filter, "--partition-filter")
        for scope in (set(pk_map) | set(pf_map)) - {ref[2].lower() for ref in refs}:
            raise RestoreError(f"Option scoped to table {scope!r}, which is not being restored")
        cfg = build_config(args)
    except (RestoreError, OSError, json.JSONDecodeError) as exc:
        LOG.error("%s", exc)
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return 1

    LOG.info("batch start tables=%d dry_run=%s", len(refs), args.dry_run)

    results: list[tuple[str, Report]] = []
    skipped: list[str] = []

    for i, ref in enumerate(refs, start=1):
        target_id = ".".join(ref)
        LOG.info("[%d/%d] processing %s", i, len(refs), target_id)
        client = bigquery.Client(project=args.billing_project or ref[0], location=args.location)

        report = Report(target=target_id)
        try:
            if args.snapshot_table:
                snapshot_id = fq(ref[0], ref[1], args.snapshot_table)
            else:
                snapshot_id = discover_snapshot(
                    client, ref[0], ref[1], ref[2], args.snapshot_suffix, args.snapshot_stamp
                )
            LOG.info("[%s] using snapshot %s", target_id, snapshot_id)
            report = process_table(
                client, ref, snapshot_id, args, cfg,
                resolve_for(ref[2], pk_default, pk_map),
                resolve_for(ref[2], pf_default, pf_map),
            )
        except RestoreError as exc:
            LOG.error("[%s] %s", target_id, exc)
            report.add("fatal", False, str(exc))

        results.append((target_id, report))
        if not report.ok and args.fail_fast:
            skipped = [".".join(r) for r in refs[i:]]
            LOG.error("--fail-fast: stopping, %d table(s) not attempted", len(skipped))
            break

    print(render_summary(results, skipped))

    all_ok = bool(results) and all(r.ok for _, r in results) and not skipped
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())