"""
dataplex_dq_runner.py
---------------------
Runs Dataplex CloudDQ checks defined in a JSON rules file.
Designed to run on a Dataproc cluster with Dataplex/BigQuery permissions.

Usage:
    python dataplex_dq_runner.py --rules dq_rules.json
    python dataplex_dq_runner.py --rules dq_rules.json --write-to-bq
    python dataplex_dq_runner.py --rules dq_rules.json --write-to-bq --skip-create
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

# ── GCP client libraries ──────────────────────────────────────────────────────
from google.cloud import dataplex_v1
from google.cloud import bigquery
from google.cloud import storage as gcs
from google.api_core import exceptions as google_exceptions

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s  [%(levelname)s]  %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)
log = logging.getLogger("dataplex_dq_runner")


# ═════════════════════════════════════════════════════════════════════════════
# 1.  LOAD & VALIDATE CONFIG
# ═════════════════════════════════════════════════════════════════════════════

def load_config(rules_file: str) -> dict:
    """Read and return the JSON rules config. Supports local and gs:// paths."""
    log.info(f"Loading rules from: {rules_file}")

    if rules_file.startswith("gs://"):
        # Strip gs:// prefix and split into bucket + blob path
        path = rules_file[5:]
        bucket_name, blob_path = path.split("/", 1)
        gcs_client = gcs.Client()
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        config = json.loads(blob.download_as_text())
        log.info("  Source: Google Cloud Storage")
    else:
        with open(rules_file, "r") as f:
            config = json.load(f)
        log.info("  Source: Local filesystem")

    # Basic validation
    required_top = ["datascan_config", "data_source", "rules"]
    for key in required_top:
        if key not in config:
            raise ValueError(f"Missing required key in JSON: '{key}'")

    required_scan = ["project_id", "location", "datascan_id"]
    for key in required_scan:
        if key not in config["datascan_config"]:
            raise ValueError(f"Missing key in datascan_config: '{key}'")

    if "bigquery_table" not in config["data_source"]:
        raise ValueError("data_source must contain 'bigquery_table'")

    if not config["rules"]:
        raise ValueError("'rules' list is empty — nothing to run.")

    log.info(f"  Scan ID   : {config['datascan_config']['datascan_id']}")
    log.info(f"  BQ Table  : {config['data_source']['bigquery_table']}")
    log.info(f"  Rule count: {len(config['rules'])}")
    return config


# ═════════════════════════════════════════════════════════════════════════════
# 2.  BUILD DATAPLEX RULE OBJECTS
# ═════════════════════════════════════════════════════════════════════════════

def build_dataplex_rule(rule: dict) -> dataplex_v1.DataQualityRule:
    """
    Convert a JSON rule dict into a Dataplex DataQualityRule proto.

    Supported types:
        NON_NULL_EXPECTATION
        UNIQUENESS_EXPECTATION
        RANGE_EXPECTATION
        REGEX_EXPECTATION
        SET_EXPECTATION
    """
    rule_type = rule.get("type", "").upper()
    rule_id   = rule.get("rule_id", "unknown")

    # Common fields
    kwargs = {
        "name"       : rule_id,
        "description": rule.get("description", ""),
        "column"     : rule.get("column", ""),
        "dimension"  : rule.get("dimension", "VALIDITY"),
        "threshold"  : rule.get("threshold", 1.0),
        "ignore_null": rule.get("ignore_null", False),
    }

    # ── Type-specific expectation ──────────────────────────────────────────
    if rule_type == "NON_NULL_EXPECTATION":
        kwargs["non_null_expectation"] = (
            dataplex_v1.DataQualityRule.NonNullExpectation()
        )

    elif rule_type == "UNIQUENESS_EXPECTATION":
        kwargs["uniqueness_expectation"] = (
            dataplex_v1.DataQualityRule.UniquenessExpectation()
        )

    elif rule_type == "RANGE_EXPECTATION":
        kwargs["range_expectation"] = dataplex_v1.DataQualityRule.RangeExpectation(
            min_value           = str(rule["min_value"]) if "min_value" in rule else None,
            max_value           = str(rule["max_value"]) if "max_value" in rule else None,
            strict_min_enabled  = rule.get("strict_min_enabled", False),
            strict_max_enabled  = rule.get("strict_max_enabled", False),
        )

    elif rule_type == "REGEX_EXPECTATION":
        if "regex" not in rule:
            raise ValueError(f"Rule '{rule_id}': REGEX_EXPECTATION requires 'regex' field.")
        kwargs["regex_expectation"] = dataplex_v1.DataQualityRule.RegexExpectation(
            regex=rule["regex"]
        )

    elif rule_type == "SET_EXPECTATION":
        if "allowed_values" not in rule:
            raise ValueError(f"Rule '{rule_id}': SET_EXPECTATION requires 'allowed_values' list.")
        kwargs["set_expectation"] = dataplex_v1.DataQualityRule.SetExpectation(
            values=rule["allowed_values"]
        )

    else:
        raise ValueError(
            f"Rule '{rule_id}': Unknown rule type '{rule_type}'. "
            "Supported: NON_NULL_EXPECTATION, UNIQUENESS_EXPECTATION, "
            "RANGE_EXPECTATION, REGEX_EXPECTATION, SET_EXPECTATION"
        )

    return dataplex_v1.DataQualityRule(**kwargs)


# ═════════════════════════════════════════════════════════════════════════════
# 3.  CREATE OR UPDATE DATASCAN
# ═════════════════════════════════════════════════════════════════════════════

def upsert_datascan(
    client: dataplex_v1.DataScanServiceClient,
    config: dict,
    skip_create: bool = False,
) -> str:
    """
    Create the DataScan if it doesn't exist, or update it.
    Returns the fully-qualified DataScan resource name.
    """
    scan_cfg = config["datascan_config"]
    project  = scan_cfg["project_id"]
    location = scan_cfg["location"]
    scan_id  = scan_cfg["datascan_id"]
    parent   = f"projects/{project}/locations/{location}"
    name     = f"{parent}/dataScans/{scan_id}"

    dq_rules = [build_dataplex_rule(r) for r in config["rules"]]
    log.info(f"Built {len(dq_rules)} DataQualityRule objects.")

    data_scan = dataplex_v1.DataScan(
        name         = name,
        display_name = scan_cfg.get("display_name", scan_id),
        description  = scan_cfg.get("description", ""),
        data         = dataplex_v1.DataSource(
            resource=config["data_source"]["bigquery_table"]
        ),
        data_quality_spec=dataplex_v1.DataQualitySpec(
            rules             = dq_rules,
            sampling_percent  = config.get("sampling_percent", 100.0),
        ),
    )

    if skip_create:
        log.info("--skip-create flag set: assuming DataScan already exists.")
        return name

    # Try to get existing scan
    try:
        existing = client.get_data_scan(name=name)
        log.info(f"DataScan '{scan_id}' already exists — updating rules...")
        operation = client.update_data_scan(
            data_scan   = data_scan,
            update_mask = {"paths": ["data_quality_spec", "display_name", "description"]},
        )
        operation.result()   # wait for LRO
        log.info(f"DataScan updated: {name}")

    except google_exceptions.NotFound:
        log.info(f"DataScan '{scan_id}' not found — creating new...")
        operation = client.create_data_scan(
            parent       = parent,
            data_scan    = data_scan,
            data_scan_id = scan_id,
        )
        operation.result()   # wait for LRO
        log.info(f"DataScan created: {name}")

    return name


# ═════════════════════════════════════════════════════════════════════════════
# 4.  RUN DATASCAN JOB AND POLL
# ═════════════════════════════════════════════════════════════════════════════

TERMINAL_STATES = {
    dataplex_v1.DataScanJob.State.SUCCEEDED,
    dataplex_v1.DataScanJob.State.FAILED,
    dataplex_v1.DataScanJob.State.CANCELLED,
}

def run_and_poll(
    client   : dataplex_v1.DataScanServiceClient,
    scan_name: str,
    poll_interval: int = 30,
    timeout  : int = 1800,
) -> dataplex_v1.DataScanJob:
    """Trigger a DataScan run and poll until completion."""
    log.info(f"Triggering DataScan job for: {scan_name}")
    job_response = client.run_data_scan(name=scan_name)
    job_name     = job_response.job.name
    log.info(f"Job started: {job_name}")

    start  = time.time()
    dots   = 0

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            raise TimeoutError(
                f"DataScan job timed out after {timeout}s. "
                f"Job: {job_name}"
            )

        job = client.get_data_scan_job(
            name      = job_name,
            view      = dataplex_v1.GetDataScanJobRequest.DataScanJobView.FULL,
        )

        state_name = dataplex_v1.DataScanJob.State(job.state).name
        dots += 1
        log.info(
            f"  [{elapsed:>5.0f}s]  Job state: {state_name}"
            + ("." * (dots % 4))
        )

        if job.state in TERMINAL_STATES:
            log.info(f"Job reached terminal state: {state_name}")
            return job

        time.sleep(poll_interval)


# ═════════════════════════════════════════════════════════════════════════════
# 5.  PARSE AND LOG RESULTS
# ═════════════════════════════════════════════════════════════════════════════

def log_results(job: dataplex_v1.DataScanJob, rules_config: list) -> list:
    """
    Parse the job result, print a detailed pass/fail table, and return
    a list of result dicts (for optional BQ write).
    """
    # Build a lookup: rule_id -> original config row
    rule_lookup = {r["rule_id"]: r for r in rules_config}

    # Job-level outcome
    state_name = dataplex_v1.DataScanJob.State(job.state).name
    result_rows = []

    if job.state != dataplex_v1.DataScanJob.State.SUCCEEDED:
        log.error(f"Job did NOT succeed. Final state: {state_name}")
        if hasattr(job, "message"):
            log.error(f"  Message: {job.message}")
        return result_rows

    dq_result = job.data_quality_result
    passed    = dq_result.passed
    score     = dq_result.score  # 0.0 – 1.0

    log.info("")
    log.info("=" * 70)
    log.info(f"  DATA QUALITY RESULTS  —  Job: {job.name.split('/')[-1]}")
    log.info(f"  Overall Passed : {'✅ YES' if passed else '❌ NO'}")
    log.info(f"  Overall Score  : {score * 100:.2f}%")
    log.info("=" * 70)

    # Per-dimension summary
    for dim_result in dq_result.dimensions:
        dim_name   = dim_result.dimension if dim_result.dimension else "UNKNOWN"
        dim_passed = dim_result.passed
        log.info(
            f"  Dimension [{dim_name:<15}] : "
            f"{'✅ PASS' if dim_passed else '❌ FAIL'}"
        )

    log.info("-" * 70)
    log.info(f"  {'RULE ID':<40} {'STATUS':<8} {'PASS %':<10} {'ROWS PASS'}")
    log.info("-" * 70)

    timestamp = datetime.now(timezone.utc).isoformat()

    for rule_result in dq_result.rules:
        rule_name    = rule_result.rule.name     # maps to rule_id
        rule_passed  = rule_result.passed
        pass_ratio   = rule_result.pass_ratio    # 0.0 – 1.0
        evaluated    = rule_result.evaluated_count
        pass_count   = rule_result.passed_count
        null_count   = rule_result.null_count
        failing_rows = rule_result.failing_rows_query  # BQ query to inspect failures

        status_icon = "✅ PASS" if rule_passed else "❌ FAIL"
        log.info(
            f"  {rule_name:<40} {status_icon:<8} "
            f"{pass_ratio * 100:>6.2f}%   "
            f"{pass_count}/{evaluated}"
        )

        if not rule_passed:
            log.warning(f"    ↳ FAILING ROWS QUERY: {failing_rows}")
            if null_count:
                log.warning(f"    ↳ Null rows skipped  : {null_count}")

        original = rule_lookup.get(rule_name, {})
        result_rows.append({
            "job_id"            : job.name.split("/")[-1],
            "job_name"          : job.name,
            "run_timestamp"     : timestamp,
            "rule_id"           : rule_name,
            "rule_description"  : original.get("description", ""),
            "column"            : original.get("column", ""),
            "dimension"         : original.get("dimension", ""),
            "rule_type"         : original.get("type", ""),
            "threshold"         : original.get("threshold", 1.0),
            "passed"            : rule_passed,
            "pass_ratio"        : pass_ratio,
            "evaluated_count"   : evaluated,
            "passed_count"      : pass_count,
            "null_count"        : null_count,
            "failing_rows_query": failing_rows,
        })

    log.info("=" * 70)
    passed_count = sum(1 for r in result_rows if r["passed"])
    failed_count = len(result_rows) - passed_count
    log.info(f"  SUMMARY  —  {passed_count} passed  |  {failed_count} failed  |  {len(result_rows)} total")
    log.info("=" * 70)

    return result_rows


# ═════════════════════════════════════════════════════════════════════════════
# 6.  OPTIONAL — WRITE RESULTS TO BIGQUERY
# ═════════════════════════════════════════════════════════════════════════════

BQ_SCHEMA = [
    bigquery.SchemaField("job_id",             "STRING",  "REQUIRED"),
    bigquery.SchemaField("job_name",            "STRING",  "NULLABLE"),
    bigquery.SchemaField("run_timestamp",       "STRING",  "NULLABLE"),
    bigquery.SchemaField("rule_id",             "STRING",  "NULLABLE"),
    bigquery.SchemaField("rule_description",    "STRING",  "NULLABLE"),
    bigquery.SchemaField("column",              "STRING",  "NULLABLE"),
    bigquery.SchemaField("dimension",           "STRING",  "NULLABLE"),
    bigquery.SchemaField("rule_type",           "STRING",  "NULLABLE"),
    bigquery.SchemaField("threshold",           "FLOAT64", "NULLABLE"),
    bigquery.SchemaField("passed",              "BOOL",    "NULLABLE"),
    bigquery.SchemaField("pass_ratio",          "FLOAT64", "NULLABLE"),
    bigquery.SchemaField("evaluated_count",     "INTEGER", "NULLABLE"),
    bigquery.SchemaField("passed_count",        "INTEGER", "NULLABLE"),
    bigquery.SchemaField("null_count",          "INTEGER", "NULLABLE"),
    bigquery.SchemaField("failing_rows_query",  "STRING",  "NULLABLE"),
]

def write_results_to_bq(result_rows: list, output_cfg: dict) -> None:
    """Stream insert result rows into a BigQuery table."""
    project  = output_cfg["bq_project_id"]
    dataset  = output_cfg["bq_dataset_id"]
    table_id = output_cfg["bq_table_id"]
    full_id  = f"{project}.{dataset}.{table_id}"

    bq_client = bigquery.Client(project=project)

    # Auto-create table if it doesn't exist
    try:
        bq_client.get_table(full_id)
        log.info(f"BQ table exists: {full_id}")
    except google_exceptions.NotFound:
        log.info(f"BQ table not found — creating: {full_id}")
        table = bigquery.Table(full_id, schema=BQ_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(field=None)
        bq_client.create_table(table)
        log.info("BQ table created.")

    errors = bq_client.insert_rows_json(full_id, result_rows)
    if errors:
        log.error(f"BQ insert errors: {errors}")
        raise RuntimeError(f"Failed to write {len(errors)} rows to BigQuery.")
    else:
        log.info(f"✅ Wrote {len(result_rows)} result rows to {full_id}")


# ═════════════════════════════════════════════════════════════════════════════
# 7.  MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Run Dataplex CloudDQ checks from a JSON rules file."
    )
    parser.add_argument(
        "--rules", required=True,
        help="Path to JSON rules file (e.g. dq_rules.json)"
    )
    parser.add_argument(
        "--write-to-bq", action="store_true", default=False,
        help="Write rule-level results to BigQuery after the run"
    )
    parser.add_argument(
        "--skip-create", action="store_true", default=False,
        help="Skip DataScan create/update step (assumes it already exists)"
    )
    parser.add_argument(
        "--poll-interval", type=int, default=30,
        help="Seconds between job status polls (default: 30)"
    )
    parser.add_argument(
        "--timeout", type=int, default=1800,
        help="Max seconds to wait for job completion (default: 1800)"
    )
    args = parser.parse_args()

    # ── Step 1: Load config ────────────────────────────────────────────────
    config = load_config(args.rules)
    scan_cfg   = config["datascan_config"]
    output_cfg = config.get("output", {})

    # Honour --write-to-bq flag; also check JSON config
    write_to_bq = args.write_to_bq or output_cfg.get("write_to_bigquery", False)

    # ── Step 2: Build Dataplex client ──────────────────────────────────────
    # On Dataproc, ADC (Application Default Credentials) picks up the
    # cluster service account automatically — no key file needed.
    log.info("Initialising Dataplex DataScanService client (using ADC)...")
    ds_client = dataplex_v1.DataScanServiceClient()

    # ── Step 3: Create / update DataScan ──────────────────────────────────
    scan_name = upsert_datascan(ds_client, config, skip_create=args.skip_create)

    # ── Step 4: Trigger job and wait ───────────────────────────────────────
    job = run_and_poll(
        ds_client, scan_name,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
    )

    # ── Step 5: Parse and log results ──────────────────────────────────────
    result_rows = log_results(job, config["rules"])

    # ── Step 6: Optionally write to BQ ────────────────────────────────────
    if write_to_bq:
        if not result_rows:
            log.warning("No result rows to write (job may have failed).")
        elif not all(k in output_cfg for k in ("bq_project_id", "bq_dataset_id", "bq_table_id")):
            log.error(
                "write_to_bigquery=true but 'output' block is missing "
                "bq_project_id / bq_dataset_id / bq_table_id in JSON."
            )
        else:
            write_results_to_bq(result_rows, output_cfg)
    else:
        log.info("BQ write skipped (use --write-to-bq to enable).")

    # ── Exit code: non-zero if any rule failed ─────────────────────────────
    overall_passed = all(r["passed"] for r in result_rows) if result_rows else False
    exit_code = 0 if overall_passed else 1
    log.info(f"Exiting with code {exit_code}.")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()