#!/usr/bin/env python3
"""
Generate BigQuery INSERT SQL from a CSV file.

Usage:
    # single file
    python csv_to_bq_insert.py --csv data.csv --mapping mapping.txt --env bld

    # whole folder (every .csv inside)
    python csv_to_bq_insert.py --folder ./csvs --mapping mapping.txt --env bld

- mapping.txt maps a CSV file name to a table template, e.g.:
      headcount.csv = ap-peocon-<env>-01-<project_code>.ap_peocon_<env>_01_headcount.headcount

  Placeholders:
      <env>          -> replaced with the env passed via --env
      <project_code> -> replaced with the project_code for the matched project (peosta/peocur/peocon)

- env is passed as an argument. Allowed: bld, int, pre   (prd is intentionally NOT allowed)
- env and project_code values are hardcoded below.
"""

import argparse
import csv
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# HARDCODED CONFIG
# ---------------------------------------------------------------------------

# Allowed environments (prd intentionally excluded)
ALLOWED_ENVS = ["bld", "int", "pre"]

# Project -> project_code mapping
PROJECT_CODES = {
    "peosta": "1234",   # <-- replace with real project_code
    "peocur": "5678",   # <-- replace with real project_code
    "peocon": "9012",   # <-- replace with real project_code
}

# ---------------------------------------------------------------------------


def load_mapping(mapping_path):
    """
    Parse mapping.txt.
    Each non-empty, non-comment line:  <csv_name> = <table_template>
    Returns dict: { csv_name: table_template }
    """
    mapping = {}
    for raw in Path(mapping_path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            print(f"WARNING: skipping malformed mapping line: {raw}", file=sys.stderr)
            continue
        key, val = line.split("=", 1)
        mapping[key.strip()] = val.strip()
    return mapping


def detect_project(table_template):
    """Find which project (peosta/peocur/peocon) the template refers to."""
    for project in PROJECT_CODES:
        if project in table_template:
            return project
    return None


def resolve_table(table_template, env):
    """Replace <env> and <project_code> placeholders in the template."""
    project = detect_project(table_template)
    if project is None:
        raise ValueError(
            f"Could not detect project (peosta/peocur/peocon) in template: {table_template}"
        )
    project_code = PROJECT_CODES[project]
    resolved = table_template.replace("<env>", env).replace("<project_code>", project_code)
    return resolved


def sql_value(val):
    """Format a single CSV value for SQL."""
    if val is None or val == "":
        return "NULL"
    # numeric?
    if re.fullmatch(r"-?\d+", val) or re.fullmatch(r"-?\d+\.\d+", val):
        return val
    # boolean?
    if val.lower() in ("true", "false"):
        return val.upper()
    # string -> escape single quotes
    escaped = val.replace("'", "''")
    return f"'{escaped}'"


def generate_insert_sql(csv_path, table_name):
    """Build INSERT statements from a CSV file."""
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        raise ValueError(f"CSV file is empty: {csv_path}")

    header = rows[0]
    data_rows = rows[1:]
    columns = ", ".join(f"`{c.strip()}`" for c in header)

    statements = []
    for row in data_rows:
        # pad short rows
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        values = ", ".join(sql_value(v.strip() if v is not None else v) for v in row)
        statements.append(
            f"INSERT INTO `{table_name}` ({columns}) VALUES ({values});"
        )
    return statements


def process_one(csv_path, mapping, env, out_dir=None):
    """Process a single CSV. Returns (table_name, n_rows, out_path) or None if skipped."""
    csv_name = Path(csv_path).name
    if csv_name not in mapping:
        print(
            f"SKIP: '{csv_name}' not in mapping.txt. "
            f"Available keys: {list(mapping.keys())}",
            file=sys.stderr,
        )
        return None

    table_template = mapping[csv_name]
    table_name = resolve_table(table_template, env)
    statements = generate_insert_sql(csv_path, table_name)

    out_name = f"{Path(csv_path).stem}_{env}.sql"
    out_path = Path(out_dir) / out_name if out_dir else Path(out_name)
    out_path.write_text("\n".join(statements) + "\n", encoding="utf-8")

    print(f"  {csv_name} -> {table_name}  ({len(statements)} rows)  => {out_path}")
    return table_name, len(statements), str(out_path)


def main():
    parser = argparse.ArgumentParser(description="Generate BigQuery INSERT SQL from CSV.")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--csv", help="Path to a single input CSV file")
    src.add_argument("--folder", help="Path to a folder containing CSV files")
    parser.add_argument("--mapping", required=True, help="Path to mapping.txt")
    parser.add_argument("--env", required=True, help=f"Environment: {ALLOWED_ENVS}")
    parser.add_argument("--out", help="Output dir (folder mode) or .sql file (single mode)")
    args = parser.parse_args()

    env = args.env.lower()
    if env not in ALLOWED_ENVS:
        sys.exit(f"ERROR: env '{env}' not allowed. Choose from {ALLOWED_ENVS} (prd excluded).")

    mapping = load_mapping(args.mapping)

    if args.folder:
        folder = Path(args.folder)
        if not folder.is_dir():
            sys.exit(f"ERROR: '{folder}' is not a folder.")
        out_dir = args.out
        if out_dir:
            Path(out_dir).mkdir(parents=True, exist_ok=True)

        csv_files = sorted(folder.glob("*.csv"))
        if not csv_files:
            sys.exit(f"ERROR: no .csv files found in {folder}")

        print(f"Processing {len(csv_files)} CSV file(s) from {folder} for env={env}")
        done = 0
        for csv_file in csv_files:
            if process_one(csv_file, mapping, env, out_dir):
                done += 1
        print(f"Done: {done}/{len(csv_files)} file(s) generated.")
    else:
        # single file mode (preserves --out as a filename)
        csv_name = Path(args.csv).name
        if csv_name not in mapping:
            sys.exit(
                f"ERROR: '{csv_name}' not found in mapping.txt. "
                f"Available keys: {list(mapping.keys())}"
            )
        table_name = resolve_table(mapping[csv_name], env)
        statements = generate_insert_sql(args.csv, table_name)
        out_path = args.out or f"{Path(args.csv).stem}_{env}.sql"
        Path(out_path).write_text("\n".join(statements) + "\n", encoding="utf-8")
        print(f"Resolved table : {table_name}")
        print(f"Rows generated : {len(statements)}")
        print(f"Output written : {out_path}")


if __name__ == "__main__":
    main()