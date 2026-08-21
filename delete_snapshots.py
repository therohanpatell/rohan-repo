#!/usr/bin/env python3
"""delete_snapshots.py

Delete one or more BigQuery table snapshots created by backup_and_validate.py.

The single most important guarantee here: this script refuses to delete anything
whose table_type is not SNAPSHOT. A typo naming a production table is rejected,
not executed.

Snapshots also expire on their own (--retention-days on the backup script,
default 7 days), so this is only needed to clean up early.

Exit code 0 -> every requested snapshot was deleted (or already absent)
Exit code 1 -> at least one was refused, failed, or the run was cancelled

Usage
-----
# preview only
python delete_snapshots.py --dry-run \\
    --snapshots proj.ds.customer__snapshot__20260821_010530_vw \\
                proj.ds.orders__snapshot__20260821_010530_vw

# delete, with an interactive confirmation
python delete_snapshots.py --snapshots proj.ds.customer__snapshot__20260821_010530_vw

# non-interactive (Dataproc / Composer)
python delete_snapshots.py --yes --snapshots proj.ds.a__snapshot__20260821_010530_vw
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Sequence

from google.api_core import exceptions as gexc
from google.cloud import bigquery

__version__ = "2026.08.21.4"
LOG = logging.getLogger("bq.delete")

IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$")
PROJECT_RE = re.compile(r"^(?:[a-z][a-z0-9.-]{0,62}:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$")
TABLE_RE = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")

LINE = "=" * 50


class DeleteError(RuntimeError):
    """Fatal condition for a single snapshot."""


def split_table_ref(ref: str) -> tuple[str, str, str]:
    """Parse and validate 'project.dataset.table'."""
    parts = ref.strip().split(".")
    if len(parts) != 3:
        raise DeleteError(f"Invalid reference {ref!r}: expected project_id.dataset_id.table_name")
    project, dataset, table = parts
    if not PROJECT_RE.match(project):
        raise DeleteError(f"Invalid project id {project!r}")
    if not IDENT_RE.match(dataset):
        raise DeleteError(f"Invalid dataset id {dataset!r}")
    if not TABLE_RE.match(table):
        raise DeleteError(f"Invalid table name {table!r}")
    return project, dataset, table


def delete_one(
    client: bigquery.Client, snapshot_id: str, dry_run: bool, allow_expired: bool
) -> str:
    """Return a short status word: DELETED / WOULD DELETE / ABSENT."""
    try:
        table = client.get_table(snapshot_id)
    except gexc.NotFound:
        LOG.info("%s does not exist - nothing to do", snapshot_id)
        return "ABSENT"
    except gexc.Forbidden as exc:
        raise DeleteError(f"Access denied: {exc}") from exc

    # The guardrail that matters: never delete a real table.
    if table.table_type != "SNAPSHOT":
        raise DeleteError(
            f"REFUSED - {snapshot_id} has table_type={table.table_type}, not SNAPSHOT. "
            "This script only deletes snapshots."
        )

    base = None
    snap_def = getattr(table, "snapshot_definition", None)
    if snap_def and snap_def.base_table_reference:
        base = snap_def.base_table_reference.path
    expires = table.expires

    LOG.info(
        "%s rows=%s base=%s expires=%s",
        snapshot_id, f"{table.num_rows:,}" if table.num_rows is not None else "?", base, expires,
    )

    if expires and expires <= datetime.now(timezone.utc) and not allow_expired:
        LOG.warning("%s already expired at %s; BigQuery will remove it", snapshot_id, expires)

    if dry_run:
        return "WOULD DELETE"

    client.delete_table(snapshot_id, not_found_ok=True)
    LOG.info("deleted %s", snapshot_id)
    return "DELETED"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Delete BigQuery table snapshots. Refuses anything that is not a snapshot.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--snapshots",
        nargs="+",
        required=True,
        metavar="PROJECT.DATASET.SNAPSHOT",
        help="One or more fully qualified snapshot tables",
    )
    p.add_argument("--dry-run", action="store_true", help="Show what would be deleted, delete nothing")
    p.add_argument("--yes", action="store_true", help="Skip the interactive confirmation")
    p.add_argument("--allow-expired", action="store_true", help="Suppress the already-expired warning")
    p.add_argument("--location")
    p.add_argument("--billing-project")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        stream=sys.stderr,
    )
    LOG.info("%s version=%s", __file__, __version__)

    try:
        refs = [split_table_ref(r) for r in args.snapshots]
    except DeleteError as exc:
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return 1

    if not args.dry_run and not args.yes:
        print(f"About to permanently delete {len(refs)} snapshot(s):", file=sys.stderr)
        for ref in refs:
            print(f"  {'.'.join(ref)}", file=sys.stderr)
        try:
            answer = input("Type 'delete' to confirm: ").strip().lower()
        except EOFError:
            answer = ""
        if answer != "delete":
            print("Cancelled. Nothing was deleted.", file=sys.stderr)
            return 1

    results: list[tuple[str, str]] = []
    for ref in refs:
        snapshot_id = ".".join(ref)
        client = bigquery.Client(
            project=args.billing_project or ref[0], location=args.location
        )
        try:
            status = delete_one(client, snapshot_id, args.dry_run, args.allow_expired)
        except DeleteError as exc:
            LOG.error("%s", exc)
            status = f"REFUSED: {exc}"
        except gexc.GoogleAPIError as exc:
            LOG.error("[%s] BigQuery API error: %s", snapshot_id, exc)
            status = f"ERROR: {exc}"
        results.append((snapshot_id, status))

    width = max(len(s) for s, _ in results)
    print("")
    print(LINE)
    print("SNAPSHOT DELETION" + (" (DRY RUN)" if args.dry_run else ""))
    print(LINE)
    print("")
    ok_states = {"DELETED", "WOULD DELETE", "ABSENT"}
    for snapshot_id, status in results:
        print(f"{snapshot_id:<{width}}  {status}")

    failed = [s for _, s in results if s not in ok_states]
    print("")
    print(f"Requested  {len(results)}")
    print(f"Succeeded  {len(results) - len(failed)}")
    print(f"Failed     {len(failed)}")
    print("")
    print(LINE)
    print(f"RESULT: {'FAIL' if failed else 'PASS'}")
    print(LINE)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
