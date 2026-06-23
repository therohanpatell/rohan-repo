# Skill: Synthetic Data Generator for BigQuery Source Tables

## Purpose
Generate realistic synthetic data as CSV files from one or more BigQuery JSON table schemas. When multiple schemas are attached, infer and honor referential integrity between them. Output **one CSV per JSON schema**, ready for direct ingestion into BigQuery source (staging) tables.

---

## Inputs You Will Receive
1. **One or more BigQuery table schema JSON files** (the standard `bq show --schema --format=prettyjson` output, i.e. an array of field objects with `name`, `type`, `mode`, and optionally `description` / nested `fields`).
2. **A free-text instruction** from the user, e.g.:
   - "generate synthetic data" → default volume.
   - "scd 2 data" → generate **5 days of history, 5 rows total** following SCD Type 2 conventions (see SCD section).
   - "100 rows", "edge cases", "nulls allowed", etc. → adjust volume/behavior accordingly.

These tables are **SOURCE tables in a data pipeline** (the landing/source layer). Generate data as a source system would produce it — raw, business-keyed, not yet conformed.

---

## Core Rules

### 1. One CSV per JSON
For each attached schema, produce a separate CSV. Name each file `<table_name>.csv` (derive table name from the JSON filename or the user's hint).

### 2. Type mapping (BigQuery → CSV value)
| BQ Type | Generated value format |
|---|---|
| STRING | Realistic text appropriate to the field name |
| INTEGER / INT64 | Whole numbers in a sensible range |
| FLOAT / FLOAT64 / NUMERIC / BIGNUMERIC | Decimals, respect scale if implied by name (amount, rate, pct) |
| BOOLEAN / BOOL | `true` / `false` (lowercase) |
| DATE | `YYYY-MM-DD` |
| DATETIME | `YYYY-MM-DD HH:MM:SS` |
| TIMESTAMP | `YYYY-MM-DD HH:MM:SS UTC` (UTC, microsecond optional) |
| TIME | `HH:MM:SS` |
| BYTES | Base64 string |
| RECORD/STRUCT (nested) | Flatten to JSON string in the cell, or dot-notation columns — default to JSON string |
| REPEATED (mode) | JSON array string, e.g. `["a","b"]` |

### 3. Mode handling
- `REQUIRED` → never null.
- `NULLABLE` → mostly populated; sprinkle a few nulls (~5–10%) **only if** the user allows nulls or asks for edge cases. Otherwise populate.
- `REPEATED` → JSON array string.

### 4. Field-name–aware generation (be smart)
Infer realistic values from the column name:
- `*_id`, `*_key`, `*_no` → identifiers (sequential or UUID-style; keep consistent for joins).
- `email` → `name@example.com`. `phone` → valid-looking numbers.
- `first_name`/`last_name`/`name` → realistic names.
- `*_date`, `*_dt` → dates; `*_ts`, `*_timestamp` → timestamps.
- `amount`, `salary`, `price`, `cost` → currency-scale decimals.
- `*_pct`, `rate`, `ratio` → 0–100 or 0–1 as appropriate.
- `status`, `type`, `category`, `code` → small realistic enum sets (e.g. status: `ACTIVE`/`INACTIVE`/`PENDING`).
- `country`, `currency`, `city`, `dept`/`department` → real-world values.
- `flag`, `is_*`, `has_*` → boolean-ish.
Use the field `description` if present — it is the strongest hint.

### 5. Realism & distribution
- Vary values; don't repeat the same row.
- Use plausible business distributions (most rows "normal", a few outliers).
- Keep referential and intra-row logical consistency (e.g. `end_date >= start_date`, `updated_ts >= created_ts`).

---

## Referential Integrity (multiple schemas)
When 3–4 schemas are attached, treat them as a related set:

1. **Detect relationships** by matching column names across tables:
   - A column like `customer_id` in `orders` that matches the PK `customer_id` (or `id`) in `customers` is a foreign key.
   - Match on `<entity>_id`, `<entity>_key`, exact-name PK/FK, and description hints.
2. **Generate parents first**, then children. Child FK values must be **drawn only from the parent's generated key pool** — never invent unmatched keys.
3. Maintain cardinality sensibly: each parent gets 0..N children; not every parent needs children, but every child needs a valid parent.
4. Keep the **same key value identical** across all CSVs so the user can load and join without orphans.
5. If you can't confidently determine a relationship, state your assumption in chat before generating.

**Always print a short "Inferred relationships" summary in chat** (e.g. `orders.customer_id → customers.customer_id`) so the user can correct you.

---

## SCD Type 2 Mode
Triggered when the user says "scd 2" / "scd type 2" / "scd2".

Default for SCD2 request: **5 distinct business days, 5 rows total** (i.e. a small set of business keys whose changes are tracked across those days).

Generate SCD2-shaped history:
- Pick a few **business keys** (e.g. 2–3 distinct entities) and show changes to a tracked attribute over the 5 days, producing 5 rows total.
- Standard SCD2 columns (generate them even if not in the source schema **only if** the table is meant to hold SCD2 — otherwise SCD2 is usually applied downstream; for a **source** table, generate the raw changing rows by `effective/business date` and let the user's pipeline build SCD2). Default behavior:
  - If the schema already contains SCD2 columns (`effective_start_date`/`effective_end_date`/`is_current`/`valid_from`/`valid_to`/`version`), populate them correctly:
    - First version: `effective_start_date` = day 1, `effective_end_date` = day before next change (or `9999-12-31`), `is_current` = false for superseded, `true` for latest.
    - Increment `version` per business key.
    - Exactly one `is_current = true` per business key.
  - If the schema does **not** contain SCD2 columns (typical for source): generate 5 rows where the same business key appears on different days with a changed attribute and a `change_date`/`updated_ts` reflecting the change. Note this in chat.
- Ensure chronological consistency: no overlapping effective ranges per key; ranges are contiguous.

If the user wants a different volume ("scd2 10 days 20 rows"), follow their numbers; the 5/5 is only the default.

---

## Audit Columns Section
Source tables in a pipeline usually need audit/lineage columns appended on ingestion. **By default, do NOT fabricate business values for audit columns** — generate them as a clearly separated block the user controls.

### Behavior
- Detect audit columns in the schema by common names:
  `ingestion_timestamp`, `load_ts`, `load_date`, `created_by`, `created_ts`, `updated_by`, `updated_ts`, `source_system`, `source_file_name`, `batch_id`, `record_hash`, `dml_type`, `etl_run_id`, `valid_flag`, `_loaded_at`.
- For these, fill with **safe, consistent placeholder defaults** (not random business data):
  - timestamps → a single fixed load timestamp for the whole batch (one run = one timestamp).
  - `source_system` → e.g. `SOURCE_SYS` (or user-provided).
  - `source_file_name` → the CSV file name.
  - `batch_id` / `etl_run_id` → one constant per batch (e.g. `BATCH_20240101_0001`).
  - `record_hash` → leave blank or note it should be computed at load.
  - `dml_type` → `I` (insert) for new source rows.
- Keep audit columns as the **last columns** in the CSV, grouped together, so they're easy to find and overwrite.

### How you (the user) update audit columns yourself — explained in chat
After I generate the CSV, you control audit columns in any of these ways:

1. **Edit the CSV directly** — the audit columns are the last N columns; replace the placeholder values (e.g. set the real `batch_id`, real `load_ts`).
2. **Override at generation time** — tell me in chat, e.g.:
   - "set source_system = WORKDAY, batch_id = B20240115, load_ts = 2024-01-15T02:00:00Z" and I'll bake those constants in.
3. **Set them in BigQuery at load** — preferred for real pipelines. Leave the audit columns blank/default and populate on insert:
   ```sql
   INSERT INTO `project.dataset.source_table`
   SELECT
     * EXCEPT(load_ts, batch_id, source_system, dml_type),
     CURRENT_TIMESTAMP()        AS load_ts,
     'B20240115'                AS batch_id,
     'WORKDAY'                  AS source_system,
     'I'                        AS dml_type
   FROM external_or_staged_csv;
   ```
   Or use a load-time default / a MERGE that stamps `ingestion_timestamp = CURRENT_TIMESTAMP()`.
4. **Compute hash at load** — if `record_hash` exists:
   ```sql
   TO_HEX(SHA256(TO_JSON_STRING(STRUCT(col1, col2, col3)))) AS record_hash
   ```

I will always tell you in chat **which columns I treated as audit columns and what placeholder I used**, so you know exactly what to overwrite.

---

## CSV Output Conventions
- **Header row required**, column names exactly matching the schema field names, in schema order (audit columns last).
- Comma-delimited, UTF-8.
- Quote any field containing a comma, quote, or newline; escape inner quotes by doubling (`""`).
- Nulls → empty field (no `NULL` literal), so BigQuery loads them as NULL.
- Booleans lowercase `true`/`false`.
- Dates/timestamps in BigQuery-loadable formats (see type table).
- Nested/repeated → valid JSON strings (load with `--json` understanding, or note that the column is JSON-typed).

---

## Output Workflow (what to do each time)
1. Read every attached JSON schema.
2. Print in chat:
   - Tables detected.
   - **Inferred relationships** (FK → PK), with assumptions flagged.
   - Audit columns detected + placeholder values used.
   - Row counts / mode (default vs SCD2 vs user-specified).
3. Generate parents → children honoring referential integrity.
4. Output **one CSV per schema** (full CSV content in a code block, or as a downloadable file), named `<table_name>.csv`.
5. End with a one-line note on how to load each CSV into BigQuery (`bq load --source_format=CSV --skip_leading_rows=1 ...`) and a reminder of which columns the user should overwrite for audit.

---

## Defaults Summary
| Setting | Default |
|---|---|
| Rows per table (normal) | 20 (override on request) |
| SCD2 | 5 days, 5 rows |
| Nulls | none unless NULLABLE + user allows / edge-case mode |
| Audit columns | placeholder constants, grouped last, user-overridable |
| Output | one CSV per JSON, header included, BigQuery-loadable |
| Relationships | auto-inferred, parents before children, no orphans |

Always favor data that is realistic, consistent, joinable, and directly loadable.
