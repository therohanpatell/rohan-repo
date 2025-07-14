#!/usr/bin/env python3
"""
scd2_synthetic_data.py

This script generates synthetic SCD Type 2 data for a transactional table based on a provided JSON DDL.
The JSON DDL is expected to be a file containing an array of objects, each with keys:
  - "name": the column name
  - "type": the BigQuery data type (e.g., STRING, INTEGER, DATE, etc.)

Each generated row includes a partition column named "partition_dt" (of type DATE) used for partitioning.
If the schema does not include "partition_dt", it is automatically added.

Audit columns are populated as follows:
  - pipeline_execution_ingest_delete_flag: False
  - pipeline_execution_brand_id: "UNK"
  - pipeline_execution_ingest_map: a dynamic value based on the --ingest_map parameter.
  
The simulation supports two SCD2 modes:
  - scd2_full: Only a random subset of previous active records is processed (updated/unchanged) and the rest are omitted.
  - scd2_delta: All previous active records are processed; on random days some records are replaced by new rows.
  
Additionally, the script now supports custom partition modes via the --partition_mode parameter:
  - daily: Partition dates are consecutive days.
  - weekly: Partition dates are separated by 7-day gaps.
  - weekdays: Partition dates fall only on Monday–Friday.

Additional parameters:
  --ingest_map: Value for pipeline_execution_ingest_map (may contain the placeholder YYYYMMDD).
  --default_values: Comma-separated list of column:default_value pairs (e.g., purchase_price:0,other_column:default).
  --quote_cols: Comma-separated list of columns to force SQL quoting.
  --scd2_type: Either "scd2_full" or "scd2_delta".
  --seed: Optional random seed (for reproducibility).
  --partition_mode: "daily" (default), "weekly", or "weekdays".

Usage Example:
    python scd2.py --ddl schema.json --primary_key id --days 6 --records_per_day 20 --delimiter "|" \
      --target_table "myProject.myDataset.myTable" --quote_cols id,integer_field \
      --ingest_map "csv file_YYYYMMDD_test.csv" --default_values "bignumeric_field:12345.6789,string_field:test_string" \
      --scd2_type scd2_delta --partition_mode weekdays --seed 123
"""

import argparse
import json
import random
import string
import datetime
import sys
import os
import threading

# Thread-safe primary key counter
_pk_counter_lock = threading.Lock()
global_pk_counter = 1

# Audit columns default values.
audit_defaults = {
    "pipeline_execution_ingest_delete_flag": False,
    "pipeline_execution_brand_id": "UNK",
    "pipeline_execution_ingest_map": "csv file"  # Will be overridden by --ingest_map
}

# Global dictionary for default values for specific columns.
col_defaults = {}

def parse_json_schema(file_path):
    """Reads a JSON DDL file and returns a list of tuples (column_name, data_type)."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except FileNotFoundError:
        sys.exit(f"Error: Schema file '{file_path}' not found.")
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Schema file '{file_path}' is not valid JSON. {str(e)}")
    
    if not isinstance(schema, list) or len(schema) == 0:
        sys.exit("Error: Schema file must contain a non-empty array of field definitions.")
    
    columns = []
    for idx, field in enumerate(schema, start=1):
        if not isinstance(field, dict):
            sys.exit(f"Error: Field #{idx} in schema is not a JSON object.")
        if "name" not in field or "type" not in field:
            sys.exit(f"Error: Field #{idx} must contain both 'name' and 'type' keys.")
        columns.append((field["name"], field["type"].upper()))
    return columns

def get_normalized_type(data_type):
    """Returns a normalized type string."""
    int_types = ["INT64", "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT"]
    float_types = ["FLOAT64", "FLOAT", "DOUBLE"]
    bool_types = ["BOOL", "BOOLEAN"]
    date_types = ["DATE"]
    datetime_types = ["DATETIME"]
    timestamp_types = ["TIMESTAMP"]
    time_types = ["TIME"]
    numeric_types = ["NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"]

    if data_type in int_types:
        return "int"
    elif data_type in float_types:
        return "float"
    elif data_type in bool_types:
        return "bool"
    elif data_type in date_types:
        return "date"
    elif data_type in datetime_types:
        return "datetime"
    elif data_type in timestamp_types:
        return "timestamp"
    elif data_type in time_types:
        return "time"
    elif data_type in numeric_types:
        return "numeric"
    else:
        return "string"

def generate_random_string(length=8):
    """Generates a random string of letters."""
    return ''.join(random.choices(string.ascii_letters, k=length))

def generate_random_value(data_type, simulation_date, is_update=False, old_value=None, base_time=None):
    """Generates a random value for the given data type."""
    norm_type = get_normalized_type(data_type)
    if norm_type == "int":
        if is_update and old_value is not None:
            delta = random.randint(1, 10)
            return old_value + delta if random.choice([True, False]) else max(0, old_value - delta)
        else:
            return random.randint(100, 1000)
    elif norm_type in ["float", "numeric"]:
        if is_update and old_value is not None:
            delta = random.uniform(0.1, 10.0)
            return round(old_value + delta, 2) if random.choice([True, False]) else round(max(0, old_value - delta), 2)
        else:
            return round(random.uniform(10.0, 1000.0), 2)
    elif norm_type == "bool":
        if is_update and old_value is not None:
            return not old_value if random.random() < 0.5 else old_value
        else:
            return random.choice([True, False])
    elif norm_type == "string":
        if is_update and old_value is not None:
            new_part = generate_random_string(3)
            new_val = old_value + new_part
            return new_val if new_val != old_value else new_val + "X"
        else:
            return generate_random_string(10)
    elif norm_type == "date":
        return simulation_date
    elif norm_type in ["datetime", "timestamp"]:
        if base_time is None:
            base_time = datetime.time(0, 0, 0)
        new_val = datetime.datetime.combine(simulation_date, base_time)
        if is_update and old_value is not None:
            delta = datetime.timedelta(seconds=random.randint(1, 59))
            new_val += delta
        return new_val
    elif norm_type == "time":
        if base_time is None:
            base_time = datetime.time(0, 0, 0)
        if is_update and old_value is not None:
            dt = datetime.datetime.combine(datetime.date(2000, 1, 1), base_time)
            delta = datetime.timedelta(seconds=random.randint(1, 59))
            dt += delta
            return dt.time()
        return base_time
    else:
        return generate_random_string(10)

def format_sql_value(value, data_type):
    """Formats a Python value into a SQL literal."""
    norm_type = get_normalized_type(data_type)
    if norm_type == "string":
        return "'" + str(value).replace("'", "''") + "'"
    elif norm_type in ["int", "float", "numeric"]:
        return str(value)
    elif norm_type == "bool":
        return "TRUE" if value else "FALSE"
    elif norm_type == "date":
        if isinstance(value, datetime.date):
            return f"DATE '{value.strftime('%Y-%m-%d')}'"
        else:
            return f"DATE '{value}'"
    elif norm_type == "datetime":
        if isinstance(value, datetime.datetime):
            return f"DATETIME '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            return f"DATETIME '{value}'"
    elif norm_type == "timestamp":
        if isinstance(value, datetime.datetime):
            return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        else:
            return f"TIMESTAMP '{value}'"
    elif norm_type == "time":
        if isinstance(value, datetime.time):
            return f"TIME '{value.strftime('%H:%M:%S')}'"
        else:
            return f"TIME '{value}'"
    else:
        return "'" + str(value).replace("'", "''") + "'"

def format_csv_value(value):
    """Formats a Python value for CSV output."""
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(value, datetime.date):
        return value.strftime('%Y-%m-%d')
    elif isinstance(value, datetime.time):
        return value.strftime('%H:%M:%S')
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    else:
        return str(value)

def format_sql_value_with_quote_option(value, data_type, col_name, quote_cols):
    """
    Formats the SQL value and then, if the column is in quote_cols, always wraps it in single quotes.
    This ensures even numeric default columns in quote_cols are quoted.
    """
    formatted = format_sql_value(value, data_type)
    if col_name in quote_cols:
        if not (formatted.startswith("'") and formatted.endswith("'")):
            formatted = "'" + formatted + "'"
    return formatted

def convert_default_value(col, val, data_type):
    """Converts the default value string into the appropriate Python type."""
    norm_type = get_normalized_type(data_type)
    try:
        if norm_type == "int":
            return int(val)
        elif norm_type in ["float", "numeric"]:
            return float(val)
        elif norm_type == "bool":
            return val.lower() in ["true", "1", "yes"]
        elif norm_type == "date":
            return datetime.datetime.strptime(val, "%Y-%m-%d").date()
        elif norm_type in ["datetime", "timestamp"]:
            return datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
        elif norm_type == "time":
            return datetime.datetime.strptime(val, "%H:%M:%S").time()
        else:
            return val
    except Exception as e:
        return val

def generate_new_row(columns, simulation_date, primary_key, base_time):
    """Generates a new row for the given simulation_date."""
    global global_pk_counter, col_defaults
    row = {}
    for col, dtype in columns:
        if col in audit_defaults:
            row[col] = audit_defaults[col]
            continue
        if col == primary_key:
            norm_type = get_normalized_type(dtype)
            # Thread-safe primary key generation
            with _pk_counter_lock:
                value = global_pk_counter if norm_type == "int" else f"PK{global_pk_counter}"
                global_pk_counter += 1
            row[col] = value
        elif col == "partition_dt":
            row[col] = simulation_date
        else:
            if col in col_defaults:
                row[col] = convert_default_value(col, col_defaults[col], dtype)
            else:
                norm_type = get_normalized_type(dtype)
                if norm_type in ["date", "datetime", "timestamp", "time"]:
                    row[col] = generate_random_value(dtype, simulation_date, base_time=base_time)
                else:
                    row[col] = generate_random_value(dtype, simulation_date)
    return row

def generate_updated_row(columns, simulation_date, primary_key, old_row, base_time):
    """Generates an updated version of an existing row."""
    global col_defaults
    new_row = old_row.copy()
    non_key_cols = [col for col, _ in columns if col not in [primary_key, "partition_dt"] and col not in audit_defaults]
    update_cols = [col for col in non_key_cols if random.random() < 0.5]
    if not update_cols and non_key_cols:
        update_cols = [random.choice(non_key_cols)]
    for col in update_cols:
        dtype = next(dt for c, dt in columns if c == col)
        if col in col_defaults:
            new_row[col] = convert_default_value(col, col_defaults[col], dtype)
        else:
            old_value = old_row[col]
            if get_normalized_type(dtype) in ["date", "datetime", "timestamp", "time"]:
                new_value = generate_random_value(dtype, simulation_date, is_update=True, old_value=old_value, base_time=base_time)
            else:
                new_value = generate_random_value(dtype, simulation_date, is_update=True, old_value=old_value)
            new_row[col] = new_value
    for col in col_defaults:
        if col not in [primary_key, "partition_dt"] and col not in audit_defaults:
            dtype = next(dt for c, dt in columns if c == col)
            new_row[col] = convert_default_value(col, col_defaults[col], dtype)
    new_row["partition_dt"] = simulation_date
    return new_row

def get_weekday_partitions(n, end_date):
    """
    Generates a list of n partition dates that are weekdays (Monday to Friday), ending at end_date.
    The dates are returned in ascending order.
    """
    partitions = []
    current = end_date
    while len(partitions) < n:
        if current.weekday() < 5:  # Monday (0) to Friday (4)
            partitions.append(current)
        current -= datetime.timedelta(days=1)
    partitions.reverse()
    return partitions

def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic SCD Type 2 data based on a provided JSON DDL."
    )
    parser.add_argument("--ddl", type=str, required=True, help="Path to the JSON DDL file.")
    parser.add_argument("--primary_key", type=str, required=True, help="Primary key column name.")
    parser.add_argument("--days", type=int, required=True, help="Number of partitions (days/weeks/weekdays) to simulate.")
    parser.add_argument("--records_per_day", type=int, required=True, help="Records per partition.")
    parser.add_argument("--delimiter", type=str, required=True, help="Delimiter for CSV output.")
    parser.add_argument("--target_table", type=str, required=True, help="Target table in format ProjectID.DatasetID.tableID.")
    parser.add_argument("--quote_cols", type=str, default="", help="Comma-separated list of columns to force quoting.")
    parser.add_argument("--ingest_map", type=str, default=None, help="Value for pipeline_execution_ingest_map (may contain YYYYMMDD).")
    parser.add_argument("--default_values", type=str, default=None, help="Comma-separated column:default_value pairs.")
    parser.add_argument("--scd2_type", type=str, default="scd2_full", help="Either 'scd2_full' or 'scd2_delta'.")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducibility.")
    parser.add_argument("--partition_mode", type=str, default="daily",
                        help="Partition mode: 'daily' (default), 'weekly', or 'weekdays'.")
    args = parser.parse_args()

    # Set random seed if provided (B1)
    if args.seed is not None:
        random.seed(args.seed)

    # Validate scd2_type (B2)
    scd2_type = args.scd2_type.lower()
    if scd2_type not in ["scd2_full", "scd2_delta"]:
        sys.exit("Error: --scd2_type must be either 'scd2_full' or 'scd2_delta'.")

    # Validate partition_mode
    partition_mode = args.partition_mode.lower()
    if partition_mode not in ["daily", "weekly", "weekdays"]:
        sys.exit("Error: --partition_mode must be 'daily', 'weekly', or 'weekdays'.")

    # Parse quote_cols.
    quote_cols = set(col.strip() for col in args.quote_cols.split(',')) if args.quote_cols else set()

    # Override audit_defaults["pipeline_execution_ingest_map"] if provided.
    if args.ingest_map:
        audit_defaults["pipeline_execution_ingest_map"] = args.ingest_map

    # Parse default_values.
    default_values_dict = {}
    if args.default_values:
        for pair in args.default_values.split(','):
            if ':' in pair:
                key, val = pair.split(':', 1)
                default_values_dict[key.strip()] = val.strip()
    global col_defaults
    col_defaults = default_values_dict

    # Parse JSON schema with robust validation (B3).
    columns = parse_json_schema(args.ddl)
    schema_dict = { col: dtype for col, dtype in columns }

    # Ensure partition_dt exists.
    if "partition_dt" not in schema_dict:
        columns.append(("partition_dt", "DATE"))
        schema_dict["partition_dt"] = "DATE"

    primary_key = args.primary_key
    if primary_key not in schema_dict:
        sys.exit(f"Error: Primary key column '{primary_key}' not found in schema.")

    num_partitions = args.days
    records_per_partition = args.records_per_day

    today = datetime.date.today()

    # Compute partition dates based on partition_mode.
    if partition_mode == "daily":
        start_date = today - datetime.timedelta(days=num_partitions)
        partition_dates = [start_date + datetime.timedelta(days=i) for i in range(num_partitions)]
    elif partition_mode == "weekly":
        start_date = today - datetime.timedelta(weeks=num_partitions)
        partition_dates = [start_date + datetime.timedelta(weeks=i) for i in range(num_partitions)]
    else:  # weekdays mode
        # Use yesterday as end_date (mimicking current behavior, which doesn't use today)
        end_date = today - datetime.timedelta(days=1)
        partition_dates = get_weekday_partitions(num_partitions, end_date)

    base_time = datetime.datetime.now().time()
    active_set = {}
    all_rows = []
    summary_lines = []
    insert_sql_scripts = []

    # In delta mode, define replacement probability and max replacement count.
    replacement_probability = 0.3
    max_replacement = max(1, int(0.2 * records_per_partition))

    # Main simulation loop using partition_dates.
    for i, simulation_date in enumerate(partition_dates, start=1):
        summary_lines.append(f"Partition {i} (partition_dt = {simulation_date.strftime('%Y-%m-%d')}):")
        daily_rows = []

        if i == 1:
            new_keys = []
            for j in range(records_per_partition):
                row = generate_new_row(columns, simulation_date, primary_key, base_time)
                active_set[row[primary_key]] = row
                daily_rows.append(row)
                new_keys.append(str(row[primary_key]))
            summary_lines.append("  New records: " + ", ".join(new_keys))
        else:
            prev_active_keys = list(active_set.keys())
            num_prev = len(prev_active_keys)
            if scd2_type == "scd2_full":
                processed_count = random.randint(1, min(num_prev, records_per_partition)) if num_prev > 0 else 0
                new_count = records_per_partition - processed_count
                processed_keys = random.sample(prev_active_keys, processed_count) if processed_count > 0 else []
                omitted_keys = [str(pk) for pk in prev_active_keys if pk not in processed_keys]
            else:  # scd2_delta mode
                if num_prev <= records_per_partition:
                    initial_processed_count = num_prev
                    new_count = records_per_partition - num_prev
                else:
                    initial_processed_count = records_per_partition
                    new_count = 0
                processed_keys = list(prev_active_keys)
                if num_prev > records_per_partition:
                    processed_keys = random.sample(prev_active_keys, initial_processed_count)
                omitted_keys = []  # No omissions in delta mode.
                if random.random() < replacement_probability and initial_processed_count > 0:
                    replacement_count = random.randint(1, min(initial_processed_count, max_replacement))
                    final_processed_count = initial_processed_count - replacement_count
                    final_new_count = new_count + replacement_count
                else:
                    final_processed_count = initial_processed_count
                    final_new_count = new_count
                if final_processed_count > 0:
                    processed_keys = random.sample(processed_keys, final_processed_count)
                else:
                    processed_keys = []
                processed_count = final_processed_count
                new_count = final_new_count

            processed_rows = []
            summary_updated = []
            summary_unchanged = []
            for pk in processed_keys:
                old_row = active_set[pk]
                if random.random() < 0.5:
                    new_row = generate_updated_row(columns, simulation_date, primary_key, old_row, base_time)
                    processed_rows.append(new_row)
                    diffs = []
                    for col, dtype in columns:
                        if col in [primary_key, "partition_dt"] or col in audit_defaults:
                            continue
                        if old_row[col] != new_row[col]:
                            diffs.append(f"{col}: {old_row[col]} -> {new_row[col]}")
                    summary_updated.append(f"PK {pk}: " + "; ".join(diffs))
                else:
                    new_row = old_row.copy()
                    new_row["partition_dt"] = simulation_date
                    processed_rows.append(new_row)
                    summary_unchanged.append(str(pk))
            new_rows = []
            new_keys = []
            for j in range(new_count):
                row = generate_new_row(columns, simulation_date, primary_key, base_time)
                new_rows.append(row)
                new_keys.append(str(row[primary_key]))
            daily_rows = processed_rows + new_rows
            if new_keys:
                summary_lines.append("  New records: " + ", ".join(new_keys))
            if summary_updated:
                summary_lines.append("  Updated records:")
                for line in summary_updated:
                    summary_lines.append("    " + line)
            if summary_unchanged:
                summary_lines.append("  Unchanged records: " + ", ".join(summary_unchanged))
            if scd2_type == "scd2_full" and omitted_keys:
                summary_lines.append("  Omitted records: " + ", ".join(omitted_keys))
            new_active_set = {}
            for row in processed_rows:
                new_active_set[row[primary_key]] = row
            for row in new_rows:
                new_active_set[row[primary_key]] = row
            active_set = new_active_set

        for row in daily_rows:
            row["partition_dt"] = simulation_date
            template = audit_defaults["pipeline_execution_ingest_map"]
            if "YYYYMMDD" in template:
                dynamic_filename = template.replace("YYYYMMDD", simulation_date.strftime("%Y%m%d"))
            else:
                dynamic_filename = f"{template}_{simulation_date.strftime('%Y%m%d')}.csv"
            row["pipeline_execution_ingest_map"] = dynamic_filename

        col_names = [col for col, _ in columns]
        values_list = []
        for row in daily_rows:
            formatted_values = [
                format_sql_value_with_quote_option(row[col], schema_dict[col], col, quote_cols)
                for col in col_names
            ]
            values_list.append("(" + ", ".join(formatted_values) + ")")
        insert_stmt = (
            f"-- Partition {i} (partition_dt = {simulation_date.strftime('%Y-%m-%d')})\n"
            f"INSERT INTO {args.target_table} ({', '.join(col_names)}) VALUES\n"
            + ",\n".join(values_list)
            + ";\n"
        )
        insert_sql_scripts.append(insert_stmt)
        all_rows.extend(daily_rows)
        summary_lines.append("")

    try:
        with open("inserts.sql", "w", encoding="utf-8") as f:
            for stmt in insert_sql_scripts:
                f.write(stmt + "\n")
    except (IOError, PermissionError) as e:
        sys.exit(f"Error writing 'inserts.sql': {str(e)}")

    try:
        with open("output.csv", "w", newline="", encoding="utf-8") as f:
            header = args.delimiter.join([col for col, _ in columns])
            f.write(header + "\n")
            for row in all_rows:
                line = args.delimiter.join([format_csv_value(row[col]) for col, _ in columns])
                f.write(line + "\n")
    except (IOError, PermissionError) as e:
        sys.exit(f"Error writing 'output.csv': {str(e)}")

    try:
        with open("summary.txt", "w", encoding="utf-8") as f:
            for line in summary_lines:
                f.write(line + "\n")
    except (IOError, PermissionError) as e:
        sys.exit(f"Error writing 'summary.txt': {str(e)}")

if __name__ == "__main__":
    main()
