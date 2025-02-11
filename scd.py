#!/usr/bin/env python3
"""
scd2_synthetic_data.py

This script generates synthetic SCD Type 2 data for a transactional table based on a provided JSON DDL.
The JSON DDL is expected to be a file containing an array of objects, each with keys including:
  - "name": the column name
  - "type": the BigQuery data type (e.g., STRING, INTEGER, DATE, etc.)

Each generated row will include a partition column named "partition_dt" (of type DATE) used for partitioning.
If the provided schema does not include "partition_dt", it is automatically added.

Audit columns in the schema are populated as follows (for every record):
  - pipeline_execution_ingest_delete_flag: False
  - pipeline_execution_brand_id: "UNK"
  - pipeline_execution_ingest_map: "csv file_YYYYMMDD_test.csv"
    (The substring "YYYYMMDD" will be replaced with the simulation date.)

The simulation uses SCD Type 2 logic. For day 1, all rows are new.
For subsequent days, a random subset of yesterday’s active rows is processed.
For each processed record an action is randomly chosen:
  - updated – a new version is generated with one or more columns modified,
  - unchanged – the row is simply reinserted with the current day’s partition_dt.
Records not selected for processing (omitted) do not appear in the current day’s output.
New rows are generated as needed so that exactly the specified number of rows are inserted each day.

A new command‑line argument --quote_cols accepts a comma‑separated list of column names.
For any column in this list, its value is forced to appear within single quotes in the generated INSERT statements,
even if its native data type would normally not use quotes.

Usage example:
    python scd2_synthetic_data.py --ddl schema.json --primary_key country_code --days 5 --records_per_day 5 --delimiter "|" --target_table "myProject.myDataset.myTable" --quote_cols purchase_price,another_column
"""

import argparse
import json
import random
import string
import datetime

# Global primary key counter (used to generate unique PK values)
global_pk_counter = 1

# Audit columns default values.
# Note: The "pipeline_execution_ingest_map" value is defined as a template.
audit_defaults = {
    "pipeline_execution_ingest_delete_flag": False,
    "pipeline_execution_brand_id": "UNK",
    "pipeline_execution_ingest_map": "csv file_YYYYMMDD_test.csv"
}

def parse_json_schema(file_path):
    """
    Reads a JSON DDL file and returns a list of tuples (column_name, data_type).
    """
    with open(file_path, "r") as f:
        schema = json.load(f)
    columns = [(field["name"], field["type"].upper()) for field in schema]
    return columns

def get_normalized_type(data_type):
    """
    Normalizes BigQuery data types to one of: int, float, bool, date, datetime, timestamp, time, numeric, or string.
    """
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
    """
    Generates a random value for the given data type.
    For date/time types, a deterministic value is derived from simulation_date and base_time.
    """
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
    """
    Formats a Python value into a BigQuery SQL literal.
    Numeric values are unquoted, strings and date/time types are formatted with appropriate quotes.
    """
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
    Wraps the formatted SQL value in single quotes if the column name is in the quote_cols set.
    """
    formatted = format_sql_value(value, data_type)
    if col_name in quote_cols:
        # If not already quoted (i.e. for numeric values), add quotes.
        if not (formatted.startswith("'") and formatted.endswith("'")):
            return "'" + formatted + "'"
    return formatted

def generate_new_row(columns, simulation_date, primary_key, base_time):
    """
    Generates a new row (a dictionary) for the given simulation_date.
    Audit columns receive constant values initially. The primary key is generated uniquely.
    """
    global global_pk_counter
    row = {}
    for col, dtype in columns:
        # Set audit columns to their default values.
        if col in audit_defaults:
            row[col] = audit_defaults[col]
            continue
        if col == primary_key:
            norm_type = get_normalized_type(dtype)
            value = global_pk_counter if norm_type == "int" else f"PK{global_pk_counter}"
            global_pk_counter += 1
            row[col] = value
        elif col == "partition_dt":
            row[col] = simulation_date
        else:
            norm_type = get_normalized_type(dtype)
            if norm_type in ["date", "datetime", "timestamp", "time"]:
                row[col] = generate_random_value(dtype, simulation_date, base_time=base_time)
            else:
                row[col] = generate_random_value(dtype, simulation_date)
    return row

def generate_updated_row(columns, simulation_date, primary_key, old_row, base_time):
    """
    Generates an updated version of an existing row.
    Only a subset of non‑key, non‑partition_dt, and non‑audit columns are modified.
    """
    new_row = old_row.copy()
    non_key_cols = [col for col, _ in columns if col not in [primary_key, "partition_dt"] and col not in audit_defaults]
    update_cols = [col for col in non_key_cols if random.random() < 0.5]
    if not update_cols:
        update_cols = [random.choice(non_key_cols)]
    for col in update_cols:
        dtype = next(dt for c, dt in columns if c == col)
        old_value = old_row[col]
        if get_normalized_type(dtype) in ["date", "datetime", "timestamp", "time"]:
            new_value = generate_random_value(dtype, simulation_date, is_update=True, old_value=old_value, base_time=base_time)
        else:
            new_value = generate_random_value(dtype, simulation_date, is_update=True, old_value=old_value)
        new_row[col] = new_value
    new_row["partition_dt"] = simulation_date
    return new_row

def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic SCD Type 2 data for a transactional table based on a provided JSON DDL."
    )
    parser.add_argument("--ddl", type=str, required=True,
                        help="Path to the JSON DDL file containing the table schema.")
    parser.add_argument("--primary_key", type=str, required=True,
                        help="Name of the primary key column (single column).")
    parser.add_argument("--days", type=int, required=True,
                        help="Number of days for which data should be generated.")
    parser.add_argument("--records_per_day", type=int, required=True,
                        help="Number of records per day in the INSERT scripts.")
    parser.add_argument("--delimiter", type=str, required=True,
                        help="Custom delimiter to use for the CSV output.")
    parser.add_argument("--target_table", type=str, required=True,
                        help="Target table in the format ProjectID.DatasetID.tableID for the INSERT scripts.")
    parser.add_argument("--quote_cols", type=str, default="",
                        help="Comma-separated list of column names to force enclosing values in single quotes in INSERT scripts.")
    args = parser.parse_args()

    # Parse the quote_cols option into a set of column names.
    quote_cols = set(col.strip() for col in args.quote_cols.split(',')) if args.quote_cols else set()

    # Parse the JSON schema.
    columns = parse_json_schema(args.ddl)
    schema_dict = { col: dtype for col, dtype in columns }

    # Ensure the partition column exists; if not, add it.
    if "partition_dt" not in schema_dict:
        columns.append(("partition_dt", "DATE"))
        schema_dict["partition_dt"] = "DATE"

    # Validate the primary key exists.
    primary_key = args.primary_key
    if primary_key not in schema_dict:
        raise ValueError(f"Primary key column '{primary_key}' not found in schema.")

    num_days = args.days
    records_per_day = args.records_per_day

    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=num_days)
    base_time = datetime.datetime.now().time()

    active_set = {}    # Mapping from primary key to row (active records carried forward)
    all_rows = []      # List of all generated rows (for CSV)
    summary_lines = [] # Summary report lines
    insert_sql_scripts = []  # List of INSERT statements per day

    # For each day, simulate a mix of updated and unchanged records from the previous active set.
    for day in range(1, num_days + 1):
        simulation_date = start_date + datetime.timedelta(days=day - 1)
        summary_lines.append(f"Day {day} (partition_dt = {simulation_date.strftime('%Y-%m-%d')}):")
        daily_rows = []

        if day == 1:
            # Day 1: all records are new.
            new_keys = []
            for i in range(records_per_day):
                row = generate_new_row(columns, simulation_date, primary_key, base_time)
                active_set[row[primary_key]] = row
                daily_rows.append(row)
                new_keys.append(str(row[primary_key]))
            summary_lines.append("  New records: " + ", ".join(new_keys))
        else:
            # For days 2+, process a subset of previous active rows.
            prev_active_keys = list(active_set.keys())
            num_prev = len(prev_active_keys)
            processed_count = random.randint(1, min(num_prev, records_per_day)) if num_prev > 0 else 0
            new_count = records_per_day - processed_count

            processed_keys = random.sample(prev_active_keys, processed_count) if processed_count > 0 else []
            processed_rows = []
            summary_updated = []
            summary_unchanged = []

            for pk in processed_keys:
                old_row = active_set[pk]
                # Choose randomly to update or carry forward unchanged (50/50 chance)
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

            omitted_keys = [str(pk) for pk in prev_active_keys if pk not in processed_keys]

            new_rows = []
            new_keys = []
            for i in range(new_count):
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
            if omitted_keys:
                summary_lines.append("  Omitted records: " + ", ".join(omitted_keys))

            # Update active_set for next day: include processed and new records.
            new_active_set = {}
            for row in processed_rows:
                new_active_set[row[primary_key]] = row
            for row in new_rows:
                new_active_set[row[primary_key]] = row
            active_set = new_active_set

        # For every row generated today, update the partition_dt and compute the dynamic file name.
        for row in daily_rows:
            row["partition_dt"] = simulation_date
            # Use the audit default template for pipeline_execution_ingest_map.
            template = audit_defaults["pipeline_execution_ingest_map"]
            if "YYYYMMDD" in template:
                dynamic_filename = template.replace("YYYYMMDD", simulation_date.strftime("%Y%m%d"))
            else:
                dynamic_filename = f"{template}_{simulation_date.strftime('%Y%m%d')}.csv"
            row["pipeline_execution_ingest_map"] = dynamic_filename

        # Generate the SQL INSERT statement for this day.
        col_names = [col for col, _ in columns]
        values_list = []
        for row in daily_rows:
            formatted_values = [
                format_sql_value_with_quote_option(row[col], schema_dict[col], col, quote_cols)
                for col in col_names
            ]
            values_list.append("(" + ", ".join(formatted_values) + ")")
        insert_stmt = (
            f"-- Day {day} (partition_dt = {simulation_date.strftime('%Y-%m-%d')})\n"
            f"INSERT INTO {args.target_table} ({', '.join(col_names)}) VALUES\n"
            + ",\n".join(values_list)
            + ";\n"
        )
        insert_sql_scripts.append(insert_stmt)
        all_rows.extend(daily_rows)
        summary_lines.append("")

    with open("inserts.sql", "w") as f:
        for stmt in insert_sql_scripts:
            f.write(stmt + "\n")

    with open("output.csv", "w", newline="") as f:
        header = args.delimiter.join([col for col, _ in columns])
        f.write(header + "\n")
        for row in all_rows:
            line = args.delimiter.join([format_csv_value(row[col]) for col, _ in columns])
            f.write(line + "\n")

    with open("summary.txt", "w") as f:
        for line in summary_lines:
            f.write(line + "\n")

if __name__ == "__main__":
    main()
