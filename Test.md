Below are several example BigQuery test queries you can run to “unit‐test” your SCD2 pipeline logic. In these examples, we assume that your tables have a primary key (here, we use “id”) plus one or more business columns (for example, “col1”, “col2”, etc.) and that all tables are partitioned on a column named partition_dt. Adjust the column names and logic as needed for your actual schema and SCD2 design.

For example, suppose your SCD2 pipeline follows these rules:

New records in the source (for a given partition date) are inserted into the latest table.

Updated records (i.e. when one or more business columns change) trigger two actions:

The previous version is “closed” by inserting it into the history table.

The new (latest) version is inserted (or updated) in the latest table.


A record should not appear in both tables for the same partition (its “history” version goes only to history and its current version remains in latest).


Below are four robust test queries along with the expected output when run.


---

Test 1: Verify that every source record for the partition is in the latest table

This test checks that for the given partition date, every record from the source that was processed (either as a new record or an update) appears in the latest table. (If a record is missing from latest, something went wrong.)

-- Test 1: Ensure that every record from source (for partition_dt = 'yyyy-mm-dd')
-- exists in the latest table.
WITH source_data AS (
  SELECT id
  FROM `your_project.your_dataset.source_table`
  WHERE partition_dt = 'yyyy-mm-dd'
),
latest_data AS (
  SELECT id
  FROM `your_project.your_dataset.latest_table`
  WHERE partition_dt = 'yyyy-mm-dd'
)
SELECT s.id
FROM source_data s
LEFT JOIN latest_data l 
  ON s.id = l.id
WHERE l.id IS NULL;

Expected output:
No rows returned (an empty result set) meaning every source record is found in the latest table.


---

Test 2: Ensure no record appears in both latest and history tables for the same partition

A record should be “current” (in the latest table) or “old” (in the history table), but never in both for the same partition date.

-- Test 2: Validate that a record for partition_dt = 'yyyy-mm-dd' does not appear in both tables.
SELECT l.id
FROM `your_project.your_dataset.latest_table` l
JOIN `your_project.your_dataset.history_table` h 
  ON l.id = h.id
WHERE l.partition_dt = 'yyyy-mm-dd'
  AND h.partition_dt = 'yyyy-mm-dd';

Expected output:
No rows returned, indicating no overlapping records between the two tables for that partition.


---

Test 3: Verify that updated records have different business values between history and latest

For records that were updated, the new (latest) version should differ from the historical version. (In this example, we compare the column col1; you can extend the logic to additional columns as needed.)

-- Test 3: Check that for records appearing in both tables (i.e. updated records),
-- the business column values are different.
SELECT l.id, l.col1 AS latest_col1, h.col1 AS history_col1
FROM `your_project.your_dataset.latest_table` l
JOIN `your_project.your_dataset.history_table` h 
  ON l.id = h.id
WHERE l.partition_dt = 'yyyy-mm-dd'
  AND h.partition_dt = 'yyyy-mm-dd'
  AND l.col1 = h.col1;

Expected output:
No rows returned. (If any rows are returned, it indicates that for those IDs the “latest” value didn’t change relative to the historical version—even though an update was expected.)


---

Test 4: Validate that the count of updated records in the source matches the number of new history entries

If you have a mechanism (for example, a flag like is_updated in the source data) to indicate which records are updates, you can compare the count of updated records from the source with the number of records in the history table for that partition.

> Note: This query assumes that your source table includes an is_updated Boolean flag for records that have changed. If you don’t have such a flag, you might compare with another audit count or expected number.



-- Test 4: Compare the count of updated records in source with the count in history.
SELECT
  (SELECT COUNT(*) 
   FROM `your_project.your_dataset.source_table`
   WHERE partition_dt = 'yyyy-mm-dd'
     AND is_updated = TRUE) AS source_updates,
  (SELECT COUNT(*) 
   FROM `your_project.your_dataset.history_table`
   WHERE partition_dt = 'yyyy-mm-dd') AS history_count;

Expected output:
A single row where source_updates equals history_count.
For example, if the source shows 15 updated records then history_count should be 15.


---

These queries help validate that:

All processed records are landed in the latest table.

There is no overlap between latest and history for the same partition.

Updated records are correctly versioned (with different values between the current and historical records).

The expected number of updates matches the historical entries inserted.


You can run these tests after each pipeline run to ensure that your SCD2 logic is working as intended. Adjust the table names, column names, and conditions as necessary for your environment.



Below is one way to rewrite Test 4 without relying on an is_updated flag. In this version, we define an “updated record” as one that appears in the source for the partition and has a corresponding history record with a business column value (here, for example, col1) that differs from the current (latest) version. We then compare the count of such updated records (based on the change) with the count of distinct IDs in the history table for that partition.

> Note:
This query assumes that:

Your business column(s) (here we use col1) are the ones you compare to decide if a change occurred.

For every updated record (i.e. where col1 changed), your pipeline inserts exactly one record into the history table.

New records (which have no corresponding history record) are not counted as updates.




Below is the combined query:


---

-- Test 4 (Rewritten): Validate that the count of updated records (determined by a change in col1)
-- matches the count of distinct IDs in the history table for partition_dt = 'yyyy-mm-dd'.

WITH updated_records AS (
  SELECT DISTINCT s.id
  FROM `your_project.your_dataset.source_table` s
  -- Join to history to get the previous version for the same id.
  JOIN `your_project.your_dataset.history_table` h ON s.id = h.id
  -- Also join to latest so we can compare the current (new) value.
  JOIN `your_project.your_dataset.latest_table` l ON s.id = l.id
  WHERE s.partition_dt = 'yyyy-mm-dd'
    AND h.partition_dt = 'yyyy-mm-dd'
    AND l.partition_dt = 'yyyy-mm-dd'
    -- Identify records where the current business value differs from the historical value.
    AND l.col1 != h.col1
)

SELECT
  (SELECT COUNT(*) FROM updated_records) AS source_update_count,
  (SELECT COUNT(DISTINCT id) FROM `your_project.your_dataset.history_table`
   WHERE partition_dt = 'yyyy-mm-dd') AS history_record_count;


---

Expected output:
A single row with two columns, for example:

The numbers should match. If they do, it indicates that for every record in the source that represented an update (as determined by a difference in col1 between the latest and history records), there is a corresponding history record. If the counts differ, then the SCD2 logic may not be capturing updates correctly.

