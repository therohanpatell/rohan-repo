Below is an example unit test document you can use to outline your SCD2 pipeline tests. You can modify this document as needed to match your project specifics.

---

# Unit Test Documentation for SCD2 Pipeline in BigQuery

## 1. Introduction

This document outlines the unit tests for validating the SCD2 pipeline that processes transactional data from a source table partitioned by `partition_dt` and generates two target tables:
- **Latest Table:** Contains all new records and the latest updated records.
- **History Table:** Contains only the records that have changed (i.e., the previous version of updated records).

The purpose of these tests is to ensure that the SCD2 logic is implemented correctly.

## 2. Scope

This document covers unit test cases that:
- Verify all records in the source (for a given partition date) are represented in the latest table.
- Ensure that no record exists simultaneously in both the latest and history tables for the same partition.
- Confirm that updated records in the history table truly reflect a change in the business column (e.g., `col1`).
- Validate that the count of updated records (as determined by a change in business values) matches the number of entries in the history table.

## 3. Test Environment

- **Data Warehouse:** BigQuery  
- **Tables:**
  - **Source Table:** `your_project.your_dataset.source_table`
  - **Latest Table:** `your_project.your_dataset.latest_table`
  - **History Table:** `your_project.your_dataset.history_table`
- **Partition Filter:** All queries are filtered on `partition_dt = 'yyyy-mm-dd'` (replace with the actual date).

## 4. Pre-Requisites

- The SCD2 pipeline has been executed for the target partition date.
- Tables are partitioned on `partition_dt`.
- Business columns (e.g., `col1`) used for change detection exist in all tables.
- Proper permissions are in place to execute queries in BigQuery.

## 5. Unit Test Cases

### Test Case 1: Validate Complete Data in Latest Table

**Objective:**  
Ensure that every record from the source table (for the given partition) appears in the latest table.

**Query:**
```sql
-- Test 1: Ensure every record from source exists in latest table for partition_dt = 'yyyy-mm-dd'
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
```

**Expected Output:**  
No rows returned (i.e., an empty result set), confirming that every source record is present in the latest table.

---

### Test Case 2: Validate No Record Overlap Between Latest and History Tables

**Objective:**  
Ensure that no record for the specified partition exists in both the latest and history tables simultaneously.

**Query:**
```sql
-- Test 2: Ensure a record for partition_dt = 'yyyy-mm-dd' does not appear in both tables.
SELECT l.id
FROM `your_project.your_dataset.latest_table` l
JOIN `your_project.your_dataset.history_table` h 
  ON l.id = h.id
WHERE l.partition_dt = 'yyyy-mm-dd'
  AND h.partition_dt = 'yyyy-mm-dd';
```

**Expected Output:**  
No rows returned, indicating there is no overlap between the two tables.

---

### Test Case 3: Validate Updated Records Reflect Business Column Changes

**Objective:**  
Confirm that for records present in both tables (i.e., updated records), the business column (e.g., `col1`) in the latest table differs from that in the history table.

**Query:**
```sql
-- Test 3: Check that for records appearing in both tables, the business column 'col1' is different.
SELECT l.id, l.col1 AS latest_col1, h.col1 AS history_col1
FROM `your_project.your_dataset.latest_table` l
JOIN `your_project.your_dataset.history_table` h 
  ON l.id = h.id
WHERE l.partition_dt = 'yyyy-mm-dd'
  AND h.partition_dt = 'yyyy-mm-dd'
  AND l.col1 = h.col1;
```

**Expected Output:**  
No rows returned. If rows are returned, it suggests that some records did not change as expected when updated.

---

### Test Case 4: Validate Count Consistency of Updated Records

**Objective:**  
Verify that the number of updated records (determined by a change in the business column) in the source corresponds to the number of records in the history table for the partition.

**Query:**
```sql
-- Test 4: Validate count consistency between updated records and history entries for partition_dt = 'yyyy-mm-dd'
WITH updated_records AS (
  SELECT DISTINCT s.id
  FROM `your_project.your_dataset.source_table` s
  JOIN `your_project.your_dataset.history_table` h ON s.id = h.id
  JOIN `your_project.your_dataset.latest_table` l ON s.id = l.id
  WHERE s.partition_dt = 'yyyy-mm-dd'
    AND h.partition_dt = 'yyyy-mm-dd'
    AND l.partition_dt = 'yyyy-mm-dd'
    -- Identify records where the business value differs between history and latest.
    AND l.col1 != h.col1
)
SELECT
  (SELECT COUNT(*) FROM updated_records) AS source_update_count,
  (SELECT COUNT(DISTINCT id) FROM `your_project.your_dataset.history_table`
   WHERE partition_dt = 'yyyy-mm-dd') AS history_record_count;
```

**Expected Output:**  
A single row with two columns where `source_update_count` equals `history_record_count`. For example:

| source_update_count | history_record_count |
|---------------------|----------------------|
| 15                  | 15                   |

This indicates that every updated record is properly captured in the history table.

---

## 6. Execution Instructions

1. **Access BigQuery Console:**  
   Log in to the BigQuery console with appropriate credentials.

2. **Update Query Parameters:**  
   Replace `your_project.your_dataset` with your actual project and dataset names. Also, update `'yyyy-mm-dd'` with the actual partition date you wish to test.

3. **Execute Queries:**  
   Run each query one by one. You can copy and paste the queries into the BigQuery editor.

4. **Compare Results:**  
   Verify that the output of each query matches the expected results.  
   - No rows for Test Cases 1, 2, and 3.
   - Matching counts for Test Case 4.

5. **Document Findings:**  
   Record any discrepancies and initiate further investigation if the results do not match the expected outputs.

---

## 7. Assumptions & Dependencies

- **Unique Identifier:**  
  All tables have a unique identifier column (`id`) that is used to join the datasets.

- **Business Logic:**  
  The SCD2 process uses business column `col1` (or other columns as defined) for change detection.

- **Partitioning:**  
  All tables are partitioned by `partition_dt`.

- **Permissions:**  
  The user executing these queries has sufficient permissions in BigQuery.

---

## 8. Conclusion

These unit tests are designed to validate the integrity of the SCD2 pipeline in ensuring:
- Complete and correct record placement in the latest table.
- No overlap of records between the latest and history tables.
- Accurate capture of changes between updated records.
- Consistency in record counts between source updates and history entries.

Passing these tests confirms that the pipeline is functioning as intended for the tested partition. Any failures should prompt a review of the SCD2 implementation and further debugging.

---

This document should serve as a comprehensive guide for your unit testing efforts. Feel free to add more test cases or adjust queries to suit your evolving requirements.
