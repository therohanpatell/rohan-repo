# Unit Testing Scenarios for Dynamic Schema Framework

## Overview
This document outlines comprehensive unit testing scenarios for the dynamic schema framework that fetches table schemas from BigQuery at runtime instead of using hardcoded schemas.

---

## 1. Schema Conversion Tests (BigQueryOperations.get_spark_schema_from_bq_table)

### Positive Scenarios

#### 1.1 Test All Supported Data Type Mappings
**Test Case**: Verify correct BigQuery to Spark type conversion
- **Input**: BigQuery schema with all supported types (STRING, INTEGER, FLOAT, NUMERIC, BOOLEAN, DATE, TIMESTAMP)
- **Expected**: Spark StructType with correct type mappings
  - STRING → StringType()
  - INTEGER/INT64 → IntegerType()
  - FLOAT/FLOAT64 → DoubleType()
  - NUMERIC/DECIMAL → DecimalType(38, 9)
  - BOOLEAN/BOOL → BooleanType()
  - DATE → DateType()
  - TIMESTAMP/DATETIME → TimestampType()
- **Validation**: Assert each field type matches expected Spark type

#### 1.2 Test Nullable Field Preservation
**Test Case**: Verify nullable fields are correctly marked
- **Input**: BigQuery schema with mix of nullable and required fields
- **Expected**: Spark StructType preserves nullable=True/False for each field
- **Validation**: Assert nullable property matches for all fields

#### 1.3 Test Required Field Preservation
**Test Case**: Verify required (non-nullable) fields are correctly marked
- **Input**: BigQuery schema with required fields (mode='REQUIRED')
- **Expected**: Spark StructType with nullable=False for required fields
- **Validation**: Assert required fields have nullable=False

#### 1.4 Test Simple Table Schema (3-5 columns)
**Test Case**: Fetch schema for simple table
- **Input**: Table with 3 columns (id: INTEGER, name: STRING, created_date: DATE)
- **Expected**: Spark StructType with 3 fields correctly mapped
- **Validation**: Assert field count and types match

#### 1.5 Test Complex Table Schema (20+ columns)
**Test Case**: Fetch schema for complex table with many columns
- **Input**: Table with 25 columns of various types
- **Expected**: Spark StructType with all 25 fields correctly mapped
- **Validation**: Assert all fields present and correctly typed

### Negative Scenarios

#### 1.6 Test Non-Existent Table
**Test Case**: Attempt to fetch schema for table that doesn't exist
- **Input**: Invalid table name "project.dataset.nonexistent_table"
- **Expected**: BigQueryError raised with clear message
- **Validation**: Assert exception type and error message contains table name

#### 1.7 Test Invalid Table Name Format
**Test Case**: Provide malformed table name
- **Input**: "invalid_table_name" (missing project/dataset)
- **Expected**: BigQueryError or ValidationError raised
- **Validation**: Assert exception raised with helpful error message

#### 1.8 Test Unsupported Data Type
**Test Case**: Table contains unsupported BigQuery type
- **Input**: BigQuery schema with GEOGRAPHY or ARRAY type
- **Expected**: BigQueryError raised or default to StringType with warning
- **Validation**: Assert error handling or fallback behavior

#### 1.9 Test Empty Schema
**Test Case**: Table exists but has no columns (edge case)
- **Input**: Table with 0 columns
- **Expected**: BigQueryError or empty StructType
- **Validation**: Assert appropriate handling

#### 1.10 Test Permission Denied
**Test Case**: User lacks permission to access table
- **Input**: Table user doesn't have access to
- **Expected**: BigQueryError with permission denied message
- **Validation**: Assert exception contains permission error details

---

## 2. Multi-Record SQL Execution Tests (BigQueryOperations.execute_sql_with_results)

### Positive Scenarios

#### 2.1 Test Single Record Return
**Test Case**: SQL query returns exactly 1 record
- **Input**: SQL query returning 1 row with 5 columns
- **Expected**: List with 1 dictionary containing all 5 columns
- **Validation**: Assert len(result) == 1 and all columns present

#### 2.2 Test Multiple Records Return (Small)
**Test Case**: SQL query returns 10 records
- **Input**: SQL query returning 10 rows
- **Expected**: List with 10 dictionaries, each with all columns
- **Validation**: Assert len(result) == 10 and structure consistent

#### 2.3 Test Multiple Records Return (Medium)
**Test Case**: SQL query returns 1,000 records
- **Input**: SQL query returning 1,000 rows
- **Expected**: List with 1,000 dictionaries
- **Validation**: Assert len(result) == 1000 and memory usage acceptable

#### 2.4 Test Multiple Records Return (Large)
**Test Case**: SQL query returns 100,000 records
- **Input**: SQL query returning 100,000 rows
- **Expected**: List with 100,000 dictionaries
- **Validation**: Assert len(result) == 100000 and performance acceptable

#### 2.5 Test All Column Types in Results
**Test Case**: SQL returns columns of all supported types
- **Input**: SQL with STRING, INTEGER, FLOAT, DECIMAL, BOOLEAN, DATE, TIMESTAMP columns
- **Expected**: List of dictionaries with correct Python types for each column
- **Validation**: Assert type(value) matches expected Python type

#### 2.6 Test NULL Values in Results
**Test Case**: SQL returns records with NULL values
- **Input**: SQL returning rows with some NULL columns
- **Expected**: List of dictionaries with None for NULL values
- **Validation**: Assert NULL columns have None value

#### 2.7 Test Dynamic Column Names
**Test Case**: SQL returns columns with various names (not hardcoded)
- **Input**: SQL with columns: custom_metric, user_count, avg_value
- **Expected**: List of dictionaries with exact column names from SQL
- **Validation**: Assert dictionary keys match SQL column names

### Negative Scenarios

#### 2.8 Test Empty Result Set
**Test Case**: SQL query returns 0 records
- **Input**: SQL query with WHERE clause matching no rows
- **Expected**: Empty list []
- **Validation**: Assert len(result) == 0 and no error raised

#### 2.9 Test SQL Syntax Error
**Test Case**: Invalid SQL syntax
- **Input**: "SELCT * FROM table" (typo)
- **Expected**: SQLExecutionError raised with syntax error details
- **Validation**: Assert exception type and error category is SQL_SYNTAX_ERROR

#### 2.10 Test SQL Timeout
**Test Case**: SQL query exceeds timeout limit
- **Input**: SQL query that runs longer than 180 seconds
- **Expected**: SQLExecutionError raised with timeout error
- **Validation**: Assert exception type and error category is SQL_TIMEOUT_ERROR

#### 2.11 Test Table Not Found in SQL
**Test Case**: SQL references non-existent table
- **Input**: "SELECT * FROM project.dataset.nonexistent"
- **Expected**: SQLExecutionError raised with table not found error
- **Validation**: Assert exception contains table name

#### 2.12 Test Column Not Found in SQL
**Test Case**: SQL references non-existent column
- **Input**: "SELECT nonexistent_column FROM table"
- **Expected**: SQLExecutionError raised with column error
- **Validation**: Assert exception contains column name

#### 2.13 Test Extremely Large Result Set (Memory)
**Test Case**: SQL query returns 1,000,000+ records
- **Input**: SQL returning over 1 million rows
- **Expected**: Either successful processing or graceful memory error
- **Validation**: Assert memory handling or appropriate error

---

## 3. Schema Caching Tests (MetricsPipeline.get_cached_schema)

### Positive Scenarios

#### 3.1 Test Cache Miss (First Access)
**Test Case**: First time accessing schema for a table
- **Input**: Target table not in cache
- **Expected**: Schema fetched from BigQuery and stored in cache
- **Validation**: Assert schema_cache contains table after call

#### 3.2 Test Cache Hit (Subsequent Access)
**Test Case**: Second access to same table schema
- **Input**: Target table already in cache
- **Expected**: Schema returned from cache without BigQuery call
- **Validation**: Assert BigQuery method not called, cache returned

#### 3.3 Test Multiple Tables Caching
**Test Case**: Cache schemas for 5 different tables
- **Input**: 5 unique target tables
- **Expected**: All 5 schemas cached independently
- **Validation**: Assert schema_cache has 5 entries with correct keys

#### 3.4 Test Cache Reuse Across Metrics
**Test Case**: 10 metrics targeting same table
- **Input**: 10 metrics with same target_table
- **Expected**: Schema fetched once, reused 9 times
- **Validation**: Assert BigQuery called once, cache hit 9 times

### Negative Scenarios

#### 3.5 Test Cache with Invalid Table
**Test Case**: Attempt to cache schema for non-existent table
- **Input**: Invalid target table name
- **Expected**: BigQueryError raised, nothing cached
- **Validation**: Assert exception raised and cache remains empty for that table

#### 3.6 Test Cache After Schema Fetch Failure
**Test Case**: Schema fetch fails, verify cache state
- **Input**: Table with permission denied
- **Expected**: Error raised, no cache entry created
- **Validation**: Assert table not in cache after failure

---

## 4. Schema Alignment Tests (BigQueryOperations.align_dataframe_schema_with_bq)

### Positive Scenarios

#### 4.1 Test Perfect Schema Match
**Test Case**: DataFrame columns exactly match target table
- **Input**: DataFrame with all required columns and correct types
- **Expected**: DataFrame returned unchanged (or with minor type adjustments)
- **Validation**: Assert all columns present and types correct

#### 4.2 Test Extra Columns Dropped
**Test Case**: DataFrame has columns not in target table
- **Input**: DataFrame with 10 columns, target table has 8
- **Expected**: 2 extra columns dropped, 8 columns retained
- **Validation**: Assert final DataFrame has exactly 8 columns

#### 4.3 Test Missing Nullable Columns Added
**Test Case**: DataFrame missing nullable columns from target
- **Input**: DataFrame missing 2 nullable columns
- **Expected**: 2 columns added with NULL values
- **Validation**: Assert columns added with None values

#### 4.4 Test Type Conversion (String to Date)
**Test Case**: DataFrame has string column, target expects DATE
- **Input**: DataFrame with '2024-01-15' as string, target has DATE type
- **Expected**: Column converted to DateType
- **Validation**: Assert column type is DateType after alignment

#### 4.5 Test Type Conversion (Integer to Decimal)
**Test Case**: DataFrame has integer, target expects NUMERIC
- **Input**: DataFrame with IntegerType, target has DecimalType(38,9)
- **Expected**: Column converted to DecimalType
- **Validation**: Assert column type is DecimalType after alignment

#### 4.6 Test Column Reordering
**Test Case**: DataFrame columns in different order than target
- **Input**: DataFrame with columns [C, A, B], target has [A, B, C]
- **Expected**: Columns reordered to match target [A, B, C]
- **Validation**: Assert column order matches target schema

### Negative Scenarios

#### 4.7 Test Missing Required Column
**Test Case**: DataFrame missing required (non-nullable) column
- **Input**: DataFrame missing 'metric_id' (required column)
- **Expected**: ValidationError raised with missing column details
- **Validation**: Assert exception message lists 'metric_id' as missing

#### 4.8 Test Multiple Missing Required Columns
**Test Case**: DataFrame missing 3 required columns
- **Input**: DataFrame missing 'metric_id', 'metric_name', 'metric_output'
- **Expected**: ValidationError with all 3 columns listed
- **Validation**: Assert exception message contains all missing columns

#### 4.9 Test Type Conversion Failure
**Test Case**: Cannot convert column to target type
- **Input**: DataFrame with 'invalid_date' string, target expects DATE
- **Expected**: ValidationError or type conversion error
- **Validation**: Assert exception with column name and type details

#### 4.10 Test Empty DataFrame
**Test Case**: DataFrame has 0 rows
- **Input**: Empty DataFrame with schema
- **Expected**: Empty DataFrame returned with aligned schema
- **Validation**: Assert schema aligned but row count remains 0

#### 4.11 Test All Columns Missing
**Test Case**: DataFrame has completely different columns
- **Input**: DataFrame with [X, Y, Z], target expects [A, B, C]
- **Expected**: ValidationError for missing required columns
- **Validation**: Assert exception lists all required missing columns

---

## 5. End-to-End Process Metrics Tests (MetricsPipeline.process_metrics)

### Positive Scenarios

#### 5.1 Test Single Metric Single Record
**Test Case**: Process 1 metric returning 1 record
- **Input**: 1 JSON metric with SQL returning 1 row
- **Expected**: 1 record written to target table, 1 recon record created
- **Validation**: Assert write_results contains 1 success

#### 5.2 Test Single Metric Multiple Records
**Test Case**: Process 1 metric returning 100 records
- **Input**: 1 JSON metric with SQL returning 100 rows
- **Expected**: 100 records written to target table, 1 recon record created
- **Validation**: Assert write_results shows 100 records written

#### 5.3 Test Multiple Metrics Same Table
**Test Case**: Process 5 metrics targeting same table
- **Input**: 5 metrics with same target_table
- **Expected**: Schema fetched once (cached), all 5 metrics processed
- **Validation**: Assert schema cache hit 4 times, all metrics succeed

#### 5.4 Test Multiple Metrics Different Tables
**Test Case**: Process 5 metrics targeting 5 different tables
- **Input**: 5 metrics with unique target_tables
- **Expected**: 5 schemas fetched and cached, all metrics processed
- **Validation**: Assert 5 cache entries, all metrics succeed

#### 5.5 Test Partition Date Added to All Records
**Test Case**: Verify partition_dt added to all SQL result rows
- **Input**: Metric with SQL returning 10 rows
- **Expected**: All 10 rows have partition_dt field with correct value
- **Validation**: Assert all records have partition_dt matching run_date

#### 5.6 Test Pipeline Execution Timestamp Added
**Test Case**: Verify pipeline_execution_ts added to all records
- **Input**: Metric with SQL returning 10 rows
- **Expected**: All 10 rows have pipeline_execution_ts field
- **Validation**: Assert all records have valid timestamp

#### 5.7 Test SQL Placeholder Replacement
**Test Case**: SQL with {currently} placeholder
- **Input**: SQL with "WHERE date = {currently}"
- **Expected**: Placeholder replaced with run_date before execution
- **Validation**: Assert SQL executed with correct date value

#### 5.8 Test Partition Info Placeholder
**Test Case**: SQL with {partition_info} placeholder
- **Input**: SQL with "WHERE partition_dt = {partition_info}"
- **Expected**: Placeholder replaced with partition date from metadata table
- **Validation**: Assert SQL executed with correct partition date

### Negative Scenarios

#### 5.9 Test Schema Fetch Failure for One Metric
**Test Case**: One metric targets non-existent table
- **Input**: 3 metrics, 1 with invalid target_table
- **Expected**: 1 metric fails with schema error, 2 succeed
- **Validation**: Assert 2 success, 1 failure with SCHEMA_VALIDATION_ERROR

#### 5.10 Test SQL Execution Failure for One Metric
**Test Case**: One metric has invalid SQL
- **Input**: 3 metrics, 1 with SQL syntax error
- **Expected**: 1 metric fails with SQL error, 2 succeed
- **Validation**: Assert 2 success, 1 failure with SQL_SYNTAX_ERROR

#### 5.11 Test Missing Required Columns in SQL Results
**Test Case**: SQL doesn't return required columns
- **Input**: Metric with SQL missing 'metric_id' column
- **Expected**: Metric fails with schema validation error
- **Validation**: Assert failure with missing column details in error

#### 5.12 Test All Metrics Fail
**Test Case**: All metrics have errors
- **Input**: 5 metrics all with invalid SQL or tables
- **Expected**: All 5 metrics fail, 5 recon records with 'Failed' status
- **Validation**: Assert 0 success, 5 failures, all recon records created

#### 5.13 Test Empty SQL Result Set
**Test Case**: SQL returns 0 rows
- **Input**: Metric with SQL WHERE clause matching nothing
- **Expected**: 0 records written, recon record shows 0 records
- **Validation**: Assert write_results shows 0 records, recon created

#### 5.14 Test Type Mismatch in SQL Results
**Test Case**: SQL returns wrong type for column
- **Input**: SQL returns string for column expecting integer
- **Expected**: Type conversion attempted or validation error
- **Validation**: Assert error handling or successful conversion

---

## 6. Recon Record Creation Tests

### Positive Scenarios

#### 6.1 Test Recon Record for Successful Metric
**Test Case**: Verify recon record created for successful metric
- **Input**: Metric successfully written to target table
- **Expected**: Recon record with rcncln_exact_pass_in = 'Passed'
- **Validation**: Assert recon record has correct status and metric details

#### 6.2 Test Recon Record with Multiple Records Written
**Test Case**: Metric writes 100 records
- **Input**: Metric with SQL returning 100 rows
- **Expected**: Recon record shows 100 records written
- **Validation**: Assert recon record count field = 100

#### 6.3 Test Recon Record for Failed Metric (Schema Error)
**Test Case**: Metric fails due to schema validation
- **Input**: Metric with missing required columns
- **Expected**: Recon record with rcncln_exact_pass_in = 'Failed', error details in excldd_reason_tx
- **Validation**: Assert recon status 'Failed' and error message contains schema details

#### 6.4 Test Recon Record for Failed Metric (SQL Error)
**Test Case**: Metric fails due to SQL execution error
- **Input**: Metric with invalid SQL
- **Expected**: Recon record with 'Failed' status and SQL error details
- **Validation**: Assert recon record contains SQL error category and message

#### 6.5 Test Recon Schema Unchanged
**Test Case**: Verify RECON_SCHEMA still exists and unchanged
- **Input**: Check config.py
- **Expected**: RECON_SCHEMA constant present with all 29 fields
- **Validation**: Assert RECON_SCHEMA exists and has correct structure

### Negative Scenarios

#### 6.6 Test Recon Record with None Values Prevented
**Test Case**: Ensure no None values in required recon fields
- **Input**: Failed metric with minimal information
- **Expected**: Recon record uses 'UNKNOWN' or safe defaults for missing values
- **Validation**: Assert no None values in required recon fields

#### 6.7 Test Recon Record for Multiple Failures
**Test Case**: Metric fails at multiple stages
- **Input**: Metric with schema error AND SQL error
- **Expected**: Recon record captures first/primary error
- **Validation**: Assert recon record has clear error message

---

## 7. Integration Tests

### Positive Scenarios

#### 7.1 Test Backward Compatibility with Old JSON
**Test Case**: Run pipeline with existing JSON configurations
- **Input**: JSON files that worked with hardcoded METRICS_SCHEMA
- **Expected**: All metrics process successfully
- **Validation**: Assert same results as before dynamic schema change

#### 7.2 Test New JSON with Custom Columns
**Test Case**: Run pipeline with JSON targeting custom table structure
- **Input**: JSON with SQL returning custom columns not in old METRICS_SCHEMA
- **Expected**: All metrics process successfully with custom columns
- **Validation**: Assert custom columns written to target table

#### 7.3 Test Mixed Metrics (Old and New Structure)
**Test Case**: Process metrics with both old and new column structures
- **Input**: 5 metrics with old structure, 5 with new custom structure
- **Expected**: All 10 metrics process successfully
- **Validation**: Assert all metrics succeed regardless of structure

#### 7.4 Test Performance with Schema Caching
**Test Case**: Measure BigQuery API calls with caching
- **Input**: 20 metrics targeting 3 different tables
- **Expected**: Only 3 schema fetch calls (1 per unique table)
- **Validation**: Assert BigQuery schema fetch called exactly 3 times

### Negative Scenarios

#### 7.5 Test Pipeline with All Invalid Metrics
**Test Case**: Run pipeline where all metrics fail
- **Input**: JSON with all metrics having errors
- **Expected**: Pipeline completes, all recon records show 'Failed'
- **Validation**: Assert pipeline doesn't crash, all failures recorded

#### 7.6 Test Pipeline with Mixed Success and Failure
**Test Case**: Some metrics succeed, some fail
- **Input**: 10 metrics, 6 succeed, 4 fail
- **Expected**: 6 written successfully, 4 recon records with 'Failed'
- **Validation**: Assert correct count of success/failure

---

## 8. Error Handling and Logging Tests

### Positive Scenarios

#### 8.1 Test Schema Cache Hit Logging
**Test Case**: Verify log message for cache hit
- **Input**: Access cached schema
- **Expected**: Log message "Schema cache hit for table: {table_name}"
- **Validation**: Assert log contains cache hit message

#### 8.2 Test Schema Cache Miss Logging
**Test Case**: Verify log message for cache miss
- **Input**: Access schema not in cache
- **Expected**: Log message "Schema cache miss for table: {table_name}, fetching from BigQuery"
- **Validation**: Assert log contains cache miss message

#### 8.3 Test Multi-Record Count Logging
**Test Case**: Verify log message for multiple records
- **Input**: SQL returning 100 records
- **Expected**: Log message "SQL query returned 100 records for metric {metric_id}"
- **Validation**: Assert log contains record count

### Negative Scenarios

#### 8.4 Test Schema Validation Error Logging
**Test Case**: Verify detailed error logging for schema validation
- **Input**: Metric with missing required columns
- **Expected**: Log contains metric_id, target_table, and list of missing columns
- **Validation**: Assert log message has all error details

#### 8.5 Test Error Category Assignment
**Test Case**: Verify SCHEMA_VALIDATION_ERROR category used
- **Input**: Metric with schema validation failure
- **Expected**: Error categorized as 'SCHEMA_VALIDATION_ERROR'
- **Validation**: Assert error_category field in recon record

#### 8.6 Test Error Message Sanitization
**Test Case**: Verify error messages cleaned properly
- **Input**: Error with special characters or sensitive data
- **Expected**: Error message sanitized using StringUtils.clean_error_message()
- **Validation**: Assert error message is clean and safe

---

## 9. Performance and Load Tests

### Positive Scenarios

#### 9.1 Test Large Result Set (100K records)
**Test Case**: Process metric returning 100,000 records
- **Input**: SQL returning 100,000 rows
- **Expected**: All records processed and written successfully
- **Validation**: Assert performance acceptable (< 5 minutes)

#### 9.2 Test Many Metrics (100 metrics)
**Test Case**: Process 100 metrics in single run
- **Input**: JSON with 100 metric definitions
- **Expected**: All metrics processed successfully
- **Validation**: Assert completion time acceptable

#### 9.3 Test Schema Cache Efficiency
**Test Case**: Measure time saved by caching
- **Input**: 50 metrics targeting 5 tables
- **Expected**: Significant time savings vs fetching schema 50 times
- **Validation**: Assert cache reduces total execution time

### Negative Scenarios

#### 9.4 Test Memory Limit with Huge Result Set
**Test Case**: SQL returning 1,000,000+ records
- **Input**: SQL with massive result set
- **Expected**: Either successful processing or graceful memory error
- **Validation**: Assert no crash, appropriate error handling

#### 9.5 Test Timeout Handling
**Test Case**: SQL query exceeds timeout
- **Input**: Complex SQL taking > 180 seconds
- **Expected**: Timeout error caught and logged
- **Validation**: Assert timeout error category and metric marked failed

---

## 10. Edge Cases and Boundary Tests

### Positive Scenarios

#### 10.1 Test Single Column Table
**Test Case**: Target table has only 1 column
- **Input**: SQL returning 1 column matching target
- **Expected**: Record written successfully
- **Validation**: Assert single column handled correctly

#### 10.2 Test Wide Table (50+ columns)
**Test Case**: Target table has 50 columns
- **Input**: SQL returning all 50 columns
- **Expected**: All columns written successfully
- **Validation**: Assert all columns present in target

#### 10.3 Test All NULL Result
**Test Case**: SQL returns record with all NULL values
- **Input**: SQL with all columns as NULL
- **Expected**: Record written with NULL values
- **Validation**: Assert NULLs handled correctly

### Negative Scenarios

#### 10.4 Test Completely Wrong Schema
**Test Case**: SQL returns columns with no overlap to target
- **Input**: SQL returns [X, Y, Z], target expects [A, B, C]
- **Expected**: Validation error for all missing required columns
- **Validation**: Assert clear error message listing all missing columns

#### 10.5 Test Special Characters in Column Names
**Test Case**: Table has columns with special characters
- **Input**: Columns like "metric-id", "metric.name"
- **Expected**: Columns handled correctly or error raised
- **Validation**: Assert special characters handled or clear error

#### 10.6 Test Reserved Keywords as Column Names
**Test Case**: Table has columns named with SQL reserved words
- **Input**: Columns like "select", "from", "where"
- **Expected**: Columns handled correctly with proper escaping
- **Validation**: Assert reserved words handled safely

---

## Test Execution Priority

### High Priority (Must Test First)
1. Schema conversion for all data types (1.1)
2. Multi-record SQL execution (2.1-2.4)
3. Schema caching (3.1-3.4)
4. Schema alignment with missing required columns (4.7-4.8)
5. End-to-end single metric (5.1)
6. Recon record creation (6.1, 6.3)

### Medium Priority
1. Error handling scenarios (all negative tests)
2. Integration tests (7.1-7.4)
3. Logging verification (8.1-8.4)
4. Performance tests (9.1-9.3)

### Low Priority (Nice to Have)
1. Edge cases (10.1-10.6)
2. Extreme load tests (9.4-9.5)
3. Special character handling (10.5-10.6)

---

## Test Data Requirements

### Mock Data Needed
1. Sample BigQuery schemas (simple, complex, various types)
2. Sample SQL queries (single record, multi-record, various columns)
3. Sample JSON metric configurations
4. Sample target tables in test BigQuery project
5. Sample partition_info_table data

### Test Environment Setup
1. Test BigQuery project with sample tables
2. Test GCS bucket with sample JSON files
3. Mock Spark session for unit tests
4. Test recon table
5. Test partition_info_table

---

## Success Criteria

### All Tests Must
1. Pass consistently (no flaky tests)
2. Have clear assertions
3. Clean up test data after execution
4. Run in isolation (no dependencies between tests)
5. Complete within reasonable time (< 30 seconds per unit test)

### Coverage Goals
- Code coverage: > 90%
- Branch coverage: > 85%
- All error paths tested
- All positive paths tested
- All edge cases covered
