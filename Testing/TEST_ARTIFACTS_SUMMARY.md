# Test Artifacts Summary

## Overview
This document provides a comprehensive summary of all test artifacts created for the Dynamic Schema Framework testing.

---

## Files Created

### 1. test_scenarios.csv
**Purpose:** Excel-compatible test scenario matrix
**Content:**
- 84 comprehensive test scenarios
- Organized by 10 categories
- Includes Test_ID, Category, Subcategory, Test_Case_Name, Test_Type, Description, Input, Expected_Output, Validation_Method, Priority, Status
- Covers both positive (48) and negative (36) test cases

**Usage:**
- Open in Excel or Google Sheets for easy tracking
- Use Status column to track test execution progress
- Filter by Priority (High/Medium/Low) to prioritize testing
- Filter by Test_Type (Positive/Negative) to run specific test suites

---

### 2. test_metrics_positive.json
**Purpose:** Positive test scenarios in JSON format
**Content:** 15 test metrics covering:
- Single record return (TEST_001)
- Multiple records (10, 1000) (TEST_002, TEST_008)
- Placeholder replacement ({currently}, {partition_info}) (TEST_003, TEST_004)
- All data types (TEST_005)
- NULL value handling (TEST_006)
- Custom columns (TEST_007)
- Schema caching (TEST_009, TEST_010, TEST_011)
- GROUP BY queries (TEST_012)
- CTE queries (TEST_013)
- Decimal precision (TEST_014)
- JOIN queries (TEST_015)

**Usage:**
```bash
spark-submit main.py \
  --gcs_path gs://bucket/test_metrics_positive.json \
  --run_date 2024-01-15 \
  --dependencies <dependency_name> \
  --partition_info_table project.dataset.partition_info \
  --env TEST \
  --recon_table project.dataset.recon_table
```

---

### 3. test_metrics_negative.json
**Purpose:** Negative test scenarios in JSON format
**Content:** 20 test metrics covering:
- Non-existent table (NEG_001)
- Invalid table name format (NEG_002)
- SQL syntax errors (NEG_003)
- Table not found in SQL (NEG_004)
- Column not found (NEG_005)
- Empty result set (NEG_006)
- Missing required columns (NEG_007, NEG_008)
- Type conversion failures (NEG_009)
- Wrong schema (NEG_010)
- Division by zero (NEG_011)
- Invalid date format (NEG_012)
- Missing target_table field (NEG_013)
- Null/empty SQL (NEG_014, NEG_015)
- SQL injection attempt (NEG_016)
- Extremely long SQL (NEG_017)
- Special characters (NEG_018)
- Circular dependency (NEG_019)
- Negative values (NEG_020)

**Usage:**
```bash
spark-submit main.py \
  --gcs_path gs://bucket/test_metrics_negative.json \
  --run_date 2024-01-15 \
  --dependencies <dependency_name> \
  --partition_info_table project.dataset.partition_info \
  --env TEST \
  --recon_table project.dataset.recon_table
```

---

### 4. bigquery_test_schemas.sql
**Purpose:** DDL statements for creating test tables in BigQuery
**Content:** 14 table schemas:

1. **test_metrics_output** - Standard metrics output table (11 columns)
2. **test_metrics_output_2** - Alternative target table
3. **test_custom_metrics** - Custom columns table (region, gender, department, category)
4. **test_source_data** - Source data for SQL queries
5. **test_employee_data** - Employee data for GROUP BY and JOIN tests
6. **test_department_data** - Department data for JOIN tests
7. **test_perm_employees** - Permanent employees for CTE tests
8. **test_non_perm_employees** - Non-permanent employees for CTE tests
9. **test_partition_info** - Partition metadata table
10. **test_recon_table** - Reconciliation tracking table (29 columns)
11. **test_all_data_types** - All supported data types for schema conversion testing
12. **test_simple_table** - Simple 3-column table
13. **test_complex_table** - Complex 25-column table
14. **test_wide_table** - Wide 50+ column table

**Usage:**
```bash
bq query --use_legacy_sql=false < bigquery_test_schemas.sql
```

---

### 5. bigquery_test_data.sql
**Purpose:** Sample data inserts for test tables
**Content:**
- 10 records in test_source_data
- 15 records in test_employee_data
- 3 records in test_department_data
- 10 records in test_perm_employees
- 5 records in test_non_perm_employees
- 5 records in test_partition_info
- 3 records in test_all_data_types
- 3 records in test_simple_table
- Additional queries to generate 1,000 and 100,000 records for volume testing

**Usage:**
```bash
bq query --use_legacy_sql=false < bigquery_test_data.sql
```

---

### 6. test_queries_multiple_records.sql
**Purpose:** 20 test queries demonstrating multiple record returns
**Content:**
1. Simple SELECT (10 records)
2. GROUP BY (multiple groups)
3. UNNEST (100 records)
4. JOIN (multiple records)
5. CTE with aggregation
6. UNION (combined records)
7. Window functions
8. CASE statements
9. Subqueries
10. CROSS JOIN (cartesian product)
11. PIVOT-like aggregation
12. Date functions
13. String functions
14. Numeric functions
15. NULL handling
16. Generate 1000 records
17. Complex aggregation
18. ARRAY_AGG
19. DISTINCT
20. HAVING clause

**Usage:**
- Copy individual queries to test in BigQuery console
- Use as reference for SQL patterns that return multiple records
- Validate execute_sql_with_results() functionality

---

### 7. TEST_EXECUTION_GUIDE.md
**Purpose:** Comprehensive guide for test execution
**Content:**
- Prerequisites and setup instructions
- Step-by-step test execution for each category
- Expected results for each test
- Validation queries
- Performance benchmarks
- Troubleshooting guide
- Cleanup instructions
- Test coverage summary

**Usage:**
- Follow step-by-step to execute all tests
- Reference for expected results
- Use validation queries to verify test outcomes

---

### 8. TEST_ARTIFACTS_SUMMARY.md
**Purpose:** This document - overview of all test artifacts

---

## Test Categories

### Category 1: Schema Conversion Tests (10 tests)
**Focus:** BigQuery to Spark schema conversion
**Key Tests:**
- All data type mappings (STRING, INTEGER, FLOAT, NUMERIC, BOOLEAN, DATE, TIMESTAMP)
- Nullable vs required fields
- Simple vs complex schemas
- Error handling (non-existent table, invalid format, unsupported types)

### Category 2: Multi-Record SQL Execution Tests (13 tests)
**Focus:** execute_sql_with_results() functionality
**Key Tests:**
- Single record return
- Multiple records (10, 1000, 100000)
- All column types
- NULL values
- Dynamic column names
- Empty result sets
- SQL errors (syntax, timeout, table not found, column not found)

### Category 3: Schema Caching Tests (6 tests)
**Focus:** get_cached_schema() functionality
**Key Tests:**
- Cache miss (first access)
- Cache hit (subsequent access)
- Multiple tables caching
- Cache reuse across metrics
- Error handling with invalid tables

### Category 4: Schema Alignment Tests (11 tests)
**Focus:** align_dataframe_schema_with_bq() functionality
**Key Tests:**
- Perfect schema match
- Extra columns dropped
- Missing nullable columns added
- Type conversions (String to Date, Integer to Decimal)
- Column reordering
- Missing required columns (error cases)
- Type conversion failures

### Category 5: End-to-End Process Metrics Tests (14 tests)
**Focus:** process_metrics() full pipeline
**Key Tests:**
- Single metric single/multiple records
- Multiple metrics same/different tables
- Partition date and timestamp enrichment
- Placeholder replacement ({currently}, {partition_info})
- Schema fetch failures
- SQL execution failures
- Missing required columns in results
- Empty result sets

### Category 6: Recon Record Creation Tests (7 tests)
**Focus:** Recon record creation and validation
**Key Tests:**
- Successful metric recon records
- Multiple records written tracking
- Failed metric recon records (schema error, SQL error)
- RECON_SCHEMA unchanged validation
- None values prevention
- Multiple failure handling

### Category 7: Integration Tests (6 tests)
**Focus:** End-to-end integration scenarios
**Key Tests:**
- Backward compatibility with old JSON
- New JSON with custom columns
- Mixed metrics (old and new structure)
- Performance with schema caching
- All invalid metrics
- Mixed success and failure

### Category 8: Logging Tests (6 tests)
**Focus:** Logging and error messages
**Key Tests:**
- Cache hit/miss logging
- Multi-record count logging
- Schema validation error logging
- Error category assignment
- Error message sanitization

### Category 9: Performance Tests (5 tests)
**Focus:** Performance and scalability
**Key Tests:**
- Large result set (100K records)
- Many metrics (100 metrics)
- Schema cache efficiency
- Memory limits with huge result sets
- Timeout handling

### Category 10: Edge Cases Tests (6 tests)
**Focus:** Boundary conditions and edge cases
**Key Tests:**
- Single column table
- Wide table (50+ columns)
- All NULL results
- Completely wrong schema
- Special characters in column names
- Reserved keywords as column names

---

## Quick Start

### 1. Setup (5 minutes)
```bash
# Set environment variables
export TEST_PROJECT="test_project"
export TEST_DATASET="test_dataset"
export TEST_GCS_BUCKET="gs://your-test-bucket"

# Create dataset
bq mk --dataset ${TEST_PROJECT}:${TEST_DATASET}

# Create tables
bq query --use_legacy_sql=false < bigquery_test_schemas.sql

# Load data
bq query --use_legacy_sql=false < bigquery_test_data.sql

# Upload JSON files
gsutil cp test_metrics_positive.json ${TEST_GCS_BUCKET}/
gsutil cp test_metrics_negative.json ${TEST_GCS_BUCKET}/
```

### 2. Run High Priority Tests (30 minutes)
```bash
# Test 1: Single record
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json --run_date 2024-01-15 --dependencies test_dependency_1 --partition_info_table ${TEST_PROJECT}.${TEST_DATASET}.test_partition_info --env TEST --recon_table ${TEST_PROJECT}.${TEST_DATASET}.test_recon_table

# Test 2: Multiple records
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json --run_date 2024-01-15 --dependencies test_dependency_2 --partition_info_table ${TEST_PROJECT}.${TEST_DATASET}.test_partition_info --env TEST --recon_table ${TEST_PROJECT}.${TEST_DATASET}.test_recon_table

# Test 3: Schema caching
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_positive.json --run_date 2024-01-15 --dependencies test_dependency_9 --partition_info_table ${TEST_PROJECT}.${TEST_DATASET}.test_partition_info --env TEST --recon_table ${TEST_PROJECT}.${TEST_DATASET}.test_recon_table

# Test 4: Negative - non-existent table
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json --run_date 2024-01-15 --dependencies test_dependency_neg_1 --partition_info_table ${TEST_PROJECT}.${TEST_DATASET}.test_partition_info --env TEST --recon_table ${TEST_PROJECT}.${TEST_DATASET}.test_recon_table

# Test 5: Negative - SQL syntax error
spark-submit main.py --gcs_path ${TEST_GCS_BUCKET}/test_metrics_negative.json --run_date 2024-01-15 --dependencies test_dependency_neg_3 --partition_info_table ${TEST_PROJECT}.${TEST_DATASET}.test_partition_info --env TEST --recon_table ${TEST_PROJECT}.${TEST_DATASET}.test_recon_table
```

### 3. Validate Results (5 minutes)
```bash
# Check metrics written
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`${TEST_PROJECT}.${TEST_DATASET}.test_metrics_output\` WHERE partition_dt = '2024-01-15'"

# Check recon records
bq query --use_legacy_sql=false "SELECT module_id, rcncln_exact_pass_in, excldd_reason_tx FROM \`${TEST_PROJECT}.${TEST_DATASET}.test_recon_table\` WHERE schdld_dt = DATE('2024-01-15')"
```

---

## Test Coverage Matrix

| Component | Coverage | Tests |
|-----------|----------|-------|
| get_spark_schema_from_bq_table | 100% | 10 |
| execute_sql_with_results | 100% | 13 |
| get_cached_schema | 100% | 6 |
| align_dataframe_schema_with_bq | 100% | 11 |
| process_metrics | 100% | 14 |
| Recon record creation | 100% | 7 |
| Integration scenarios | 100% | 6 |
| Logging | 100% | 6 |
| Performance | 100% | 5 |
| Edge cases | 100% | 6 |
| **TOTAL** | **100%** | **84** |

---

## Expected Outcomes

### Positive Tests (48 tests)
✅ All metrics process successfully
✅ Data written to target tables
✅ Recon records created with 'Passed' status
✅ Schema caching reduces BigQuery API calls
✅ Custom columns supported
✅ Placeholders replaced correctly
✅ Performance meets benchmarks

### Negative Tests (36 tests)
✅ Appropriate errors raised
✅ Error messages are clear and actionable
✅ Error categories assigned correctly
✅ Recon records created with 'Failed' status
✅ Pipeline doesn't crash
✅ Error details captured in recon records

---

## Key Metrics to Track

1. **Test Pass Rate:** Target 100% for positive tests
2. **Error Detection Rate:** Target 100% for negative tests
3. **Schema Cache Hit Rate:** Target >80% for repeated table access
4. **Performance:** 
   - Single record: <5 seconds
   - 1000 records: <30 seconds
   - 100K records: <5 minutes
5. **Recon Record Accuracy:** 100% of metrics have recon records

---

## Maintenance

### Adding New Tests
1. Add row to test_scenarios.csv
2. Create metric in appropriate JSON file (positive/negative)
3. Update TEST_EXECUTION_GUIDE.md with execution steps
4. Add validation query if needed
5. Update this summary document

### Updating Existing Tests
1. Modify metric in JSON file
2. Update expected results in TEST_EXECUTION_GUIDE.md
3. Update test_scenarios.csv if description changes
4. Re-run affected tests

---

## References

- **unit_testing_scenarios.md** - Original detailed test scenario descriptions
- **TEST_EXECUTION_GUIDE.md** - Step-by-step execution instructions
- **test_scenarios.csv** - Test tracking matrix
- **Pipeline documentation** - See .kiro/steering/*.md files

---

## Support

For questions or issues:
1. Review TEST_EXECUTION_GUIDE.md for troubleshooting
2. Check pipeline logs for detailed error messages
3. Verify BigQuery table schemas match expected structure
4. Ensure GCS paths and permissions are correct

