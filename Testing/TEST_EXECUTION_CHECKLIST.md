# Test Execution Checklist

Use this checklist to track your testing progress. Mark items as complete as you execute tests.

---

## Pre-Test Setup

- [ ] BigQuery test project created
- [ ] Test dataset created (test_project.test_dataset)
- [ ] All 14 test tables created (run bigquery_test_schemas.sql)
- [ ] Test data loaded (run bigquery_test_data.sql)
- [ ] GCS bucket accessible
- [ ] test_metrics_positive.json uploaded to GCS
- [ ] test_metrics_negative.json uploaded to GCS
- [ ] Environment variables set (TEST_PROJECT, TEST_DATASET, TEST_GCS_BUCKET)
- [ ] Spark environment configured
- [ ] Pipeline code deployed

---

## High Priority Tests (Must Execute First)

### Schema Conversion Tests
- [ ] **1.1** All supported data type mappings (TEST_005)
- [ ] **1.2** Nullable field preservation
- [ ] **1.3** Required field preservation
- [ ] **1.4** Simple table schema (3 columns)
- [ ] **1.6** Non-existent table error (NEG_001) ❌

### Multi-Record SQL Tests
- [ ] **2.1** Single record return (TEST_001)
- [ ] **2.2** Multiple records - 10 rows (TEST_002)
- [ ] **2.3** Multiple records - 1000 rows (TEST_008)
- [ ] **2.5** All column types in results (TEST_005)
- [ ] **2.6** NULL values in results (TEST_006)
- [ ] **2.7** Dynamic column names (TEST_007)
- [ ] **2.8** Empty result set (NEG_006) ❌
- [ ] **2.9** SQL syntax error (NEG_003) ❌

### Schema Caching Tests
- [ ] **3.1** Cache miss - first access (TEST_009)
- [ ] **3.2** Cache hit - subsequent access (TEST_010)
- [ ] **3.3** Multiple tables caching (TEST_009, TEST_010, TEST_011)
- [ ] **3.4** Cache reuse across metrics (TEST_009, TEST_010)

### Schema Alignment Tests
- [ ] **4.1** Perfect schema match (TEST_001)
- [ ] **4.2** Extra columns dropped
- [ ] **4.3** Missing nullable columns added
- [ ] **4.7** Missing required column error (NEG_007) ❌
- [ ] **4.8** Multiple missing required columns (NEG_008) ❌

### End-to-End Tests
- [ ] **5.1** Single metric single record (TEST_001)
- [ ] **5.2** Single metric multiple records (TEST_002)
- [ ] **5.3** Multiple metrics same table (TEST_009, TEST_010)
- [ ] **5.5** Partition date added to all records
- [ ] **5.6** Pipeline execution timestamp added
- [ ] **5.7** {currently} placeholder replacement (TEST_003)
- [ ] **5.8** {partition_info} placeholder replacement (TEST_004)

### Recon Record Tests
- [ ] **6.1** Recon record for successful metric (TEST_001)
- [ ] **6.3** Recon record for failed metric - schema error (NEG_007) ❌
- [ ] **6.4** Recon record for failed metric - SQL error (NEG_003) ❌

---

## Medium Priority Tests

### Schema Conversion Tests
- [ ] **1.5** Complex table schema (25 columns)
- [ ] **1.7** Invalid table name format (NEG_002) ❌
- [ ] **1.8** Unsupported data type ❌
- [ ] **1.10** Permission denied ❌

### Multi-Record SQL Tests
- [ ] **2.4** Multiple records - 100K rows (large volume)
- [ ] **2.10** SQL timeout ❌
- [ ] **2.11** Table not found in SQL (NEG_004) ❌
- [ ] **2.12** Column not found in SQL (NEG_005) ❌

### Schema Caching Tests
- [ ] **3.5** Cache with invalid table (NEG_001) ❌
- [ ] **3.6** Cache after schema fetch failure ❌

### Schema Alignment Tests
- [ ] **4.4** Type conversion - String to Date
- [ ] **4.5** Type conversion - Integer to Decimal
- [ ] **4.6** Column reordering
- [ ] **4.9** Type conversion failure (NEG_009) ❌

### End-to-End Tests
- [ ] **5.4** Multiple metrics different tables (TEST_009, TEST_011)
- [ ] **5.9** Schema fetch failure for one metric ❌
- [ ] **5.10** SQL execution failure for one metric ❌
- [ ] **5.11** Missing required columns in SQL results ❌
- [ ] **5.13** Empty SQL result set (NEG_006) ❌
- [ ] **5.14** Type mismatch in SQL results ❌

### Recon Record Tests
- [ ] **6.2** Recon record with multiple records written (TEST_008)
- [ ] **6.5** RECON_SCHEMA unchanged validation
- [ ] **6.6** Recon record with None values prevented ❌
- [ ] **6.7** Recon record for multiple failures ❌

### Integration Tests
- [ ] **7.1** Backward compatibility with old JSON
- [ ] **7.2** New JSON with custom columns (TEST_007)
- [ ] **7.4** Performance with schema caching
- [ ] **7.6** Mixed success and failure ❌

### Logging Tests
- [ ] **8.1** Schema cache hit logging
- [ ] **8.2** Schema cache miss logging
- [ ] **8.3** Multi-record count logging
- [ ] **8.4** Schema validation error logging ❌

### Performance Tests
- [ ] **9.1** Large result set - 100K records
- [ ] **9.2** Many metrics - 100 metrics
- [ ] **9.3** Schema cache efficiency

---

## Low Priority Tests (Nice to Have)

### Schema Conversion Tests
- [ ] **1.9** Empty schema ❌

### Multi-Record SQL Tests
- [ ] **2.13** Extremely large result set - 1M+ records ❌

### Schema Alignment Tests
- [ ] **4.10** Empty DataFrame ❌
- [ ] **4.11** All columns missing (NEG_010) ❌

### End-to-End Tests
- [ ] **5.12** All metrics fail ❌

### Integration Tests
- [ ] **7.3** Mixed metrics (old and new structure)
- [ ] **7.5** Pipeline with all invalid metrics ❌

### Logging Tests
- [ ] **8.5** Error category assignment ❌
- [ ] **8.6** Error message sanitization ❌

### Performance Tests
- [ ] **9.4** Memory limit with huge result set ❌
- [ ] **9.5** Timeout handling ❌

### Edge Cases Tests
- [ ] **10.1** Single column table
- [ ] **10.2** Wide table (50+ columns)
- [ ] **10.3** All NULL result
- [ ] **10.4** Completely wrong schema (NEG_010) ❌
- [ ] **10.5** Special characters in column names ❌
- [ ] **10.6** Reserved keywords as column names ❌

---

## Post-Test Validation

### Data Validation
- [ ] Verify metrics written to test_metrics_output
- [ ] Verify custom metrics written to test_custom_metrics
- [ ] Verify recon records created for all tests
- [ ] Verify partition_dt added to all records
- [ ] Verify pipeline_execution_ts added to all records

### Recon Validation
- [ ] All successful tests have rcncln_exact_pass_in = 'Passed'
- [ ] All failed tests have rcncln_exact_pass_in = 'Failed'
- [ ] All failed tests have error details in excldd_reason_tx
- [ ] All failed tests have error category assigned
- [ ] Record counts match in source_vl and target_vl

### Performance Validation
- [ ] Single record test completed in < 5 seconds
- [ ] 10 records test completed in < 10 seconds
- [ ] 1000 records test completed in < 30 seconds
- [ ] 100K records test completed in < 5 minutes
- [ ] Schema cache reduced BigQuery API calls

### Log Validation
- [ ] Cache hit messages present in logs
- [ ] Cache miss messages present in logs
- [ ] Record count messages present in logs
- [ ] Error messages are clear and actionable
- [ ] No unexpected errors or warnings

---

## Test Results Summary

### Positive Tests
- Total Positive Tests: 48
- Tests Passed: _____ / 48
- Tests Failed: _____ / 48
- Pass Rate: _____ %

### Negative Tests
- Total Negative Tests: 36
- Tests Passed (error detected): _____ / 36
- Tests Failed (error not detected): _____ / 36
- Detection Rate: _____ %

### Overall
- Total Tests: 84
- Tests Executed: _____ / 84
- Tests Passed: _____ / 84
- Overall Success Rate: _____ %

---

## Issues Found

### Critical Issues
1. _____________________________________________________
2. _____________________________________________________
3. _____________________________________________________

### Major Issues
1. _____________________________________________________
2. _____________________________________________________
3. _____________________________________________________

### Minor Issues
1. _____________________________________________________
2. _____________________________________________________
3. _____________________________________________________

---

## Performance Metrics

| Test | Expected Time | Actual Time | Status |
|------|---------------|-------------|--------|
| Single record | < 5 sec | _____ sec | _____ |
| 10 records | < 10 sec | _____ sec | _____ |
| 1000 records | < 30 sec | _____ sec | _____ |
| 100K records | < 5 min | _____ min | _____ |
| Schema cache hit | < 1 sec | _____ sec | _____ |
| Schema cache miss | < 5 sec | _____ sec | _____ |

---

## Schema Cache Efficiency

- Unique target tables: _____
- Total metrics processed: _____
- Expected BigQuery schema fetch calls: _____
- Actual BigQuery schema fetch calls: _____
- Cache hit rate: _____ %
- Time saved by caching: _____ seconds

---

## Sign-Off

### Tester Information
- Name: _____________________________
- Date: _____________________________
- Environment: _____________________________

### Test Completion
- [ ] All high priority tests completed
- [ ] All medium priority tests completed
- [ ] All low priority tests completed
- [ ] All validation checks passed
- [ ] Test results documented
- [ ] Issues logged and tracked
- [ ] Performance metrics recorded

### Approval
- [ ] Tests meet acceptance criteria
- [ ] Ready for production deployment
- [ ] Documentation updated

**Tester Signature:** _____________________________

**Date:** _____________________________

---

## Notes

_Use this section for any additional notes, observations, or recommendations_

_______________________________________________________________
_______________________________________________________________
_______________________________________________________________
_______________________________________________________________
_______________________________________________________________

