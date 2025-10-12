# Negative Test Quick Reference Card

## 🎯 Quick Test Commands

### Test 1: Mixed Valid/Invalid (18 valid, 2 invalid)
```bash
python main.py --config sample_dq_config_negative_tests.json
```
**Expected:** 18 checks execute, 2 fail validation

### Test 2: All Invalid (0 valid, 20 invalid)
```bash
python main.py --config sample_dq_config_all_negative_scenarios.json
```
**Expected:** 0 checks execute, 20 fail validation

---

## 📊 Test Files Summary

| File | Total | Valid | Invalid | Purpose |
|------|-------|-------|---------|---------|
| `sample_dq_config_negative_tests.json` | 20 | 18 | 2 | Realistic mixed scenario |
| `sample_dq_config_all_negative_scenarios.json` | 20 | 0 | 20 | All error types |

---

## ❌ The 2 Errors in Mixed Test

### Error 1: CHK_NEG_002_INVALID_SEVERITY
```json
"severity": "Critical"  // ❌ Should be: High, Medium, or Low
```
**Expected Error:** `Invalid severity 'Critical'. Must be one of: High, Medium, Low`

### Error 2: CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH
```json
"comparison_type": "numeric_condition",  // Expects string
"expected_output": ["APAC", "EMEA", "Americas"]  // ❌ But got list
```
**Expected Error:** `For comparison_type 'numeric_condition', expected_output must be a string, got list`

---

## ✅ Expected Behavior

### Pipeline Should:
- ✅ NOT crash on validation errors
- ✅ Process all valid checks
- ✅ Write valid results to BigQuery
- ✅ Log clear error messages for invalid checks
- ✅ Return non-zero exit code when errors occur

### Pipeline Should NOT:
- ❌ Stop on first error
- ❌ Skip valid checks when errors exist
- ❌ Crash or hang
- ❌ Write invalid checks to BigQuery
- ❌ Return zero exit code when errors occur

---

## 🔍 Verification Queries

### Check Results in BigQuery
```sql
-- Count processed checks (should be 18 for mixed test)
SELECT COUNT(*) as processed_checks
FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%'
  AND DATE(execution_timestamp) = CURRENT_DATE();

-- List all processed checks
SELECT check_id, check_status, execution_timestamp
FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%'
  AND DATE(execution_timestamp) = CURRENT_DATE()
ORDER BY check_id;

-- Verify missing checks (should be CHK_NEG_002 and CHK_NEG_006)
WITH all_checks AS (
  SELECT 'CHK_NEG_' || LPAD(CAST(num AS STRING), 3, '0') || '_' as check_prefix
  FROM UNNEST(GENERATE_ARRAY(1, 20)) as num
)
SELECT check_prefix
FROM all_checks
WHERE check_prefix NOT IN (
  SELECT SUBSTR(check_id, 1, 12)
  FROM `project.dataset.dq_results`
  WHERE check_id LIKE 'CHK_NEG_%'
    AND DATE(execution_timestamp) = CURRENT_DATE()
);
```

---

## 🐛 All 20 Error Types (Comprehensive Test)

| # | Check ID | Error Type |
|---|----------|------------|
| 1 | CHK_ERR_001 | Missing required field (check_id) |
| 2 | CHK_ERR_002 | Invalid severity: "Critical" |
| 3 | CHK_ERR_003 | Invalid severity: "Urgent" |
| 4 | CHK_ERR_004 | Invalid comparison_type: "exact_match" |
| 5 | CHK_ERR_005 | numeric_condition with list |
| 6 | CHK_ERR_006 | numeric_condition with object |
| 7 | CHK_ERR_007 | set_match with string |
| 8 | CHK_ERR_008 | set_match with empty list |
| 9 | CHK_ERR_009 | not_in_result with string |
| 10 | CHK_ERR_010 | not_in_result with empty list |
| 11 | CHK_ERR_011 | row_match with string |
| 12 | CHK_ERR_012 | row_match with empty list |
| 13 | CHK_ERR_013 | row_match with string items |
| 14 | CHK_ERR_014 | numeric_condition with text: "at least ten" |
| 15 | CHK_ERR_015 | numeric_condition with invalid operator: "=>1000" |
| 16 | CHK_ERR_016 | Empty sql_query |
| 17 | CHK_ERR_017 | Empty check_id |
| 18 | CHK_ERR_018 | active: "true" (string not boolean) |
| 19 | CHK_ERR_019 | active: 1 (number not boolean) |
| 20 | CHK_ERR_020 | severity: 1 (number not string) |

---

## 📝 Validation Rules Reference

### Required Fields
- check_id
- category
- sql_query
- description
- severity
- expected_output
- comparison_type
- active

### Valid Severities
- High
- Medium
- Low

### Valid Comparison Types
- numeric_condition
- set_match
- not_in_result
- row_match

### Expected Output Format by Comparison Type

| Comparison Type | Expected Output Type | Example |
|----------------|---------------------|---------|
| numeric_condition | string | "0", ">=10", "<100", "!=0" |
| set_match | list of strings | ["A", "B", "C"] |
| not_in_result | list of strings | ["INVALID", "ERROR"] |
| row_match | list of objects | [{"col": "val", "cnt": 10}] |

---

## 🚀 Quick Test Checklist

### Before Running Tests
- [ ] BigQuery credentials configured
- [ ] DQ results table exists
- [ ] Python dependencies installed
- [ ] Test config files present

### After Running Mixed Test
- [ ] Pipeline completed without crash
- [ ] Console shows 18 valid, 2 invalid
- [ ] 2 error messages displayed
- [ ] BigQuery has 18 new rows
- [ ] CHK_NEG_002 and CHK_NEG_006 not in BigQuery
- [ ] Exit code is non-zero

### After Running All Errors Test
- [ ] Pipeline completed without crash
- [ ] Console shows 0 valid, 20 invalid
- [ ] 20 unique error messages displayed
- [ ] BigQuery has 0 new rows
- [ ] Exit code is non-zero

---

## 💡 Common Issues & Quick Fixes

| Issue | Quick Fix |
|-------|-----------|
| Pipeline crashes | Add try-except around validation |
| Valid checks not running | Separate validation from execution |
| No BigQuery results | Check write_results_to_bigquery() |
| Unclear errors | Include check_id in error messages |
| Exit code always 0 | Add sys.exit(1) when errors exist |

---

## 📞 Need More Details?

- Full documentation: `NEGATIVE_TEST_GUIDE.md`
- Detailed scenarios: `NEGATIVE_TEST_SCENARIOS.md`
- Test files:
  - `sample_dq_config_negative_tests.json` (mixed)
  - `sample_dq_config_all_negative_scenarios.json` (all errors)

---

## 🎓 Key Takeaways

1. **Validate individually** - Check each config item separately
2. **Continue on error** - Don't stop on first validation failure
3. **Clear messages** - Include check_id and specific error details
4. **Separate phases** - Validate first, then execute valid checks
5. **Proper exit codes** - Non-zero when errors occur

---

**Last Updated:** 2025-10-12
