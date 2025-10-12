# Negative Testing for DQ Pipeline

## 📋 Overview

This package provides comprehensive negative testing capabilities for the Data Quality (DQ) validation pipeline. It includes test configuration files, documentation, and implementation examples to ensure the pipeline handles validation errors gracefully.

## 🎯 Key Objective

**Test that when 20 checks are configured and 2 have validation errors, the pipeline should:**
1. ✅ Identify the 2 invalid checks with clear error messages
2. ✅ Process the 18 valid checks successfully
3. ✅ Write results to BigQuery for the 18 valid checks
4. ✅ Log errors for the 2 invalid checks
5. ✅ Complete without crashing

---

## 📁 Files Included

### Test Configuration Files

| File | Purpose | Valid | Invalid |
|------|---------|-------|---------|
| `sample_dq_config_negative_tests.json` | Realistic mixed scenario | 18 | 2 |
| `sample_dq_config_all_negative_scenarios.json` | All error types | 0 | 20 |

### Documentation Files

| File | Description |
|------|-------------|
| `NEGATIVE_TEST_SCENARIOS.md` | Detailed explanation of the 2 errors in mixed test |
| `NEGATIVE_TEST_GUIDE.md` | Comprehensive guide covering all 20 error types |
| `NEGATIVE_TEST_QUICK_REFERENCE.md` | Quick reference card for testing |
| `README_NEGATIVE_TESTING.md` | This file - overview and getting started |

### Implementation Files

| File | Description |
|------|-------------|
| `IMPLEMENTATION_EXAMPLE.py` | Example code showing how to handle partial validation |

---

## 🚀 Quick Start

### 1. Run the Mixed Scenario Test (18 valid, 2 invalid)

```bash
python main.py --config sample_dq_config_negative_tests.json \
               --run-date 2025-10-12 \
               --target-table project.dataset.dq_results \
               --project-id your-project-id
```

**Expected Result:**
- 18 checks execute successfully
- 2 checks fail validation
- 18 rows written to BigQuery
- Clear error messages for the 2 failed checks

### 2. Verify Results in BigQuery

```sql
-- Should return 18 rows
SELECT COUNT(*) as processed_checks
FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%'
  AND DATE(execution_timestamp) = CURRENT_DATE();

-- List all processed checks (should be missing CHK_NEG_002 and CHK_NEG_006)
SELECT check_id, validation_status
FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%'
  AND DATE(execution_timestamp) = CURRENT_DATE()
ORDER BY check_id;
```

### 3. Test All Error Scenarios (0 valid, 20 invalid)

```bash
python main.py --config sample_dq_config_all_negative_scenarios.json \
               --run-date 2025-10-12 \
               --target-table project.dataset.dq_results \
               --project-id your-project-id
```

**Expected Result:**
- 0 checks execute (all invalid)
- 20 validation errors reported
- 0 rows written to BigQuery
- 20 unique error messages

---

## 🔍 The 2 Validation Errors in Mixed Test

### Error 1: Invalid Severity
**Check ID:** `CHK_NEG_002_INVALID_SEVERITY`

**Problem:**
```json
"severity": "Critical"  // ❌ Invalid - must be High, Medium, or Low
```

**Expected Error:**
```
ValidationError: Invalid severity 'Critical'. Must be one of: High, Medium, Low
```

### Error 2: Type Mismatch
**Check ID:** `CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH`

**Problem:**
```json
"comparison_type": "numeric_condition",  // Expects string
"expected_output": ["APAC", "EMEA", "Americas"]  // ❌ But got list
```

**Expected Error:**
```
ValidationError: For comparison_type 'numeric_condition', expected_output must be a string, got list
```

---

## 📚 Documentation Guide

### For Quick Testing
👉 Start with: **`NEGATIVE_TEST_QUICK_REFERENCE.md`**
- Quick commands
- Expected results
- Verification queries

### For Understanding Errors
👉 Read: **`NEGATIVE_TEST_SCENARIOS.md`**
- Detailed explanation of the 2 errors
- Expected system behavior
- Phase-by-phase breakdown

### For Comprehensive Testing
👉 Read: **`NEGATIVE_TEST_GUIDE.md`**
- All 20 error types
- Complete error message reference
- Testing strategy
- Implementation requirements

### For Implementation
👉 Review: **`IMPLEMENTATION_EXAMPLE.py`**
- Code examples
- Individual check validation
- Error handling patterns
- Expected output examples

---

## 🛠️ Implementation Requirements

To support partial validation (processing valid checks when some fail), your code needs:

### 1. Individual Check Validation
```python
valid_checks = []
validation_errors = []

for index, check in enumerate(config_data):
    try:
        ValidationEngine.validate_dq_record(check, index)
        valid_checks.append(check)
    except ValidationError as e:
        validation_errors.append({
            'check_id': check.get('check_id', f'Unknown_{index}'),
            'error': str(e)
        })
```

### 2. Continue Processing Valid Checks
```python
if valid_checks:
    results = execute_dq_checks(valid_checks)
    write_to_bigquery(results)
```

### 3. Report All Errors
```python
if validation_errors:
    for error in validation_errors:
        print(f"❌ {error['check_id']}: {error['error']}")
    sys.exit(1)
```

See `IMPLEMENTATION_EXAMPLE.py` for complete implementation.

---

## ✅ Success Criteria

### For Mixed Test (18 valid, 2 invalid)

**✅ PASS if:**
- Pipeline completes without crashing
- 18 checks execute and write to BigQuery
- 2 validation errors reported with clear messages
- Exit code is non-zero (indicating errors occurred)

**❌ FAIL if:**
- Pipeline crashes on first error
- Valid checks are not executed
- No results written to BigQuery
- Error messages are unclear
- Exit code is zero (should be non-zero when errors occur)

### For All Errors Test (0 valid, 20 invalid)

**✅ PASS if:**
- Pipeline completes without crashing
- 0 checks execute (all invalid)
- 20 unique validation errors reported
- Exit code is non-zero

**❌ FAIL if:**
- Pipeline crashes
- Any checks execute (all should be invalid)
- Error messages are missing or unclear

---

## 🎓 All 20 Error Types (Comprehensive Test)

The `sample_dq_config_all_negative_scenarios.json` file covers:

1. **Missing Fields** - Missing required field (check_id)
2-3. **Invalid Severity** - "Critical", "Urgent" (valid: High, Medium, Low)
4. **Invalid Comparison Type** - "exact_match" (valid: numeric_condition, set_match, not_in_result, row_match)
5-6. **numeric_condition Type Mismatches** - List, object (should be string)
7-8. **set_match Type Mismatches** - String, empty list (should be non-empty list)
9-10. **not_in_result Type Mismatches** - String, empty list (should be non-empty list)
11-13. **row_match Type Mismatches** - String, empty list, string items (should be list of objects)
14-15. **Invalid Numeric Conditions** - "at least ten", "=>1000" (invalid format)
16-17. **Empty Strings** - Empty sql_query, empty check_id
18-19. **Invalid Boolean** - "true" (string), 1 (number) for active field
20. **Invalid Type** - Number for severity field

See `NEGATIVE_TEST_GUIDE.md` for detailed descriptions of each error.

---

## 🔧 Troubleshooting

### Issue: Pipeline crashes on first validation error
**Solution:** Implement individual check validation (see `IMPLEMENTATION_EXAMPLE.py`)

### Issue: Valid checks not executing when errors present
**Solution:** Separate validation phase from execution phase

### Issue: Unclear error messages
**Solution:** Include check_id, index, and specific error details

### Issue: All checks fail even valid ones
**Solution:** Review ValidationEngine logic for false positives

### Issue: No results in BigQuery
**Solution:** Check BigQuery write logic and permissions

---

## 📊 Test Results Summary

After running the mixed test, you should see:

```
================================================================================
VALIDATION SUMMARY
================================================================================
Total checks: 20
Valid checks: 18
Invalid checks: 2
Success rate: 90.0%
================================================================================

Processing 18 valid checks...
✅ Successfully processed 18 checks
✅ Results written to BigQuery

❌ 2 Validation Errors:
  1. CHK_NEG_002_INVALID_SEVERITY: Invalid severity 'Critical'
  2. CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH: Type mismatch

⚠️  Pipeline completed with 2 validation errors
⚠️  18 valid checks were processed successfully
```

---

## 📞 Next Steps

1. **Run the mixed test** to verify your pipeline handles partial validation
2. **Check BigQuery** to confirm 18 rows were written
3. **Review error logs** to ensure clear error messages
4. **Run the comprehensive test** to verify all error types are handled
5. **Update your code** if needed using `IMPLEMENTATION_EXAMPLE.py` as reference

---

## 📖 Additional Resources

- **Validation Rules:** See `validation.py` for all validation logic
- **Schema Definition:** See `dq_config_schema.json` for JSON schema
- **Sample Valid Configs:** See `sample_dq_config.json` and `sample_dq_config_comprehensive.json`

---

## 🎯 Summary

This negative testing package ensures your DQ pipeline is:
- **Robust** - Handles errors gracefully without crashing
- **Resilient** - Continues processing valid checks when some fail
- **Clear** - Provides detailed error messages for debugging
- **Reliable** - Writes results for valid checks even when errors occur

Use these tests to build confidence in your pipeline's error handling capabilities!

---

**Created:** 2025-10-12  
**Version:** 1.0  
**Author:** Kiro AI Assistant
