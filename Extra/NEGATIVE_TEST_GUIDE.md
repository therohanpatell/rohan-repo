# Complete Negative Testing Guide

## Overview
This guide provides comprehensive negative test scenarios for the Data Quality (DQ) validation pipeline. It includes multiple test configuration files with various error scenarios to ensure robust error handling.

---

## Test Configuration Files

### 1. `sample_dq_config_negative_tests.json`
**Purpose:** Realistic mixed scenario with 18 valid and 2 invalid checks

**Use Case:** Test that the pipeline can process valid checks while gracefully handling validation errors

**Contents:**
- 20 total checks
- 18 valid checks (should execute and write to BigQuery)
- 2 invalid checks (should fail validation with clear errors)

**Invalid Checks:**
1. `CHK_NEG_002_INVALID_SEVERITY` - Uses "Critical" instead of High/Medium/Low
2. `CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH` - numeric_condition with list expected_output

**Expected Behavior:**
- ✅ 18 checks execute successfully
- ✅ 18 result rows written to BigQuery
- ❌ 2 checks fail validation
- ✅ Clear error messages for failed checks
- ✅ Pipeline completes without crashing

---

### 2. `sample_dq_config_all_negative_scenarios.json`
**Purpose:** Comprehensive collection of all possible validation errors

**Use Case:** Test all validation error paths and error messages

**Contents:**
- 20 checks, each demonstrating a different validation error
- Covers all validation rules in the ValidationEngine

**Error Categories:**

#### A. Missing Required Fields
- `CHK_ERR_001_MISSING_CHECK_ID` - Missing check_id field

#### B. Invalid Severity Values
- `CHK_ERR_002_INVALID_SEVERITY_CRITICAL` - severity: "Critical"
- `CHK_ERR_003_INVALID_SEVERITY_URGENT` - severity: "Urgent"
- `CHK_ERR_020_SEVERITY_NOT_STRING` - severity: 1 (number)

#### C. Invalid Comparison Types
- `CHK_ERR_004_INVALID_COMPARISON_TYPE` - comparison_type: "exact_match"

#### D. Type Mismatches for numeric_condition
- `CHK_ERR_005_NUMERIC_WITH_LIST` - expected_output is list
- `CHK_ERR_006_NUMERIC_WITH_OBJECT` - expected_output is object
- `CHK_ERR_014_INVALID_NUMERIC_CONDITION_TEXT` - expected_output: "at least ten"
- `CHK_ERR_015_INVALID_NUMERIC_OPERATOR` - expected_output: "=>1000" (invalid operator)

#### E. Type Mismatches for set_match
- `CHK_ERR_007_SET_MATCH_WITH_STRING` - expected_output is string
- `CHK_ERR_008_SET_MATCH_EMPTY_LIST` - expected_output is empty list

#### F. Type Mismatches for not_in_result
- `CHK_ERR_009_NOT_IN_RESULT_WITH_STRING` - expected_output is string
- `CHK_ERR_010_NOT_IN_RESULT_EMPTY_LIST` - expected_output is empty list

#### G. Type Mismatches for row_match
- `CHK_ERR_011_ROW_MATCH_WITH_STRING` - expected_output is string
- `CHK_ERR_012_ROW_MATCH_EMPTY_LIST` - expected_output is empty list
- `CHK_ERR_013_ROW_MATCH_WITH_STRING_ITEMS` - expected_output contains strings not objects

#### H. Empty String Values
- `CHK_ERR_016_EMPTY_SQL_QUERY` - sql_query: ""
- `CHK_ERR_017_EMPTY_CHECK_ID` - check_id: ""

#### I. Invalid Boolean Values
- `CHK_ERR_018_ACTIVE_NOT_BOOLEAN_STRING` - active: "true" (string)
- `CHK_ERR_019_ACTIVE_NOT_BOOLEAN_NUMBER` - active: 1 (number)

---

## Expected Error Messages Reference

### Severity Errors
```
ValidationError: Invalid severity 'Critical'. Must be one of: High, Medium, Low
ValidationError: Invalid severity 'Urgent'. Must be one of: High, Medium, Low
ValidationError: Severity must be a string, got int
```

### Comparison Type Errors
```
ValidationError: Invalid comparison_type 'exact_match'. Must be one of: numeric_condition, set_match, not_in_result, row_match
```

### Type Mismatch Errors - numeric_condition
```
ValidationError: For comparison_type 'numeric_condition', expected_output must be a string, got list
ValidationError: For comparison_type 'numeric_condition', expected_output must be a string, got dict
ValidationError: For comparison_type 'numeric_condition', expected_output 'at least ten' must be a valid number or condition (e.g., '0', '>=10')
ValidationError: For comparison_type 'numeric_condition', expected_output '=>1000' has invalid numeric value after operator
```

### Type Mismatch Errors - set_match
```
ValidationError: For comparison_type 'set_match', expected_output must be a list, got str
ValidationError: For comparison_type 'set_match', expected_output list cannot be empty
```

### Type Mismatch Errors - not_in_result
```
ValidationError: For comparison_type 'not_in_result', expected_output must be a list, got str
ValidationError: For comparison_type 'not_in_result', expected_output list cannot be empty
```

### Type Mismatch Errors - row_match
```
ValidationError: For comparison_type 'row_match', expected_output must be a list, got str
ValidationError: For comparison_type 'row_match', expected_output list cannot be empty
ValidationError: For comparison_type 'row_match', expected_output must be a list of objects. Item at index 0 is str, not a dictionary
```

### Empty String Errors
```
ValidationError: Record at index X: 'sql_query' must be a non-empty string
ValidationError: Record at index X: 'check_id' must be a non-empty string
```

### Boolean Type Errors
```
ValidationError: Record at index X: 'active' field must be a boolean, got str
ValidationError: Record at index X: 'active' field must be a boolean, got int
```

### Missing Field Errors
```
ValidationError: Record at index X missing required fields: check_id
ValidationError: Record at index X missing required fields: severity, expected_output
```

---

## Testing Strategy

### Test 1: Mixed Valid/Invalid Scenario
**File:** `sample_dq_config_negative_tests.json`

**Command:**
```bash
python main.py --config sample_dq_config_negative_tests.json
```

**Validation Checklist:**
- [ ] Pipeline does not crash
- [ ] 18 valid checks execute
- [ ] 18 rows written to BigQuery
- [ ] 2 validation errors logged
- [ ] Error messages clearly identify CHK_NEG_002 and CHK_NEG_006
- [ ] Error messages explain what's wrong
- [ ] Exit code indicates errors occurred (non-zero)

**BigQuery Verification:**
```sql
-- Should return 18 rows
SELECT COUNT(*) as processed_checks
FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%'
  AND DATE(execution_timestamp) = CURRENT_DATE();

-- Should return 18 rows (missing CHK_NEG_002 and CHK_NEG_006)
SELECT check_id
FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%'
  AND DATE(execution_timestamp) = CURRENT_DATE()
ORDER BY check_id;
```

---

### Test 2: All Error Scenarios
**File:** `sample_dq_config_all_negative_scenarios.json`

**Command:**
```bash
python main.py --config sample_dq_config_all_negative_scenarios.json
```

**Validation Checklist:**
- [ ] Pipeline does not crash
- [ ] 0 checks execute (all invalid)
- [ ] 0 rows written to BigQuery
- [ ] 20 validation errors logged
- [ ] Each error message is unique and descriptive
- [ ] All error types are covered
- [ ] Exit code indicates errors occurred (non-zero)

**Error Message Verification:**
Check that all 20 different error messages appear in the output.

---

## Implementation Requirements

### 1. Individual Check Validation
The pipeline must validate each check independently:

```python
def load_and_validate_config(config_file):
    """Load config and validate each check individually"""
    with open(config_file, 'r') as f:
        config_data = json.load(f)
    
    valid_checks = []
    validation_errors = []
    
    for index, check in enumerate(config_data):
        try:
            # Validate the check
            ValidationEngine.validate_dq_record(check, index)
            valid_checks.append(check)
            print(f"✅ Check {index + 1}/{len(config_data)}: {check.get('check_id', 'Unknown')} - VALID")
        except ValidationError as e:
            error_info = {
                'index': index,
                'check_id': check.get('check_id', f'Unknown_{index}'),
                'error': str(e)
            }
            validation_errors.append(error_info)
            print(f"❌ Check {index + 1}/{len(config_data)}: {check.get('check_id', 'Unknown')} - FAILED")
            print(f"   Error: {e}")
    
    return valid_checks, validation_errors
```

### 2. Continue Processing Valid Checks
```python
def main():
    valid_checks, validation_errors = load_and_validate_config(config_file)
    
    print(f"\nValidation Summary:")
    print(f"  Total checks: {len(valid_checks) + len(validation_errors)}")
    print(f"  Valid checks: {len(valid_checks)}")
    print(f"  Invalid checks: {len(validation_errors)}")
    
    # Process valid checks even if some failed validation
    if valid_checks:
        print(f"\nProcessing {len(valid_checks)} valid checks...")
        results = execute_dq_checks(valid_checks)
        write_results_to_bigquery(results)
        print(f"✅ Successfully processed {len(results)} checks")
    else:
        print("\n⚠️  No valid checks to process")
    
    # Report validation errors
    if validation_errors:
        print(f"\n❌ {len(validation_errors)} Validation Errors:")
        for error in validation_errors:
            print(f"\n  Check ID: {error['check_id']}")
            print(f"  Error: {error['error']}")
        
        # Exit with error code
        sys.exit(1)
    else:
        print("\n✅ All checks validated successfully")
        sys.exit(0)
```

### 3. Error Logging
```python
def log_validation_errors(validation_errors, log_file='validation_errors.log'):
    """Log validation errors to file for debugging"""
    if not validation_errors:
        return
    
    with open(log_file, 'a') as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"Validation Errors - {datetime.now().isoformat()}\n")
        f.write(f"{'='*80}\n")
        
        for error in validation_errors:
            f.write(f"\nCheck ID: {error['check_id']}\n")
            f.write(f"Index: {error['index']}\n")
            f.write(f"Error: {error['error']}\n")
            f.write(f"{'-'*80}\n")
```

---

## Test Execution Workflow

### Step 1: Run Mixed Scenario Test
```bash
# Run the test
python main.py --config sample_dq_config_negative_tests.json

# Expected output:
# Validating check 1/20: CHK_NEG_001_VALID_NULL_CHECK ✅ VALID
# Validating check 2/20: CHK_NEG_002_INVALID_SEVERITY ❌ FAILED
#   Error: Invalid severity 'Critical'. Must be one of: High, Medium, Low
# ...
# Validation Summary:
#   Total checks: 20
#   Valid checks: 18
#   Invalid checks: 2
# Processing 18 valid checks...
# ✅ Successfully processed 18 checks
# ❌ 2 Validation Errors:
#   Check ID: CHK_NEG_002_INVALID_SEVERITY
#   Error: Invalid severity 'Critical'. Must be one of: High, Medium, Low
#   Check ID: CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH
#   Error: For comparison_type 'numeric_condition', expected_output must be a string, got list
```

### Step 2: Verify BigQuery Results
```bash
# Query BigQuery to verify 18 rows were written
bq query --use_legacy_sql=false "
SELECT 
  check_id,
  check_status,
  execution_timestamp
FROM \`project.dataset.dq_results\`
WHERE check_id LIKE 'CHK_NEG_%'
  AND DATE(execution_timestamp) = CURRENT_DATE()
ORDER BY check_id
"
```

### Step 3: Run All Error Scenarios Test
```bash
# Run the comprehensive error test
python main.py --config sample_dq_config_all_negative_scenarios.json

# Expected output:
# Validating check 1/20: CHK_ERR_001_MISSING_CHECK_ID ❌ FAILED
# ...
# Validation Summary:
#   Total checks: 20
#   Valid checks: 0
#   Invalid checks: 20
# ⚠️  No valid checks to process
# ❌ 20 Validation Errors:
# [List of all 20 errors]
```

---

## Success Criteria

### For Mixed Scenario Test (sample_dq_config_negative_tests.json)
✅ **PASS** if:
- Pipeline completes without crashing
- 18 checks execute and write to BigQuery
- 2 validation errors are reported with clear messages
- Exit code is non-zero (indicating errors)

❌ **FAIL** if:
- Pipeline crashes on first error
- Valid checks are not executed
- No results written to BigQuery
- Error messages are unclear or missing
- Exit code is zero (indicating success when errors occurred)

### For All Error Scenarios Test (sample_dq_config_all_negative_scenarios.json)
✅ **PASS** if:
- Pipeline completes without crashing
- 0 checks execute (all invalid)
- 20 unique validation errors are reported
- Each error message is descriptive and accurate
- Exit code is non-zero

❌ **FAIL** if:
- Pipeline crashes
- Any checks execute (all should be invalid)
- Error messages are missing or unclear
- Exit code is zero

---

## Troubleshooting

### Issue: Pipeline crashes on first validation error
**Solution:** Wrap validation in try-except and collect all errors before processing

### Issue: Valid checks not executing when errors present
**Solution:** Separate validation phase from execution phase

### Issue: Unclear error messages
**Solution:** Include check_id, index, and specific error details in messages

### Issue: All checks fail even valid ones
**Solution:** Review ValidationEngine logic for false positives

### Issue: No results in BigQuery
**Solution:** Check BigQuery write logic and permissions

---

## Additional Test Scenarios

### Create Custom Negative Tests
You can create your own negative test files by:

1. Copy `sample_dq_config_negative_tests.json`
2. Modify specific checks to introduce errors
3. Document expected errors
4. Run and verify behavior

### Example: Test Missing Multiple Fields
```json
{
  "check_id": "CHK_CUSTOM_001",
  "sql_query": "SELECT COUNT(*) FROM table",
  "description": "Missing severity, expected_output, comparison_type, active"
}
```

Expected Error:
```
ValidationError: Record at index 0 missing required fields: severity, expected_output, comparison_type, active
```

---

## Summary

This comprehensive negative testing guide provides:
- ✅ Two test configuration files with different scenarios
- ✅ Complete error message reference
- ✅ Implementation requirements
- ✅ Testing workflow and verification steps
- ✅ Success criteria and troubleshooting

Use these tests to ensure your DQ pipeline is robust, resilient, and provides clear error messages when configuration issues occur.
