# Negative Test Scenarios Documentation

## Overview
This document describes the negative test configuration file (`sample_dq_config_negative_tests.json`) which contains 20 data quality checks where **2 checks have intentional validation errors** and **18 checks are valid**.

The purpose is to test the system's ability to:
1. **Identify and report validation errors** for invalid checks
2. **Continue processing valid checks** even when some checks fail validation
3. **Write results to BigQuery** for the 18 valid checks
4. **Throw appropriate errors** for the 2 invalid checks without stopping the entire pipeline

---

## Test Configuration Summary

| Total Checks | Valid Checks | Invalid Checks |
|--------------|--------------|----------------|
| 20           | 18           | 2              |

---

## Invalid Checks (Expected to Fail Validation)

### 1. CHK_NEG_002_INVALID_SEVERITY
**Location:** Check #2 in the configuration file

**Error Type:** Invalid Severity Value

**Issue:** 
- Uses severity value `"Critical"` which is not in the allowed list
- Valid severities are: `"High"`, `"Medium"`, `"Low"`

**Expected Error Message:**
```
ValidationError: Invalid severity 'Critical'. Must be one of: High, Medium, Low
```

**Check Details:**
```json
{
  "check_id": "CHK_NEG_002_INVALID_SEVERITY",
  "category": "Value Check",
  "sql_query": "SELECT DISTINCT status FROM `project.dataset.orders`",
  "description": "INVALID - Has invalid severity value 'Critical' (should be High, Medium, or Low)",
  "severity": "Critical",  // ❌ INVALID - Should be High, Medium, or Low
  "expected_output": ["Pending", "Completed"],
  "comparison_type": "set_match",
  "impacted_downstream": ["Order_Dashboard"],
  "tags": ["orders", "invalid_severity"],
  "active": true
}
```

---

### 2. CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH
**Location:** Check #6 in the configuration file

**Error Type:** Comparison Type and Expected Output Mismatch

**Issue:**
- Uses `comparison_type: "numeric_condition"` which expects a string value
- But provides `expected_output` as a list `["APAC", "EMEA", "Americas"]`
- For `numeric_condition`, expected_output must be a string representing a number or condition (e.g., "0", ">=10", "<100")

**Expected Error Message:**
```
ValidationError: For comparison_type 'numeric_condition', expected_output must be a string, got list
```

**Check Details:**
```json
{
  "check_id": "CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH",
  "category": "Value Check",
  "sql_query": "SELECT DISTINCT region FROM `project.dataset.sales`",
  "description": "INVALID - comparison_type is 'numeric_condition' but expected_output is a list (should be string)",
  "severity": "Medium",
  "expected_output": ["APAC", "EMEA", "Americas"],  // ❌ INVALID - Should be a string for numeric_condition
  "comparison_type": "numeric_condition",  // ❌ MISMATCH - Expects string, not list
  "impacted_downstream": ["Sales_Dashboard"],
  "tags": ["sales", "invalid_type_mismatch"],
  "active": true
}
```

---

## Valid Checks (Expected to Pass Validation and Process Successfully)

The remaining **18 checks** are all valid and should:
1. ✅ Pass JSON schema validation
2. ✅ Pass field validation (required fields, data types)
3. ✅ Pass comparison_type and expected_output compatibility validation
4. ✅ Execute SQL queries against BigQuery
5. ✅ Write results to the DQ results table

### Valid Check Categories:

| Check ID | Category | Comparison Type | Description |
|----------|----------|-----------------|-------------|
| CHK_NEG_001 | Null Check | numeric_condition | Valid null check with "0" |
| CHK_NEG_003 | Threshold Check | numeric_condition | Valid with ">=" operator |
| CHK_NEG_004 | Value Check | set_match | Valid set match with list |
| CHK_NEG_005 | Value Check | not_in_result | Valid not_in_result with list |
| CHK_NEG_007 | Row Match | row_match | Valid row match with list of objects |
| CHK_NEG_008 | Threshold Check | numeric_condition | Valid with ">" operator |
| CHK_NEG_009 | Threshold Check | numeric_condition | Valid with "<=" operator |
| CHK_NEG_010 | Threshold Check | numeric_condition | Valid with "!=" operator |
| CHK_NEG_011 | Threshold Check | numeric_condition | Valid exact match "15" |
| CHK_NEG_012 | Threshold Check | numeric_condition | Valid with "<" operator |
| CHK_NEG_013 | Value Check | set_match | Valid set match with countries |
| CHK_NEG_014 | Value Check | not_in_result | Valid not_in_result for orders |
| CHK_NEG_015 | Value Check | set_match | Valid set match for categories |
| CHK_NEG_016 | Value Check | set_match | Valid set match for regions |
| CHK_NEG_017 | Threshold Check | numeric_condition | Valid with ">" operator |
| CHK_NEG_018 | Threshold Check | numeric_condition | Valid with ">=" operator |
| CHK_NEG_019 | Row Match | row_match | Valid row match for countries |
| CHK_NEG_020 | Threshold Check | numeric_condition | Valid duplicate check "0" |

---

## Expected System Behavior

### Phase 1: Configuration Loading and Validation
```
Loading configuration: sample_dq_config_negative_tests.json
Total checks found: 20

Validating check 1/20: CHK_NEG_001_VALID_NULL_CHECK ✅ VALID
Validating check 2/20: CHK_NEG_002_INVALID_SEVERITY ❌ FAILED
  Error: Invalid severity 'Critical'. Must be one of: High, Medium, Low
  
Validating check 3/20: CHK_NEG_003_VALID_THRESHOLD ✅ VALID
Validating check 4/20: CHK_NEG_004_VALID_SET_MATCH ✅ VALID
Validating check 5/20: CHK_NEG_005_VALID_NOT_IN_RESULT ✅ VALID
Validating check 6/20: CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH ❌ FAILED
  Error: For comparison_type 'numeric_condition', expected_output must be a string, got list
  
Validating check 7/20: CHK_NEG_007_VALID_ROW_MATCH ✅ VALID
... (remaining checks all valid)

Validation Summary:
- Total checks: 20
- Valid checks: 18
- Invalid checks: 2
- Will proceed with 18 valid checks
```

### Phase 2: Execution of Valid Checks
```
Executing 18 valid checks...

Executing CHK_NEG_001_VALID_NULL_CHECK... ✅ Complete
Executing CHK_NEG_003_VALID_THRESHOLD... ✅ Complete
Executing CHK_NEG_004_VALID_SET_MATCH... ✅ Complete
... (all 18 valid checks execute)

Execution Summary:
- Checks executed: 18
- Results written to BigQuery: 18
- Checks skipped due to validation errors: 2
```

### Phase 3: Error Reporting
```
Validation Errors Encountered:

1. Check ID: CHK_NEG_002_INVALID_SEVERITY
   Error: ValidationError: Invalid severity 'Critical'. Must be one of: High, Medium, Low
   Action: Check skipped, not executed

2. Check ID: CHK_NEG_006_INVALID_COMPARISON_TYPE_MISMATCH
   Error: ValidationError: For comparison_type 'numeric_condition', expected_output must be a string, got list
   Action: Check skipped, not executed

Pipeline completed with 2 validation errors.
```

---

## Testing Instructions

### 1. Run the Negative Test Configuration
```bash
python main.py --config sample_dq_config_negative_tests.json
```

### 2. Expected Outcomes

#### ✅ Success Criteria:
- Pipeline does NOT crash or stop completely
- 18 valid checks are executed successfully
- 18 result rows are written to BigQuery DQ results table
- 2 validation errors are logged with clear error messages
- Error messages identify which checks failed and why

#### ❌ Failure Criteria:
- Pipeline crashes on first validation error
- Valid checks are not executed
- No results written to BigQuery
- Unclear or missing error messages

### 3. Verify Results in BigQuery

Query to check results were written:
```sql
SELECT 
  check_id,
  check_status,
  execution_timestamp
FROM `project.dataset.dq_results`
WHERE check_id LIKE 'CHK_NEG_%'
  AND DATE(execution_timestamp) = CURRENT_DATE()
ORDER BY check_id;
```

Expected: 18 rows (CHK_NEG_001, CHK_NEG_003, CHK_NEG_004, CHK_NEG_005, CHK_NEG_007 through CHK_NEG_020)

Missing: CHK_NEG_002 and CHK_NEG_006 (validation errors)

---

## Additional Negative Test Scenarios

### Other Validation Errors to Test (Future Enhancements)

1. **Missing Required Fields**
   - Remove `check_id` field
   - Expected: "Record at index X missing required fields: check_id"

2. **Empty String Values**
   - Set `sql_query: ""`
   - Expected: "'sql_query' must be a non-empty string"

3. **Invalid Active Flag**
   - Set `active: "true"` (string instead of boolean)
   - Expected: "'active' field must be a boolean, got str"

4. **Empty Expected Output for set_match**
   - Set `expected_output: []` with `comparison_type: "set_match"`
   - Expected: "For comparison_type 'set_match', expected_output list cannot be empty"

5. **Invalid Numeric Condition Format**
   - Set `expected_output: "abc"` with `comparison_type: "numeric_condition"`
   - Expected: "expected_output 'abc' must be a valid number or condition"

6. **Row Match with Non-Object Items**
   - Set `expected_output: ["string"]` with `comparison_type: "row_match"`
   - Expected: "Item at index 0 is str, not a dictionary"

---

## Code Implementation Requirements

To support this negative testing scenario, the code should:

1. **Validate Each Check Individually**
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

2. **Continue Processing Valid Checks**
   ```python
   if valid_checks:
       print(f"Processing {len(valid_checks)} valid checks...")
       results = execute_checks(valid_checks)
       write_to_bigquery(results)
   ```

3. **Report All Validation Errors**
   ```python
   if validation_errors:
       print(f"\n{len(validation_errors)} validation errors encountered:")
       for error in validation_errors:
           print(f"  - {error['check_id']}: {error['error']}")
   ```

4. **Return Appropriate Exit Code**
   ```python
   if validation_errors:
       sys.exit(1)  # Indicate errors occurred
   else:
       sys.exit(0)  # Success
   ```

---

## Summary

This negative test configuration provides a realistic scenario where:
- **Partial failures** occur in the configuration
- **Valid checks continue processing** despite errors
- **Clear error reporting** helps identify and fix issues
- **System resilience** is demonstrated by not crashing on validation errors

Use this configuration to ensure your DQ pipeline handles errors gracefully and continues processing valid checks even when some checks have configuration issues.
