# DQ Validation Framework - All Validation Scenarios

This document provides a comprehensive overview of all validation scenarios supported by the framework with examples from the sample data.

## 1. Numeric Condition Validations

### 1.1 Exact Match (=)
```json
{
  "expected_output": "15",
  "comparison_type": "numeric_condition"
}
```
**Example**: CHK_008 - Verify exact employee count is 15

### 1.2 Not Equal (!=)
```json
{
  "expected_output": "!=0",
  "comparison_type": "numeric_condition"
}
```
**Example**: CHK_009 - Verify active customer count is not zero

### 1.3 Greater Than (>)
```json
{
  "expected_output": ">50000",
  "comparison_type": "numeric_condition"
}
```
**Example**: CHK_007 - Verify minimum salary is above 50000

### 1.4 Greater Than or Equal (>=)
```json
{
  "expected_output": ">=10",
  "comparison_type": "numeric_condition"
}
```
**Example**: CHK_004 - Verify at least 10 departments exist

### 1.5 Less Than (<)
```json
{
  "expected_output": "<300000",
  "comparison_type": "numeric_condition"
}
```
**Example**: CHK_016 - Verify total revenue is less than 300000

### 1.6 Less Than or Equal (<=)
```json
{
  "expected_output": "<=100000",
  "comparison_type": "numeric_condition"
}
```
**Example**: CHK_006 - Verify maximum salary does not exceed 100000

## 2. Set Match Validations

Validates that query results contain ONLY the specified values (exact set match).

```json
{
  "expected_output": ["Rohan", "Rahul", "Priya"],
  "comparison_type": "set_match"
}
```

**Examples**:
- **CHK_002**: Employee names validation
- **CHK_010**: Customer countries validation
- **CHK_012**: Product categories validation
- **CHK_013**: Order regions validation

**SQL Pattern**:
```sql
SELECT DISTINCT column_name FROM table_name ORDER BY column_name
```

## 3. Not In Result Validations

Validates that specified values do NOT appear in query results.

```json
{
  "expected_output": ["INVALID", "ERROR"],
  "comparison_type": "not_in_result"
}
```

**Examples**:
- **CHK_003**: No INVALID or ERROR status in orders
- **CHK_011**: No Failed or Rejected orders

**SQL Pattern**:
```sql
SELECT DISTINCT status FROM table_name
```

## 4. Row Match Validations

Validates that query results match expected rows exactly (array of objects).

```json
{
  "expected_output": [
    {"region": "Americas", "cnt": 3},
    {"region": "APAC", "cnt": 4},
    {"region": "EMEA", "cnt": 3}
  ],
  "comparison_type": "row_match"
}
```

**Examples**:
- **CHK_005**: Region-wise sales counts
- **CHK_018**: Customer counts by country

**SQL Pattern**:
```sql
SELECT column1, COUNT(*) as cnt 
FROM table_name 
GROUP BY column1 
ORDER BY column1
```

## 5. Null Check Validations

Special case of numeric_condition to check for NULL values.

```json
{
  "expected_output": "0",
  "comparison_type": "numeric_condition",
  "sql_query": "SELECT COUNT(*) as cnt FROM table WHERE column IS NULL"
}
```

**Examples**:
- **CHK_001**: Employee ID null check (Expected to FAIL)
- **CHK_020**: Customer ID null check (INACTIVE)

## Complete Validation Matrix

| Check ID | Scenario | Comparison Type | Operator | Expected Result | Status |
|----------|----------|----------------|----------|-----------------|--------|
| CHK_001 | Null Check | numeric_condition | = | 0 | FAIL (1 null exists) |
| CHK_002 | Allowed Values | set_match | exact set | [Rohan, Rahul, Priya] | PASS |
| CHK_003 | Disallowed Values | not_in_result | not in | [INVALID, ERROR] | PASS |
| CHK_004 | Min Threshold | numeric_condition | >= | 10 | PASS (12 depts) |
| CHK_005 | Row Match | row_match | exact rows | 3 region counts | PASS |
| CHK_006 | Max Threshold | numeric_condition | <= | 100000 | PASS (88000) |
| CHK_007 | Min Range | numeric_condition | > | 50000 | PASS (60000) |
| CHK_008 | Exact Count | numeric_condition | = | 15 | PASS |
| CHK_009 | Not Zero | numeric_condition | != | 0 | PASS (9 active) |
| CHK_010 | Country Set | set_match | exact set | 7 countries | PASS |
| CHK_011 | No Failed Orders | not_in_result | not in | [Failed, Rejected] | PASS |
| CHK_012 | Category Set | set_match | exact set | [Electronics, Furniture] | PASS |
| CHK_013 | Region Set | set_match | exact set | [APAC, Americas, EMEA] | PASS |
| CHK_014 | Min Orders | numeric_condition | >= | 10 | PASS (12 orders) |
| CHK_015 | Avg Amount | numeric_condition | > | 1000 | PASS (~1900) |
| CHK_016 | Max Revenue | numeric_condition | < | 300000 | PASS (~258000) |
| CHK_017 | Min Stock | numeric_condition | >= | 30 | PASS |
| CHK_018 | Country Counts | row_match | exact rows | 2 country counts | PASS |
| CHK_019 | No Duplicates | numeric_condition | = | 0 | PASS |
| CHK_020 | Inactive Check | numeric_condition | = | 0 | INACTIVE |

## Advanced Scenarios

### Aggregation Validations
```sql
-- Average
SELECT AVG(amount) as avg_amount FROM orders

-- Sum
SELECT SUM(revenue) as total_revenue FROM sales

-- Min/Max
SELECT MIN(salary) as min_salary FROM employees
SELECT MAX(salary) as max_salary FROM employees

-- Count Distinct
SELECT COUNT(DISTINCT department) as dept_count FROM employees
```

### Duplicate Detection
```sql
SELECT COUNT(*) - COUNT(DISTINCT product_id) as duplicate_count 
FROM products
```
Expected: 0 (no duplicates)

### Conditional Aggregations
```sql
SELECT COUNT(*) as active_count 
FROM customers 
WHERE is_active = TRUE
```

### Multi-Column Row Match
```sql
SELECT region, product, SUM(quantity) as total_qty
FROM sales
GROUP BY region, product
ORDER BY region, product
```

## Best Practices

### 1. Use ORDER BY for Consistency
Always use ORDER BY in queries for set_match and row_match to ensure consistent ordering:
```sql
SELECT DISTINCT country FROM customers ORDER BY country
```

### 2. Handle NULLs Explicitly
```sql
-- Exclude NULLs
SELECT DISTINCT name FROM employees WHERE name IS NOT NULL

-- Include NULL check separately
SELECT COUNT(*) FROM employees WHERE employee_id IS NULL
```

### 3. Use Appropriate Severity Levels
- **Critical**: Data integrity issues (duplicates, NULLs in primary keys)
- **High**: Business rule violations (invalid statuses, missing required data)
- **Medium**: Data quality issues (unexpected values, threshold breaches)
- **Low**: Informational checks (counts, distributions)

### 4. Tag Your Checks
Use tags for filtering and organization:
```json
"tags": ["employee", "critical", "daily"]
```

### 5. Document Expected Behavior
Include clear descriptions indicating expected pass/fail:
```json
"description": "Verify employee_id has no NULL values (Expected to FAIL - 1 null exists)"
```

## Testing Your Validations

### Step 1: Test Individual Queries
Run each SQL query in BigQuery Console to verify results:
```sql
SELECT COUNT(*) as cnt FROM `project.dataset.employees` WHERE employee_id IS NULL
```

### Step 2: Verify Expected Output Format
Ensure query results match the expected_output format:
- Single value: `"expected_output": "15"`
- Array: `"expected_output": ["A", "B", "C"]`
- Objects: `"expected_output": [{"col1": "val1", "col2": 123}]`

### Step 3: Run Framework
```bash
python main.py --config sample_dq_config_comprehensive.json
```

### Step 4: Review Results
Check the output for:
- Pass/Fail status
- Actual vs Expected values
- Error messages for failures

## Extending the Framework

### Add New Comparison Types
Edit `comparison.py` to add new comparison logic:
```python
def custom_comparison(actual, expected):
    # Your custom logic
    return result, message
```

### Add New Validation Scenarios
1. Create SQL query that returns desired format
2. Choose appropriate comparison_type
3. Define expected_output
4. Add to configuration JSON
5. Test thoroughly

### Integration with Monitoring
- Export results to monitoring systems
- Set up alerts for critical failures
- Track trends over time
- Create dashboards for visualization
