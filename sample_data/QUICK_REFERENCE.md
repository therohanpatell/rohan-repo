# DQ Validation Framework - Quick Reference Card

## Setup Commands

### Linux/Mac
```bash
cd sample_data
./setup_bq_tables.sh YOUR_PROJECT YOUR_DATASET
cd ..
python main.py --config sample_dq_config_comprehensive.json
```

### Windows
```powershell
cd sample_data
.\setup_bq_tables.ps1 -ProjectId "YOUR_PROJECT" -DatasetName "YOUR_DATASET"
cd ..
python main.py --config sample_dq_config_comprehensive.json
```

## Validation Types Quick Reference

### 1. Numeric Condition
```json
{
  "expected_output": ">=10",
  "comparison_type": "numeric_condition"
}
```
**Operators**: `=`, `!=`, `>`, `>=`, `<`, `<=`

### 2. Set Match
```json
{
  "expected_output": ["Value1", "Value2"],
  "comparison_type": "set_match"
}
```
**Use**: Validate exact set of allowed values

### 3. Not In Result
```json
{
  "expected_output": ["BadValue1", "BadValue2"],
  "comparison_type": "not_in_result"
}
```
**Use**: Ensure disallowed values don't exist

### 4. Row Match
```json
{
  "expected_output": [
    {"col1": "val1", "col2": 123},
    {"col1": "val2", "col2": 456}
  ],
  "comparison_type": "row_match"
}
```
**Use**: Validate exact row-level results

## Common SQL Patterns

### Null Check
```sql
SELECT COUNT(*) as cnt 
FROM table 
WHERE column IS NULL
```
Expected: `"0"`

### Distinct Values
```sql
SELECT DISTINCT column 
FROM table 
ORDER BY column
```
Expected: `["Val1", "Val2"]`

### Aggregation
```sql
SELECT region, COUNT(*) as cnt 
FROM table 
GROUP BY region 
ORDER BY region
```
Expected: `[{"region": "A", "cnt": 10}]`

### Threshold
```sql
SELECT COUNT(*) as total 
FROM table
```
Expected: `">=100"` or `"<1000"`

### Duplicate Check
```sql
SELECT COUNT(*) - COUNT(DISTINCT id) as dup_count 
FROM table
```
Expected: `"0"`

## Sample Data Summary

| Table | Rows | Purpose |
|-------|------|---------|
| employees | 15 | Null checks, value checks, thresholds |
| orders | 12 | Disallowed values, numeric conditions |
| sales | 10 | Row match, aggregations |
| customers | 10 | Set match, patterns |
| products | 10 | Range checks, duplicates |

## Check Configuration Template

```json
{
  "check_id": "CHK_XXX_DESCRIPTION",
  "category": "Check Category",
  "sql_query": "SELECT ... FROM ...",
  "description": "What this check validates",
  "severity": "High|Medium|Low|Critical",
  "expected_output": "value or array or objects",
  "comparison_type": "numeric_condition|set_match|not_in_result|row_match",
  "impacted_downstream": ["System1", "System2"],
  "tags": ["tag1", "tag2"],
  "active": true
}
```

## Expected Test Results

- **Total Checks**: 20
- **Active**: 19
- **Inactive**: 1 (CHK_020)
- **Pass**: 18
- **Fail**: 1 (CHK_001 - intentional)

## Severity Levels

- **Critical**: Data integrity (duplicates, primary key nulls)
- **High**: Business rules (invalid statuses, required data)
- **Medium**: Data quality (unexpected values, thresholds)
- **Low**: Informational (counts, distributions)

## Best Practices

1. ✅ Always use `ORDER BY` for consistent results
2. ✅ Handle NULLs explicitly in queries
3. ✅ Use descriptive check_ids
4. ✅ Document expected pass/fail in description
5. ✅ Tag checks for easy filtering
6. ✅ Set appropriate severity levels
7. ✅ Test queries in BigQuery Console first

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Authentication error | `gcloud auth application-default login` |
| Table not found | Verify project.dataset names |
| Permission denied | Check BigQuery IAM roles |
| Query timeout | Optimize SQL or increase timeout |
| Wrong results | Check ORDER BY in query |

## File Locations

```
project/
├── main.py                                    # Run this
├── sample_dq_config_comprehensive.json        # 20 checks
├── SAMPLE_DATA_SUMMARY.md                     # Overview
└── sample_data/
    ├── README.md                              # Detailed setup
    ├── QUICK_REFERENCE.md                     # This file
    ├── VALIDATION_SCENARIOS.md                # All scenarios
    ├── bq_schemas.sql                         # Table DDL
    ├── insert_data.sql                        # Sample data
    ├── setup_bq_tables.sh                     # Linux setup
    └── setup_bq_tables.ps1                    # Windows setup
```

## Next Steps

1. Run setup script
2. Verify data in BigQuery Console
3. Run framework: `python main.py --config sample_dq_config_comprehensive.json`
4. Review results
5. Customize for your needs

## Support Files

- **Full Setup Guide**: `sample_data/README.md`
- **All Scenarios**: `sample_data/VALIDATION_SCENARIOS.md`
- **Overview**: `SAMPLE_DATA_SUMMARY.md`
