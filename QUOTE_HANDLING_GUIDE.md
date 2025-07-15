# Quote Handling in SQL Queries - Implementation Guide

## Overview

This guide explains how the MetricsPipeline handles double quotes in SQL queries stored in JSON configuration files. The solution automatically detects and normalizes quote usage to prevent JSON parsing issues and SQL execution errors.

## Problem Statement

When storing SQL queries in JSON format, double quotes can create several issues:

1. **JSON Parsing Conflicts**: JSON uses double quotes for string delimiters, so double quotes in SQL can break JSON structure
2. **SQL Execution Issues**: Different SQL dialects have varying quote handling requirements
3. **BigQuery Compatibility**: BigQuery expects single quotes for string literals and backticks for identifiers

## Solution Architecture

The pipeline implements a comprehensive quote handling system with these components:

### 1. Quote Detection (`_detect_quote_issues`)
- Identifies different quote types in SQL queries
- Counts quote occurrences and detects potential issues
- Finds string literals and identifiers
- Provides detailed analysis for debugging

### 2. Quote Normalization (`_normalize_quotes_in_sql`)
- Converts double quotes to single quotes for string literals
- Preserves backticks for table/column identifiers
- Handles nested quotes and escape sequences
- Maintains SQL semantic integrity

### 3. SQL Preprocessing (`_preprocess_sql_query`)
- Orchestrates the complete quote handling process
- Validates SQL syntax after normalization
- Cleans up whitespace and formatting
- Integrates with existing pipeline flow

### 4. Validation and Logging
- Validates balanced quotes
- Logs all quote conversions for audit trail
- Provides detailed error messages
- Tracks conversion statistics

## How It Works

### Step 1: Detection Phase
```python
# Example input SQL with double quotes
sql = 'SELECT * FROM `project.dataset.table` WHERE name = "Rohan" AND status = "ACTIVE"'

# Detection results
quote_issues = {
    'has_double_quotes': True,
    'double_quote_count': 4,
    'potential_string_literals': ['Rohan', 'ACTIVE']
}
```

### Step 2: Normalization Phase
```python
# Original SQL
"SELECT * FROM `project.dataset.table` WHERE name = \"Rohan\" AND status = \"ACTIVE\""

# Normalized SQL
"SELECT * FROM `project.dataset.table` WHERE name = 'Rohan' AND status = 'ACTIVE'"
```

### Step 3: Complex Cases Handling
```python
# Handle nested quotes
Original: "WHERE name = \"John's Department\""
Normalized: "WHERE name = 'John''s Department'"

# Preserve table identifiers
Original: "SELECT * FROM `project.dataset.table` WHERE name = \"value\""
Normalized: "SELECT * FROM `project.dataset.table` WHERE name = 'value'"
```

## Integration Points

### 1. JSON Validation
The `validate_json` method now includes quote detection:
```python
def validate_json(self, json_data: List[Dict]) -> List[Dict]:
    # ... existing validation ...
    
    # Check for quote issues in SQL
    quote_issues = self._detect_quote_issues(sql_query, metric_id)
    if quote_issues['has_double_quotes']:
        logger.info(f"SQL query contains double quotes - will be normalized")
```

### 2. SQL Execution
The `execute_sql` method includes preprocessing:
```python
def execute_sql(self, sql: str, run_date: str, partition_info_table: str, metric_id: str) -> Dict:
    # Step 1: Preprocess SQL to handle quotes
    preprocessed_sql = self._preprocess_sql_query(sql, metric_id)
    
    # Step 2: Replace placeholders
    final_sql = self.replace_sql_placeholders(preprocessed_sql, run_date, partition_info_table)
    
    # Step 3: Execute query
    results = self._execute_bq_query(final_sql, "SQL execution")
```

### 3. Reconciliation Records
Quote handling is also applied to recon record creation:
```python
# Preprocess SQL with quote handling before placeholder replacement
preprocessed_sql = self._preprocess_sql_query(record['sql'], metric_id)
final_sql = self.replace_sql_placeholders(preprocessed_sql, run_date, partition_info_table)
```

## Example Usage

### JSON Configuration
```json
{
    "metric_id": "user_count_001",
    "metric_name": "Active User Count",
    "metric_type": "count",
    "sql": "SELECT COUNT(*) as metric_output FROM `project.dataset.users` WHERE status = \"ACTIVE\" AND name = \"Rohan\"",
    "dependency": "daily_users",
    "target_table": "project.dataset.metrics"
}
```

### Processing Log Output
```
INFO - Metric 'user_count_001': Found 4 double quotes in SQL
INFO - Metric 'user_count_001': Potential string literals: ['ACTIVE', 'Rohan']
INFO - Metric 'user_count_001': Normalizing quotes in SQL query
INFO - Metric 'user_count_001': Quote normalization completed
```

### Final Normalized SQL
```sql
SELECT COUNT(*) as metric_output 
FROM `project.dataset.users` 
WHERE status = 'ACTIVE' AND name = 'Rohan'
```

## Quote Conversion Summary

The pipeline provides comprehensive reporting:

```python
# At the end of pipeline execution
pipeline.log_quote_conversion_summary()

# Output
INFO - Quote Conversion Summary: 3 SQL queries had quotes normalized
INFO - Conversion 1: Metric 'user_count_001'
INFO -   - Double quotes found: 4
INFO -   - String literals converted: 2
INFO -   - Converted literals: ['ACTIVE', 'Rohan']
```

## Best Practices

### 1. JSON Configuration
- Use double quotes for JSON string values
- Escape double quotes in SQL as `\"` when necessary
- Consider using single quotes in SQL to avoid conflicts

### 2. SQL Writing
- Prefer single quotes for string literals
- Use backticks for table/column identifiers
- Avoid mixing quote types unnecessarily

### 3. Testing
- Always test SQL queries with the actual data
- Review quote conversion logs for accuracy
- Validate that conversions preserve SQL semantics

## Error Handling

The system includes robust error handling:

### Quote Imbalance Detection
```python
# Detects unbalanced quotes
if single_quote_count % 2 != 0:
    logger.warning(f"Unbalanced single quotes in SQL")
```

### Conversion Failure Handling
```python
# Graceful degradation if conversion fails
try:
    normalized_sql = self._normalize_quotes_in_sql(sql, metric_id)
except Exception as e:
    logger.error(f"Quote normalization failed: {e}")
    # Falls back to original SQL with warning
```

### Validation Warnings
```python
# Warns about potential issues
if not self._validate_sql_syntax(normalized_sql, metric_id):
    logger.warning(f"SQL syntax validation failed, proceeding with caution")
```

## Performance Considerations

1. **Regex Optimization**: Quote detection uses compiled regex patterns
2. **Lazy Processing**: Only processes queries with detected issues
3. **Memory Efficiency**: Processes queries one at a time
4. **Logging Control**: Debug logging can be disabled in production

## Monitoring and Debugging

### Quote Conversion Tracking
- All conversions are logged with before/after SQL
- Conversion statistics are tracked per pipeline run
- Detailed error messages for troubleshooting

### Debug Information
```python
# Get detailed conversion report
report = pipeline.get_quote_conversion_report()
print(f"Total conversions: {report['total_conversions']}")
print(f"Total double quotes converted: {report['summary']['total_double_quotes_converted']}")
```

## Future Enhancements

1. **Advanced SQL Parsing**: More sophisticated SQL parsing for complex cases
2. **Custom Quote Rules**: Configurable quote handling rules
3. **Quote Style Validation**: Enforce consistent quote styles
4. **Performance Optimization**: Caching and optimization for large datasets

## Conclusion

The quote handling system provides a robust solution for managing SQL queries with double quotes in JSON configuration files. It automatically detects and normalizes quote usage while maintaining SQL semantic integrity and providing comprehensive logging for debugging and monitoring. 