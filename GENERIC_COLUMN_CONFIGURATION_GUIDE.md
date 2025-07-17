# Generic Column Configuration Guide

## Overview

The metrics pipeline now supports a flexible, generic column configuration system that allows you to easily change column names and add new columns without modifying the core code. This makes the pipeline adaptable to different naming conventions and future requirements.

## 🔧 Key Features

### 1. **Configurable Column Names**
- Change SQL column names without code modifications
- Support different naming conventions across projects
- Easy migration from old to new column names

### 2. **Dynamic Schema Generation**
- Schema automatically adapts to column configuration
- Support for different data types (numeric, date, string)
- Automatic precision preservation for numeric columns

### 3. **Runtime Configuration**
- Update column mappings at runtime
- Add new columns without restarting the pipeline
- Backward compatibility with existing configurations

## 📋 Default Configuration

```python
DEFAULT_COLUMN_CONFIG = {
    'metric_output': 'metric_output',
    'numerator_value': 'numerator_value', 
    'denominator_value': 'denominator_value',
    'business_data_date': 'business_data_date'
}
```

## 🚀 Usage Examples

### Example 1: Using Default Configuration

```python
# Initialize with default column names
pipeline = MetricsPipeline(spark, bq_client)

# Your SQL queries must return these columns:
# - metric_output
# - numerator_value  
# - denominator_value
# - business_data_date
```

### Example 2: Custom Column Names

```python
# Custom column configuration
custom_config = {
    'metric_output': 'output',
    'numerator_value': 'numerator', 
    'denominator_value': 'denominator',
    'business_data_date': 'date'
}

pipeline = MetricsPipeline(spark, bq_client, column_config=custom_config)

# Your SQL queries must return these columns:
# - output
# - numerator
# - denominator  
# - date
```

### Example 3: Adding New Columns

```python
# Start with default config
pipeline = MetricsPipeline(spark, bq_client)

# Add new column mappings
pipeline.add_column_mapping('confidence_score', 'confidence')
pipeline.add_column_mapping('data_quality', 'quality_score')

# Your SQL queries must now return:
# - metric_output, numerator_value, denominator_value, business_data_date
# - confidence (mapped to confidence_score)
# - quality_score (mapped to data_quality)
```

### Example 4: Runtime Configuration Updates

```python
# Update configuration at runtime
new_config = {
    'metric_output': 'result',
    'numerator_value': 'num', 
    'denominator_value': 'denom',
    'business_data_date': 'biz_date'
}

pipeline.update_column_config(new_config)

# Your SQL queries must now return:
# - result, num, denom, biz_date
```

## 📊 Column Types and Validation

### Numeric Columns
```python
COLUMN_TYPES = {
    'numeric_columns': ['metric_output', 'numerator_value', 'denominator_value']
}
```
- Automatically get precision preservation
- Stored as `DecimalType(38, 9)` in Spark DataFrames
- Validated for zero/negative denominators

### Date Columns
```python
COLUMN_TYPES = {
    'date_columns': ['business_data_date']
}
```
- Automatically formatted as 'YYYY-MM-DD' strings
- Required for pipeline operation

### Required Columns
```python
COLUMN_TYPES = {
    'required_columns': ['business_data_date']
}
```
- Must be present in SQL query results
- Pipeline fails if missing

## 🔄 Migration Examples

### Migration 1: Rename Columns

```python
# Old configuration
old_config = {
    'metric_output': 'metric_output',
    'numerator_value': 'numerator_value',
    'denominator_value': 'denominator_value', 
    'business_data_date': 'business_data_date'
}

# New configuration
new_config = {
    'metric_output': 'output',
    'numerator_value': 'numerator',
    'denominator_value': 'denominator',
    'business_data_date': 'date'
}

# Update pipeline
pipeline.update_column_config(new_config)
```

### Migration 2: Add New Metric Types

```python
# Add new metric columns
pipeline.add_column_mapping('confidence_interval_lower', 'ci_lower')
pipeline.add_column_mapping('confidence_interval_upper', 'ci_upper')
pipeline.add_column_mapping('sample_size', 'n')

# Update column types
pipeline.COLUMN_TYPES['numeric_columns'].extend([
    'confidence_interval_lower', 
    'confidence_interval_upper',
    'sample_size'
])
```

## 📝 SQL Query Examples

### Example 1: Default Column Names

```sql
SELECT 
    SUM(revenue) as metric_output,
    COUNT(*) as numerator_value,
    COUNT(DISTINCT customer_id) as denominator_value,
    CURRENT_DATE() as business_data_date
FROM `project.dataset.sales`
WHERE date = '{currently}'
```

### Example 2: Custom Column Names

```sql
SELECT 
    SUM(revenue) as output,
    COUNT(*) as numerator,
    COUNT(DISTINCT customer_id) as denominator,
    CURRENT_DATE() as date
FROM `project.dataset.sales`
WHERE date = '{currently}'
```

### Example 3: With Additional Columns

```sql
SELECT 
    SUM(revenue) as output,
    COUNT(*) as numerator,
    COUNT(DISTINCT customer_id) as denominator,
    CURRENT_DATE() as date,
    0.95 as confidence,
    'HIGH' as quality_score,
    1000 as n
FROM `project.dataset.sales`
WHERE date = '{currently}'
```

## 🛠️ Configuration Methods

### 1. `update_column_config(new_config)`
```python
# Update entire configuration
new_config = {
    'metric_output': 'result',
    'numerator_value': 'num',
    'denominator_value': 'denom',
    'business_data_date': 'date'
}
pipeline.update_column_config(new_config)
```

### 2. `add_column_mapping(internal_name, sql_column_name)`
```python
# Add single column mapping
pipeline.add_column_mapping('confidence_score', 'confidence')
```

### 3. `get_sql_column_name(internal_name)`
```python
# Get SQL column name from internal name
sql_name = pipeline.get_sql_column_name('metric_output')
# Returns: 'metric_output' (or custom name if configured)
```

### 4. `get_internal_name(sql_column_name)`
```python
# Get internal name from SQL column name
internal_name = pipeline.get_internal_name('output')
# Returns: 'metric_output' (or custom name if configured)
```

## 🔍 Monitoring and Debugging

### View Current Configuration
```python
print(f"Current column config: {pipeline.column_config}")
print(f"Numeric columns: {pipeline.numeric_columns}")
print(f"Date columns: {pipeline.date_columns}")
print(f"Required columns: {pipeline.required_columns}")
```

### Validate SQL Results
```python
# Check if SQL returns required columns
required_sql_columns = list(pipeline.column_config.values())
print(f"SQL must return: {required_sql_columns}")
```

## ⚠️ Important Notes

### 1. **Backward Compatibility**
- Default configuration maintains existing behavior
- Existing SQL queries continue to work without changes
- Gradual migration supported

### 2. **Column Type Validation**
- Numeric columns automatically get precision preservation
- Date columns are formatted as 'YYYY-MM-DD' strings
- Required columns must be present in SQL results

### 3. **Schema Generation**
- Spark DataFrame schema is generated dynamically
- Column types are inferred from configuration
- New columns are automatically included

### 4. **Error Handling**
- Clear error messages for missing required columns
- Validation for numeric column values (zero/negative denominators)
- Graceful handling of missing optional columns

## 🚀 Advanced Usage

### Dynamic Configuration Based on Environment

```python
import os

# Environment-specific configurations
configs = {
    'dev': {
        'metric_output': 'dev_output',
        'numerator_value': 'dev_numerator',
        'denominator_value': 'dev_denominator',
        'business_data_date': 'dev_date'
    },
    'prod': {
        'metric_output': 'output',
        'numerator_value': 'numerator',
        'denominator_value': 'denominator',
        'business_data_date': 'date'
    }
}

env = os.getenv('ENVIRONMENT', 'dev')
pipeline = MetricsPipeline(spark, bq_client, column_config=configs[env])
```

### Configuration from JSON File

```python
import json

# Load configuration from file
with open('column_config.json', 'r') as f:
    config = json.load(f)

pipeline = MetricsPipeline(spark, bq_client, column_config=config)
```

### Validation and Testing

```python
def validate_sql_columns(sql_query, pipeline):
    """Validate that SQL query returns required columns"""
    required_columns = list(pipeline.column_config.values())
    
    # Simple validation - check if column names appear in SQL
    missing_columns = []
    for col in required_columns:
        if col not in sql_query:
            missing_columns.append(col)
    
    if missing_columns:
        raise ValueError(f"SQL query missing required columns: {missing_columns}")
    
    return True
```

## 📈 Benefits

1. **Flexibility**: Easy to adapt to different naming conventions
2. **Maintainability**: No code changes needed for column name updates
3. **Scalability**: Simple to add new columns and metric types
4. **Compatibility**: Backward compatible with existing configurations
5. **Validation**: Built-in validation for required columns and data types

## 🔮 Future Enhancements

### 1. **Column Type Inference**
```python
# Automatically detect column types from SQL
pipeline.infer_column_types(sql_query)
```

### 2. **Configuration Validation**
```python
# Validate configuration before use
pipeline.validate_column_config()
```

### 3. **Schema Evolution**
```python
# Handle schema changes gracefully
pipeline.migrate_schema(old_config, new_config)
```

This generic column configuration system makes the metrics pipeline much more flexible and maintainable, allowing you to easily adapt to changing requirements without modifying the core code. 