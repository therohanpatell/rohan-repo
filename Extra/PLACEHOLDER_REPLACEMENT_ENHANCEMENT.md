# Placeholder Replacement Enhancement for DQ Pipeline

## 📋 Current Status

**❌ NOT IMPLEMENTED** - The DQ pipeline currently does NOT replace placeholders like `{currently}` and `{partition_info}` in SQL queries.

The metrics pipeline (`pipeline.py`) has this feature, but it's not yet implemented in the DQ pipeline (`dq_pipeline.py`).

---

## 🎯 What Needs to Be Added

### Placeholders to Support

1. **`{currently}`** - Replaced with the `run_date` parameter
   - Example: `WHERE date = {currently}` → `WHERE date = '2025-10-12'`

2. **`{partition_info}`** - Replaced with partition date from metadata table
   - Example: `WHERE partition_dt = {partition_info}` → `WHERE partition_dt = '2025-10-11'`

---

## 💡 Implementation Plan

### Step 1: Add Placeholder Replacement Method to DQPipeline

Add this method to the `DQPipeline` class in `dq_pipeline.py`:

```python
def replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str = None) -> str:
    """
    Replace {currently} and {partition_info} placeholders in SQL with appropriate dates
    
    Args:
        sql: SQL query with placeholders
        run_date: Business date for {currently} replacement
        partition_info_table: Table to query for {partition_info} replacement (optional)
        
    Returns:
        SQL with placeholders replaced
    """
    try:
        from utils import SQLUtils
        
        placeholders = SQLUtils.find_placeholder_positions(sql)
        
        if not placeholders:
            return sql  # No placeholders to replace
        
        # Sort placeholders by position (descending) to replace from end to start
        placeholders.sort(key=lambda x: x[1], reverse=True)
        
        final_sql = sql
        
        for placeholder_type, start_pos, end_pos in placeholders:
            if placeholder_type == 'currently':
                replacement_date = run_date
                logger.info(f"  Replacing {{currently}} placeholder with run_date: {replacement_date}")
                final_sql = final_sql[:start_pos] + f"'{replacement_date}'" + final_sql[end_pos:]
            
            elif placeholder_type == 'partition_info':
                if not partition_info_table:
                    raise ValidationError(
                        f"SQL contains {{partition_info}} placeholder but no partition_info_table provided"
                    )
                
                # Extract table reference before the placeholder
                table_match = re.search(r'FROM\s+`([^`]+)`', final_sql[:start_pos], re.IGNORECASE)
                
                if table_match:
                    full_table_name = table_match.group(1)
                    parts = full_table_name.split('.')
                    
                    if len(parts) >= 2:
                        project_dataset = f"{parts[0]}.{parts[1]}"
                        table_name = parts[2] if len(parts) == 3 else parts[1]
                        
                        # Get partition date from metadata table
                        replacement_date = self.bq_operations.get_partition_date(
                            project_dataset, 
                            table_name, 
                            partition_info_table
                        )
                        
                        if replacement_date:
                            logger.info(
                                f"  Replacing {{partition_info}} placeholder with partition_dt: {replacement_date} "
                                f"for table {project_dataset}.{table_name}"
                            )
                            final_sql = final_sql[:start_pos] + f"'{replacement_date}'" + final_sql[end_pos:]
                        else:
                            raise ValidationError(
                                f"Could not find partition_dt for {project_dataset}.{table_name} "
                                f"in {partition_info_table}"
                            )
                    else:
                        raise ValidationError(
                            f"Invalid table reference format: {full_table_name}. "
                            f"Expected format: project.dataset.table"
                        )
                else:
                    raise ValidationError(
                        f"Could not find table reference for {{partition_info}} placeholder"
                    )
        
        return final_sql
        
    except Exception as e:
        logger.error(f"Failed to replace SQL placeholders: {str(e)}")
        raise ValidationError(f"Failed to replace SQL placeholders: {str(e)}")
```

### Step 2: Update execute_single_check Method

Modify the `execute_single_check` method to replace placeholders before executing SQL:

```python
def execute_single_check(self, check_config: Dict, run_date: str, partition_info_table: str = None) -> Dict:
    """
    Execute a single DQ check
    
    Args:
        check_config: DQ check configuration
        run_date: Business date
        partition_info_table: Partition info table for {partition_info} replacement (optional)
        
    Returns:
        DQ result record
    """
    check_id = check_config['check_id']
    description = check_config.get('description', 'No description')
    sql_query = check_config['sql_query']
    expected_output = check_config['expected_output']
    comparison_type = check_config['comparison_type']
    severity = check_config.get('severity', 'N/A')
    
    # Start timing
    start_time = time.time()
    
    actual_result = None
    validation_status = "FAIL"
    failure_reason = ""
    error_message = None
    
    try:
        # ✨ NEW: Replace placeholders in SQL query
        logger.info(f"  Checking for SQL placeholders...")
        sql_query_with_replacements = self.replace_sql_placeholders(
            sql_query, 
            run_date, 
            partition_info_table
        )
        
        if sql_query_with_replacements != sql_query:
            logger.info(f"  SQL placeholders replaced")
            logger.debug(f"  Original SQL: {sql_query}")
            logger.debug(f"  Final SQL: {sql_query_with_replacements}")
        else:
            logger.debug(f"  No placeholders found in SQL")
        
        # Execute SQL query (with replacements)
        logger.info(f"  Executing SQL query...")
        logger.debug(f"  SQL: {sql_query_with_replacements}")
        
        actual_result = self._execute_dq_sql(sql_query_with_replacements, check_id)
        
        # ... rest of the method remains the same
```

### Step 3: Update execute_dq_checks Method

Update the method signature to accept `partition_info_table`:

```python
def execute_dq_checks(
    self, 
    dq_config: List[Dict], 
    run_date: str, 
    partition_info_table: str = None
) -> List[Dict]:
    """
    Execute DQ checks and perform validation
    
    Args:
        dq_config: DQ check configurations
        run_date: Business date for the run
        partition_info_table: Partition info table for {partition_info} replacement (optional)
        
    Returns:
        List of DQ result records
    """
    # ... existing code ...
    
    for i, check_config in enumerate(active_checks, 1):
        # ... existing code ...
        
        try:
            # Execute single check with partition_info_table
            result = self.execute_single_check(
                check_config, 
                run_date, 
                partition_info_table  # ✨ NEW parameter
            )
            dq_results.append(result)
            
            # ... rest of the code
```

### Step 4: Update Main Entry Point

Update `main.py` to pass `partition_info_table` parameter:

```python
# In main.py, DQ mode section
if args.mode == 'dq':
    # ... existing code ...
    
    # Execute DQ checks with partition_info_table
    dq_results = dq_pipeline.execute_dq_checks(
        dq_config=validated_config,
        run_date=args.run_date,
        partition_info_table=args.partition_info_table  # ✨ NEW parameter
    )
```

---

## 📝 Example Usage

### Example 1: Using {currently} Placeholder

**Config:**
```json
{
  "check_id": "CHK_001_EMPLOYEE_COUNT",
  "category": "Threshold Check",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.employees` WHERE hire_date = {currently}",
  "description": "Count employees hired on run date",
  "severity": "Medium",
  "expected_output": ">0",
  "comparison_type": "numeric_condition",
  "active": true
}
```

**Execution:**
```bash
python main.py --mode dq \
               --config dq_config.json \
               --run-date 2025-10-12 \
               --dq-target-table project.dataset.dq_results
```

**Result:**
- SQL before: `SELECT COUNT(*) as cnt FROM ... WHERE hire_date = {currently}`
- SQL after: `SELECT COUNT(*) as cnt FROM ... WHERE hire_date = '2025-10-12'`

### Example 2: Using {partition_info} Placeholder

**Config:**
```json
{
  "check_id": "CHK_002_SALES_PARTITION",
  "category": "Null Check",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.sales` WHERE partition_dt = {partition_info} AND amount IS NULL",
  "description": "Check for null amounts in latest partition",
  "severity": "High",
  "expected_output": "0",
  "comparison_type": "numeric_condition",
  "active": true
}
```

**Execution:**
```bash
python main.py --mode dq \
               --config dq_config.json \
               --run-date 2025-10-12 \
               --partition-info-table project.dataset.partition_info \
               --dq-target-table project.dataset.dq_results
```

**Result:**
- System queries `partition_info` table for latest partition of `project.dataset.sales`
- Finds partition_dt = '2025-10-11'
- SQL before: `SELECT COUNT(*) as cnt FROM ... WHERE partition_dt = {partition_info} AND ...`
- SQL after: `SELECT COUNT(*) as cnt FROM ... WHERE partition_dt = '2025-10-11' AND ...`

### Example 3: Using Both Placeholders

**Config:**
```json
{
  "check_id": "CHK_003_ORDERS_COMPARISON",
  "category": "Threshold Check",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.orders` WHERE order_date = {currently} AND partition_dt = {partition_info}",
  "description": "Count orders for run date in latest partition",
  "severity": "Medium",
  "expected_output": ">=100",
  "comparison_type": "numeric_condition",
  "active": true
}
```

**Result:**
- `{currently}` → '2025-10-12'
- `{partition_info}` → '2025-10-11' (from metadata table)
- SQL after: `... WHERE order_date = '2025-10-12' AND partition_dt = '2025-10-11'`

---

## 🔧 Command Line Arguments

Add new optional argument to `main.py`:

```python
# In DQ mode argument group
dq_group.add_argument(
    '--partition-info-table',
    help='Partition info table for {partition_info} placeholder replacement (optional)',
    required=False
)
```

---

## ✅ Benefits

1. **Dynamic Date Handling** - No need to hardcode dates in SQL queries
2. **Reusable Configs** - Same config works for different run dates
3. **Partition Awareness** - Automatically uses latest partition from metadata
4. **Consistency** - Same placeholder syntax as metrics pipeline
5. **Flexibility** - Placeholders are optional, static dates still work

---

## 📊 Updated Test Configs

### With {currently} Placeholder

```json
{
  "check_id": "CHK_DYNAMIC_001",
  "category": "Null Check",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.employees` WHERE hire_date = {currently} AND employee_id IS NULL",
  "description": "Check for null employee IDs on run date",
  "severity": "High",
  "expected_output": "0",
  "comparison_type": "numeric_condition",
  "active": true
}
```

### With {partition_info} Placeholder

```json
{
  "check_id": "CHK_DYNAMIC_002",
  "category": "Threshold Check",
  "sql_query": "SELECT COUNT(*) as cnt FROM `project.dataset.sales` WHERE partition_dt = {partition_info}",
  "description": "Count records in latest partition",
  "severity": "Medium",
  "expected_output": ">0",
  "comparison_type": "numeric_condition",
  "active": true
}
```

---

## 🚨 Error Handling

### Error 1: {partition_info} without partition_info_table

**Error:**
```
ValidationError: SQL contains {partition_info} placeholder but no partition_info_table provided
```

**Solution:** Provide `--partition-info-table` argument

### Error 2: Partition not found in metadata

**Error:**
```
ValidationError: Could not find partition_dt for project.dataset.sales in project.dataset.partition_info
```

**Solution:** Ensure partition metadata exists for the table

### Error 3: Invalid table reference

**Error:**
```
ValidationError: Could not find table reference for {partition_info} placeholder
```

**Solution:** Ensure SQL has proper `FROM` clause before placeholder

---

## 📝 Summary

**Current Status:** ❌ NOT IMPLEMENTED in DQ pipeline  
**Existing in:** ✅ Metrics pipeline (`pipeline.py`)  
**Effort:** Medium (reuse existing logic from metrics pipeline)  
**Impact:** High (enables dynamic date handling in DQ checks)

**To implement:**
1. Copy `replace_sql_placeholders` method from `pipeline.py` to `dq_pipeline.py`
2. Update `execute_single_check` to call placeholder replacement
3. Update `execute_dq_checks` to accept `partition_info_table` parameter
4. Update `main.py` to pass `partition_info_table` argument
5. Add command-line argument for `--partition-info-table`

**Recommendation:** Implement this enhancement to enable dynamic date handling in DQ checks, making configs more reusable and maintainable.

---

**Document Version:** 1.0  
**Created:** 2025-10-12
