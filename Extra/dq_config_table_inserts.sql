-- Sample INSERT statements for DQ Configuration Table
-- Replace project.dataset with your actual BigQuery project and dataset

-- Example 1: Null Check (numeric_condition)
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_string, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_001_NULL_EMPLOYEE_ID',
  'Null Check',
  'Verify that employee_id column has no NULL values',
  'High',
  'SELECT COUNT(*) as cnt FROM `project.dataset.employees` WHERE employee_id IS NULL',
  'numeric',
  '0',
  'numeric_condition',
  ['HR_Dashboard', 'Payroll_System'],
  ['employee', 'critical', 'daily'],
  TRUE,
  'admin@company.com'
);

-- Example 2: Set Match (allowed values)
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_array, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_002_ALLOWED_NAMES',
  'Value Check - Set Match',
  'Verify only allowed names (Rohan, Rahul, Priya) are present in employee table',
  'Medium',
  'SELECT DISTINCT name FROM `project.dataset.employees` WHERE name IS NOT NULL',
  'array',
  ['Rohan', 'Rahul', 'Priya'],
  'set_match',
  ['Employee_Report'],
  ['employee', 'validation'],
  TRUE,
  'admin@company.com'
);

-- Example 3: Not In Result (disallowed values)
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_array, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_003_NO_INVALID_STATUS',
  'Value Check - Not In Result',
  'Verify no INVALID or ERROR status values in orders',
  'High',
  'SELECT DISTINCT status FROM `project.dataset.orders`',
  'array',
  ['INVALID', 'ERROR'],
  'not_in_result',
  ['Order_Dashboard', 'Sales_Report'],
  ['orders', 'critical'],
  TRUE,
  'admin@company.com'
);

-- Example 4: Threshold Check (>=)
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_string, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_004_MIN_DEPARTMENTS',
  'Threshold Check - Greater Than or Equal',
  'Verify at least 10 departments exist',
  'Medium',
  'SELECT COUNT(DISTINCT department) as dept_count FROM `project.dataset.employees` WHERE department IS NOT NULL',
  'numeric',
  '>=10',
  'numeric_condition',
  ['Department_Report', 'HR_Dashboard'],
  ['employee', 'threshold'],
  TRUE,
  'admin@company.com'
);

-- Example 5: Row Match (array of objects)
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_json, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_005_REGION_SALES_COUNT',
  'Row Match - Exact Match',
  'Verify region-wise sales counts match expected',
  'Low',
  'SELECT region, COUNT(*) as cnt FROM `project.dataset.sales` GROUP BY region ORDER BY region',
  'object_array',
  '[{"region":"Americas","cnt":3},{"region":"APAC","cnt":4},{"region":"EMEA","cnt":3}]',
  'row_match',
  ['Sales_Dashboard'],
  ['sales', 'regional'],
  TRUE,
  'admin@company.com'
);

-- Example 6: Threshold Check (<=)
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_string, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_006_MAX_SALARY',
  'Threshold Check - Less Than or Equal',
  'Verify maximum salary does not exceed 100000',
  'High',
  'SELECT MAX(salary) as max_salary FROM `project.dataset.employees`',
  'numeric',
  '<=100000',
  'numeric_condition',
  ['Payroll_System', 'Budget_Report'],
  ['employee', 'salary', 'threshold'],
  TRUE,
  'admin@company.com'
);

-- Example 7: Threshold Check (>)
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_string, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_007_MIN_SALARY',
  'Threshold Check - Greater Than',
  'Verify minimum salary is above 50000',
  'Medium',
  'SELECT MIN(salary) as min_salary FROM `project.dataset.employees`',
  'numeric',
  '>50000',
  'numeric_condition',
  ['HR_Dashboard'],
  ['employee', 'salary', 'threshold'],
  TRUE,
  'admin@company.com'
);

-- Example 8: Exact Match
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_string, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_008_EXACT_EMPLOYEE_COUNT',
  'Threshold Check - Exact Match',
  'Verify exact employee count is 15',
  'Low',
  'SELECT COUNT(*) as emp_count FROM `project.dataset.employees`',
  'numeric',
  '15',
  'numeric_condition',
  ['HR_Dashboard'],
  ['employee', 'count'],
  TRUE,
  'admin@company.com'
);

-- Example 9: Not Equal (!=)
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_string, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_009_ACTIVE_CUSTOMERS_COUNT',
  'Threshold Check - Not Equal',
  'Verify active customer count is not zero',
  'High',
  'SELECT COUNT(*) as active_count FROM `project.dataset.customers` WHERE is_active = TRUE',
  'numeric',
  '!=0',
  'numeric_condition',
  ['Customer_Dashboard'],
  ['customer', 'active'],
  TRUE,
  'admin@company.com'
);

-- Example 10: Inactive Check
INSERT INTO `project.dataset.dq_config` (
  check_id, category, description, severity, sql_query,
  expected_output_type, expected_output_string, comparison_type,
  impacted_downstream, tags, active, created_by
) VALUES (
  'CHK_010_INACTIVE_CHECK',
  'Null Check',
  'INACTIVE - Verify customer_id has no NULL values',
  'Low',
  'SELECT COUNT(*) as cnt FROM `project.dataset.customers` WHERE customer_id IS NULL',
  'numeric',
  '0',
  'numeric_condition',
  ['Customer_Report'],
  ['customer', 'inactive'],
  FALSE,
  'admin@company.com'
);
