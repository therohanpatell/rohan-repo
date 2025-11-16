-- ============================================================================
-- Test Queries Returning Multiple Records
-- For testing execute_sql_with_results() functionality
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Simple SELECT returning 10 records
-- ----------------------------------------------------------------------------
-- Expected: 10 rows with id, name, value, status
SELECT 
  id,
  name,
  value,
  status,
  created_date
FROM `test_project.test_dataset.test_source_data`
WHERE partition_dt = '2024-01-15'
  AND id <= 10
ORDER BY id;

-- ----------------------------------------------------------------------------
-- Query 2: GROUP BY returning multiple records (by region and gender)
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows grouped by region and gender
SELECT 
  region,
  gender,
  COUNT(*) AS employee_count,
  AVG(salary) AS avg_salary,
  MIN(salary) AS min_salary,
  MAX(salary) AS max_salary
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
GROUP BY region, gender
ORDER BY region, gender;

-- ----------------------------------------------------------------------------
-- Query 3: UNNEST generating 100 records
-- ----------------------------------------------------------------------------
-- Expected: 100 rows with sequential numbers
SELECT 
  row_num AS id,
  CONCAT('Generated Row ', CAST(row_num AS STRING)) AS name,
  row_num * 10 AS value,
  CASE WHEN MOD(row_num, 2) = 0 THEN 'Even' ELSE 'Odd' END AS category
FROM UNNEST(GENERATE_ARRAY(1, 100)) AS row_num
ORDER BY row_num;

-- ----------------------------------------------------------------------------
-- Query 4: JOIN returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with employee and department information
SELECT 
  e.employee_id,
  e.employee_name,
  e.region,
  e.gender,
  e.salary,
  d.department_name,
  d.location
FROM `test_project.test_dataset.test_employee_data` e
INNER JOIN `test_project.test_dataset.test_department_data` d
  ON e.department_id = d.department_id
WHERE e.partition_dt = '2024-01-15'
  AND d.partition_dt = '2024-01-15'
ORDER BY e.employee_id;

-- ----------------------------------------------------------------------------
-- Query 5: CTE with multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with aggregated statistics
WITH employee_stats AS (
  SELECT 
    department_id,
    region,
    COUNT(*) AS emp_count,
    AVG(salary) AS avg_salary
  FROM `test_project.test_dataset.test_employee_data`
  WHERE partition_dt = '2024-01-15'
  GROUP BY department_id, region
)
SELECT 
  es.department_id,
  es.region,
  es.emp_count,
  es.avg_salary,
  d.department_name
FROM employee_stats es
INNER JOIN `test_project.test_dataset.test_department_data` d
  ON es.department_id = d.department_id
WHERE d.partition_dt = '2024-01-15'
ORDER BY es.department_id, es.region;

-- ----------------------------------------------------------------------------
-- Query 6: UNION returning combined records
-- ----------------------------------------------------------------------------
-- Expected: Combined rows from permanent and non-permanent employees
SELECT 
  employee_id,
  employee_name,
  employee_type,
  'Permanent' AS source_table
FROM `test_project.test_dataset.test_perm_employees`
WHERE partition_dt = '2024-01-15'
UNION ALL
SELECT 
  employee_id,
  employee_name,
  employee_type,
  'Non-Permanent' AS source_table
FROM `test_project.test_dataset.test_non_perm_employees`
WHERE partition_dt = '2024-01-15'
ORDER BY employee_id;

-- ----------------------------------------------------------------------------
-- Query 7: Window function returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with ranking information
SELECT 
  employee_id,
  employee_name,
  department_id,
  salary,
  RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS salary_rank,
  ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS row_num,
  AVG(salary) OVER (PARTITION BY department_id) AS dept_avg_salary
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
ORDER BY department_id, salary DESC;

-- ----------------------------------------------------------------------------
-- Query 8: CASE statement with multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with categorized data
SELECT 
  employee_id,
  employee_name,
  salary,
  CASE 
    WHEN salary < 75000 THEN 'Low'
    WHEN salary BETWEEN 75000 AND 85000 THEN 'Medium'
    WHEN salary > 85000 THEN 'High'
    ELSE 'Unknown'
  END AS salary_category,
  CASE 
    WHEN gender = 'Male' THEN 'M'
    WHEN gender = 'Female' THEN 'F'
    ELSE 'O'
  END AS gender_code
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
ORDER BY salary DESC;

-- ----------------------------------------------------------------------------
-- Query 9: Subquery returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with employees earning above average
SELECT 
  e.employee_id,
  e.employee_name,
  e.department_id,
  e.salary,
  (SELECT AVG(salary) FROM `test_project.test_dataset.test_employee_data` 
   WHERE partition_dt = '2024-01-15' AND department_id = e.department_id) AS dept_avg_salary
FROM `test_project.test_dataset.test_employee_data` e
WHERE e.partition_dt = '2024-01-15'
  AND e.salary > (SELECT AVG(salary) FROM `test_project.test_dataset.test_employee_data` 
                  WHERE partition_dt = '2024-01-15')
ORDER BY e.salary DESC;

-- ----------------------------------------------------------------------------
-- Query 10: CROSS JOIN generating many records
-- ----------------------------------------------------------------------------
-- Expected: Cartesian product (15 employees × 3 departments = 45 rows)
SELECT 
  e.employee_id,
  e.employee_name,
  d.department_id,
  d.department_name
FROM `test_project.test_dataset.test_employee_data` e
CROSS JOIN `test_project.test_dataset.test_department_data` d
WHERE e.partition_dt = '2024-01-15'
  AND d.partition_dt = '2024-01-15'
ORDER BY e.employee_id, d.department_id;

-- ----------------------------------------------------------------------------
-- Query 11: PIVOT-like query returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with aggregated counts by region and gender
SELECT 
  region,
  COUNTIF(gender = 'Male') AS male_count,
  COUNTIF(gender = 'Female') AS female_count,
  COUNT(*) AS total_count,
  ROUND(AVG(CASE WHEN gender = 'Male' THEN salary END), 2) AS male_avg_salary,
  ROUND(AVG(CASE WHEN gender = 'Female' THEN salary END), 2) AS female_avg_salary
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
GROUP BY region
ORDER BY region;

-- ----------------------------------------------------------------------------
-- Query 12: Date functions returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with date calculations
SELECT 
  employee_id,
  employee_name,
  hire_date,
  DATE_DIFF(DATE('2024-01-15'), hire_date, DAY) AS days_employed,
  DATE_DIFF(DATE('2024-01-15'), hire_date, MONTH) AS months_employed,
  DATE_DIFF(DATE('2024-01-15'), hire_date, YEAR) AS years_employed,
  EXTRACT(YEAR FROM hire_date) AS hire_year,
  EXTRACT(MONTH FROM hire_date) AS hire_month
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
ORDER BY hire_date;

-- ----------------------------------------------------------------------------
-- Query 13: String functions returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with string manipulations
SELECT 
  employee_id,
  employee_name,
  UPPER(employee_name) AS name_upper,
  LOWER(employee_name) AS name_lower,
  LENGTH(employee_name) AS name_length,
  SUBSTR(employee_name, 1, 3) AS name_prefix,
  CONCAT(employee_id, ' - ', employee_name) AS full_identifier
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
ORDER BY employee_id;

-- ----------------------------------------------------------------------------
-- Query 14: Numeric functions returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with numeric calculations
SELECT 
  employee_id,
  employee_name,
  salary,
  ROUND(salary, 0) AS salary_rounded,
  CEIL(salary / 1000) AS salary_thousands_ceil,
  FLOOR(salary / 1000) AS salary_thousands_floor,
  MOD(CAST(salary AS INT64), 1000) AS salary_mod_1000,
  salary * 1.1 AS salary_with_10_percent_raise
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
ORDER BY salary DESC;

-- ----------------------------------------------------------------------------
-- Query 15: NULL handling returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with NULL handling
SELECT 
  id,
  name,
  value,
  status,
  COALESCE(status, 'Unknown') AS status_with_default,
  IFNULL(value, 0.0) AS value_with_default,
  CASE WHEN status IS NULL THEN 'Missing' ELSE 'Present' END AS status_check
FROM `test_project.test_dataset.test_source_data`
WHERE partition_dt = '2024-01-15'
ORDER BY id;

-- ----------------------------------------------------------------------------
-- Query 16: Generate 1000 records for medium volume testing
-- ----------------------------------------------------------------------------
-- Expected: 1000 rows with generated data
SELECT 
  row_num AS metric_id,
  CONCAT('Metric_', CAST(row_num AS STRING)) AS metric_name,
  'Count' AS metric_type,
  CONCAT('Test metric number ', CAST(row_num AS STRING)) AS metric_description,
  'Daily' AS frequency,
  row_num * 10 AS numerator_value,
  row_num * 20 AS denominator_value,
  CAST(row_num * 10 AS FLOAT64) AS metric_output,
  '2024-01-15' AS business_data_date
FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS row_num
ORDER BY row_num;

-- ----------------------------------------------------------------------------
-- Query 17: Complex aggregation returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with complex aggregations
SELECT 
  department_id,
  region,
  gender,
  COUNT(*) AS employee_count,
  AVG(salary) AS avg_salary,
  STDDEV(salary) AS stddev_salary,
  MIN(salary) AS min_salary,
  MAX(salary) AS max_salary,
  SUM(salary) AS total_salary,
  APPROX_QUANTILES(salary, 4)[OFFSET(2)] AS median_salary
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
GROUP BY department_id, region, gender
HAVING COUNT(*) > 0
ORDER BY department_id, region, gender;

-- ----------------------------------------------------------------------------
-- Query 18: ARRAY_AGG returning multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows with aggregated arrays
SELECT 
  department_id,
  COUNT(*) AS employee_count,
  ARRAY_AGG(employee_name ORDER BY employee_name) AS employee_names,
  ARRAY_AGG(salary ORDER BY salary DESC LIMIT 3) AS top_3_salaries
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
GROUP BY department_id
ORDER BY department_id;

-- ----------------------------------------------------------------------------
-- Query 19: DISTINCT returning multiple unique records
-- ----------------------------------------------------------------------------
-- Expected: Multiple unique combinations
SELECT DISTINCT
  region,
  gender,
  department_id
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
ORDER BY region, gender, department_id;

-- ----------------------------------------------------------------------------
-- Query 20: HAVING clause filtering multiple records
-- ----------------------------------------------------------------------------
-- Expected: Multiple rows filtered by HAVING clause
SELECT 
  department_id,
  COUNT(*) AS employee_count,
  AVG(salary) AS avg_salary
FROM `test_project.test_dataset.test_employee_data`
WHERE partition_dt = '2024-01-15'
GROUP BY department_id
HAVING COUNT(*) >= 3 AND AVG(salary) > 75000
ORDER BY avg_salary DESC;

-- ============================================================================
-- End of Test Queries
-- ============================================================================
