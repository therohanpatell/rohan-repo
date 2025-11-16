-- ============================================================================
-- BigQuery Test Data for Dynamic Schema Framework Testing
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Insert Test Source Data
-- ----------------------------------------------------------------------------
INSERT INTO `test_project.test_dataset.test_source_data` 
(id, name, value, status, created_date, partition_dt)
VALUES
  (1, 'Record 1', 100.5, 'Active', DATE('2024-01-15'), '2024-01-15'),
  (2, 'Record 2', 200.75, 'Active', DATE('2024-01-15'), '2024-01-15'),
  (3, 'Record 3', 150.25, 'Inactive', DATE('2024-01-15'), '2024-01-15'),
  (4, 'Record 4', 300.00, 'Active', DATE('2024-01-15'), '2024-01-15'),
  (5, 'Record 5', 250.50, 'Active', DATE('2024-01-15'), '2024-01-15'),
  (6, 'Record 6', 175.80, 'Inactive', DATE('2024-01-15'), '2024-01-15'),
  (7, 'Record 7', 400.25, 'Active', DATE('2024-01-15'), '2024-01-15'),
  (8, 'Record 8', 125.60, 'Active', DATE('2024-01-15'), '2024-01-15'),
  (9, 'Record 9', 350.90, 'Inactive', DATE('2024-01-15'), '2024-01-15'),
  (10, 'Record 10', 275.40, 'Active', DATE('2024-01-15'), '2024-01-15');

-- ----------------------------------------------------------------------------
-- 2. Insert Test Employee Data
-- ----------------------------------------------------------------------------
INSERT INTO `test_project.test_dataset.test_employee_data`
(employee_id, employee_name, department_id, region, gender, hire_date, salary, partition_dt)
VALUES
  ('EMP001', 'John Doe', 'DEPT01', 'North', 'Male', DATE('2020-01-15'), 75000.00, '2024-01-15'),
  ('EMP002', 'Jane Smith', 'DEPT01', 'North', 'Female', DATE('2020-03-20'), 80000.00, '2024-01-15'),
  ('EMP003', 'Bob Johnson', 'DEPT02', 'South', 'Male', DATE('2019-06-10'), 70000.00, '2024-01-15'),
  ('EMP004', 'Alice Williams', 'DEPT02', 'South', 'Female', DATE('2021-02-14'), 85000.00, '2024-01-15'),
  ('EMP005', 'Charlie Brown', 'DEPT01', 'East', 'Male', DATE('2018-11-05'), 90000.00, '2024-01-15'),
  ('EMP006', 'Diana Prince', 'DEPT03', 'West', 'Female', DATE('2022-04-18'), 95000.00, '2024-01-15'),
  ('EMP007', 'Eve Davis', 'DEPT02', 'North', 'Female', DATE('2020-08-22'), 72000.00, '2024-01-15'),
  ('EMP008', 'Frank Miller', 'DEPT03', 'South', 'Male', DATE('2019-12-01'), 88000.00, '2024-01-15'),
  ('EMP009', 'Grace Lee', 'DEPT01', 'East', 'Female', DATE('2021-07-30'), 78000.00, '2024-01-15'),
  ('EMP010', 'Henry Wilson', 'DEPT02', 'West', 'Male', DATE('2020-05-12'), 82000.00, '2024-01-15'),
  ('EMP011', 'Ivy Chen', 'DEPT03', 'North', 'Female', DATE('2022-01-08'), 76000.00, '2024-01-15'),
  ('EMP012', 'Jack Taylor', 'DEPT01', 'South', 'Male', DATE('2019-09-25'), 91000.00, '2024-01-15'),
  ('EMP013', 'Kate Anderson', 'DEPT02', 'East', 'Female', DATE('2021-11-14'), 84000.00, '2024-01-15'),
  ('EMP014', 'Leo Martinez', 'DEPT03', 'West', 'Male', DATE('2020-03-03'), 79000.00, '2024-01-15'),
  ('EMP015', 'Mia Garcia', 'DEPT01', 'North', 'Female', DATE('2022-06-20'), 87000.00, '2024-01-15');

-- ----------------------------------------------------------------------------
-- 3. Insert Test Department Data
-- ----------------------------------------------------------------------------
INSERT INTO `test_project.test_dataset.test_department_data`
(department_id, department_name, location, partition_dt)
VALUES
  ('DEPT01', 'Engineering', 'Building A', '2024-01-15'),
  ('DEPT02', 'Marketing', 'Building B', '2024-01-15'),
  ('DEPT03', 'Sales', 'Building C', '2024-01-15');

-- ----------------------------------------------------------------------------
-- 4. Insert Test Permanent Employees
-- ----------------------------------------------------------------------------
INSERT INTO `test_project.test_dataset.test_perm_employees`
(employee_id, employee_name, employee_type, partition_dt)
VALUES
  ('PERM001', 'Permanent Employee 1', 'Permanent', '2024-01-15'),
  ('PERM002', 'Permanent Employee 2', 'Permanent', '2024-01-15'),
  ('PERM003', 'Permanent Employee 3', 'Permanent', '2024-01-15'),
  ('PERM004', 'Permanent Employee 4', 'Permanent', '2024-01-15'),
  ('PERM005', 'Permanent Employee 5', 'Permanent', '2024-01-15'),
  ('PERM006', 'Permanent Employee 6', 'Permanent', '2024-01-15'),
  ('PERM007', 'Permanent Employee 7', 'Permanent', '2024-01-15'),
  ('PERM008', 'Permanent Employee 8', 'Permanent', '2024-01-15'),
  ('PERM009', 'Permanent Employee 9', 'Permanent', '2024-01-15'),
  ('PERM010', 'Permanent Employee 10', 'Permanent', '2024-01-15');

-- ----------------------------------------------------------------------------
-- 5. Insert Test Non-Permanent Employees
-- ----------------------------------------------------------------------------
INSERT INTO `test_project.test_dataset.test_non_perm_employees`
(employee_id, employee_name, employee_type, partition_dt)
VALUES
  ('TEMP001', 'Temporary Employee 1', 'Temporary', '2024-01-15'),
  ('TEMP002', 'Temporary Employee 2', 'Temporary', '2024-01-15'),
  ('TEMP003', 'Temporary Employee 3', 'Temporary', '2024-01-15'),
  ('TEMP004', 'Temporary Employee 4', 'Temporary', '2024-01-15'),
  ('TEMP005', 'Temporary Employee 5', 'Temporary', '2024-01-15');

-- ----------------------------------------------------------------------------
-- 6. Insert Test Partition Info
-- ----------------------------------------------------------------------------
INSERT INTO `test_project.test_dataset.test_partition_info`
(table_name, partition_dt, record_count, last_updated)
VALUES
  ('test_source_data', '2024-01-15', 10, CURRENT_TIMESTAMP()),
  ('test_employee_data', '2024-01-15', 15, CURRENT_TIMESTAMP()),
  ('test_department_data', '2024-01-15', 3, CURRENT_TIMESTAMP()),
  ('test_perm_employees', '2024-01-15', 10, CURRENT_TIMESTAMP()),
  ('test_non_perm_employees', '2024-01-15', 5, CURRENT_TIMESTAMP());

-- ----------------------------------------------------------------------------
-- 7. Insert Test All Data Types (for schema conversion testing)
-- ----------------------------------------------------------------------------
INSERT INTO `test_project.test_dataset.test_all_data_types`
(string_col, int_col, integer_col, float_col, numeric_col, decimal_col, 
 boolean_col, bool_col, date_col, timestamp_col, datetime_col,
 required_string, required_int, nullable_string, nullable_int)
VALUES
  ('String Value 1', 100, 200, 123.456, 789.123456789, 456.78, 
   TRUE, FALSE, DATE('2024-01-15'), TIMESTAMP('2024-01-15 10:30:00'), DATETIME('2024-01-15 10:30:00'),
   'Required String 1', 1, 'Nullable String 1', 10),
  ('String Value 2', 300, 400, 234.567, 890.234567890, 567.89, 
   FALSE, TRUE, DATE('2024-01-16'), TIMESTAMP('2024-01-16 11:45:00'), DATETIME('2024-01-16 11:45:00'),
   'Required String 2', 2, NULL, NULL),
  ('String Value 3', 500, 600, 345.678, 901.345678901, 678.90, 
   TRUE, TRUE, DATE('2024-01-17'), TIMESTAMP('2024-01-17 12:00:00'), DATETIME('2024-01-17 12:00:00'),
   'Required String 3', 3, 'Nullable String 3', 30);

-- ----------------------------------------------------------------------------
-- 8. Insert Test Simple Table Data
-- ----------------------------------------------------------------------------
INSERT INTO `test_project.test_dataset.test_simple_table`
(id, name, created_date)
VALUES
  (1, 'Simple Record 1', DATE('2024-01-15')),
  (2, 'Simple Record 2', DATE('2024-01-15')),
  (3, 'Simple Record 3', DATE('2024-01-15'));

-- ============================================================================
-- Additional Test Data Generation Queries
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Generate 1000 records for medium volume testing
-- ----------------------------------------------------------------------------
-- Note: Run this separately to generate test data for TEST_008
INSERT INTO `test_project.test_dataset.test_source_data` 
(id, name, value, status, created_date, partition_dt)
SELECT 
  row_num + 100 AS id,
  CONCAT('Generated Record ', CAST(row_num AS STRING)) AS name,
  CAST(row_num * 1.5 AS FLOAT64) AS value,
  CASE WHEN MOD(row_num, 2) = 0 THEN 'Active' ELSE 'Inactive' END AS status,
  DATE('2024-01-15') AS created_date,
  '2024-01-15' AS partition_dt
FROM UNNEST(GENERATE_ARRAY(1, 1000)) AS row_num;

-- ----------------------------------------------------------------------------
-- Generate 100,000 records for large volume testing
-- ----------------------------------------------------------------------------
-- Note: Run this separately for performance testing (TEST_004, 9.1)
-- WARNING: This will create a large dataset
INSERT INTO `test_project.test_dataset.test_source_data` 
(id, name, value, status, created_date, partition_dt)
SELECT 
  row_num + 10000 AS id,
  CONCAT('Large Volume Record ', CAST(row_num AS STRING)) AS name,
  CAST(row_num * 0.75 AS FLOAT64) AS value,
  CASE WHEN MOD(row_num, 3) = 0 THEN 'Active' ELSE 'Inactive' END AS status,
  DATE('2024-01-15') AS created_date,
  '2024-01-15' AS partition_dt
FROM UNNEST(GENERATE_ARRAY(1, 100000)) AS row_num;

-- ============================================================================
-- End of Test Data
-- ============================================================================
