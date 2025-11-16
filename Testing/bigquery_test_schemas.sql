-- ============================================================================
-- BigQuery Test Table Schemas for Dynamic Schema Framework Testing
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Test Metrics Output Table (Standard Schema)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_metrics_output` (
  metric_id STRING NOT NULL,
  metric_name STRING NOT NULL,
  metric_type STRING NOT NULL,
  metric_description STRING,
  frequency STRING,
  numerator_value NUMERIC(38, 9),
  denominator_value NUMERIC(38, 9),
  metric_output NUMERIC(38, 9) NOT NULL,
  business_data_date STRING,
  partition_dt STRING NOT NULL,
  pipeline_execution_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Test table for standard metrics output with dynamic schema",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 2. Test Metrics Output Table 2 (Alternative Target)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_metrics_output_2` (
  metric_id STRING NOT NULL,
  metric_name STRING NOT NULL,
  metric_type STRING NOT NULL,
  metric_description STRING,
  frequency STRING,
  numerator_value NUMERIC(38, 9),
  denominator_value NUMERIC(38, 9),
  metric_output NUMERIC(38, 9) NOT NULL,
  business_data_date STRING,
  partition_dt STRING NOT NULL,
  pipeline_execution_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Alternative test table for testing multiple target tables",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 3. Test Custom Metrics Table (Custom Columns)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_custom_metrics` (
  metric_id STRING NOT NULL,
  metric_name STRING NOT NULL,
  metric_type STRING NOT NULL,
  metric_description STRING,
  frequency STRING,
  region STRING,
  gender STRING,
  department STRING,
  category STRING,
  numerator_value NUMERIC(38, 9),
  denominator_value NUMERIC(38, 9),
  metric_output NUMERIC(38, 9) NOT NULL,
  business_data_date STRING,
  partition_dt STRING NOT NULL,
  pipeline_execution_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Test table with custom columns not in standard METRICS_SCHEMA",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 4. Test Source Data Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_source_data` (
  id INT64 NOT NULL,
  name STRING,
  value FLOAT64,
  status STRING,
  created_date DATE,
  partition_dt STRING NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Test source data table for SQL queries",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 5. Test Employee Data Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_employee_data` (
  employee_id STRING NOT NULL,
  employee_name STRING,
  department_id STRING,
  region STRING,
  gender STRING,
  hire_date DATE,
  salary NUMERIC(15, 2),
  partition_dt STRING NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Test employee data for GROUP BY and JOIN queries",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 6. Test Department Data Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_department_data` (
  department_id STRING NOT NULL,
  department_name STRING,
  location STRING,
  partition_dt STRING NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Test department data for JOIN queries",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 7. Test Permanent Employees Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_perm_employees` (
  employee_id STRING NOT NULL,
  employee_name STRING,
  employee_type STRING,
  partition_dt STRING NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Test permanent employees data for CTE queries",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 8. Test Non-Permanent Employees Table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_non_perm_employees` (
  employee_id STRING NOT NULL,
  employee_name STRING,
  employee_type STRING,
  partition_dt STRING NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Test non-permanent employees data for CTE queries",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 9. Test Partition Info Table (Metadata)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_partition_info` (
  table_name STRING NOT NULL,
  partition_dt STRING NOT NULL,
  record_count INT64,
  last_updated TIMESTAMP
)
OPTIONS(
  description="Test partition info metadata table",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 10. Test Recon Table (Reconciliation)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_recon_table` (
  module_id STRING NOT NULL,
  module_type_nm STRING NOT NULL,
  source_databs_nm STRING,
  source_table_nm STRING,
  source_column_nm STRING,
  source_file_nm STRING,
  source_contrl_file_nm STRING,
  source_server_nm STRING NOT NULL,
  target_databs_nm STRING,
  target_table_nm STRING,
  target_column_nm STRING,
  target_file_nm STRING,
  target_contrl_file_nm STRING,
  target_server_nm STRING NOT NULL,
  source_vl STRING NOT NULL,
  target_vl STRING NOT NULL,
  clcltn_ds STRING,
  excldd_vl STRING,
  excldd_reason_tx STRING,
  tolrnc_pc STRING,
  rcncln_exact_pass_in STRING NOT NULL,
  rcncln_tolrnc_pass_in STRING,
  latest_source_parttn_dt STRING NOT NULL,
  latest_target_parttn_dt STRING NOT NULL,
  load_ts STRING NOT NULL,
  schdld_dt DATE NOT NULL,
  source_system_id STRING NOT NULL,
  schdld_yr INT64 NOT NULL,
  Job_Name STRING NOT NULL
)
OPTIONS(
  description="Test reconciliation tracking table",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 11. Test All Data Types Table (Schema Conversion Testing)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_all_data_types` (
  string_col STRING,
  int_col INT64,
  integer_col INTEGER,
  float_col FLOAT64,
  numeric_col NUMERIC(38, 9),
  decimal_col DECIMAL(20, 5),
  boolean_col BOOLEAN,
  bool_col BOOL,
  date_col DATE,
  timestamp_col TIMESTAMP,
  datetime_col DATETIME,
  required_string STRING NOT NULL,
  required_int INT64 NOT NULL,
  nullable_string STRING,
  nullable_int INT64
)
OPTIONS(
  description="Test table with all supported data types for schema conversion testing",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 12. Test Simple Table (3 columns)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_simple_table` (
  id INTEGER NOT NULL,
  name STRING,
  created_date DATE
)
OPTIONS(
  description="Simple test table with only 3 columns",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 13. Test Complex Table (25+ columns)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_complex_table` (
  col_01 STRING NOT NULL,
  col_02 STRING,
  col_03 INT64,
  col_04 INT64,
  col_05 FLOAT64,
  col_06 FLOAT64,
  col_07 NUMERIC(38, 9),
  col_08 NUMERIC(38, 9),
  col_09 BOOLEAN,
  col_10 BOOLEAN,
  col_11 DATE,
  col_12 DATE,
  col_13 TIMESTAMP,
  col_14 TIMESTAMP,
  col_15 STRING,
  col_16 STRING,
  col_17 INT64,
  col_18 INT64,
  col_19 FLOAT64,
  col_20 FLOAT64,
  col_21 NUMERIC(15, 2),
  col_22 NUMERIC(15, 2),
  col_23 STRING,
  col_24 STRING,
  col_25 DATE NOT NULL
)
OPTIONS(
  description="Complex test table with 25 columns of various types",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ----------------------------------------------------------------------------
-- 14. Test Wide Table (50+ columns)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `test_project.test_dataset.test_wide_table` (
  metric_id STRING NOT NULL,
  metric_name STRING NOT NULL,
  col_01 STRING, col_02 STRING, col_03 STRING, col_04 STRING, col_05 STRING,
  col_06 STRING, col_07 STRING, col_08 STRING, col_09 STRING, col_10 STRING,
  col_11 INT64, col_12 INT64, col_13 INT64, col_14 INT64, col_15 INT64,
  col_16 INT64, col_17 INT64, col_18 INT64, col_19 INT64, col_20 INT64,
  col_21 FLOAT64, col_22 FLOAT64, col_23 FLOAT64, col_24 FLOAT64, col_25 FLOAT64,
  col_26 FLOAT64, col_27 FLOAT64, col_28 FLOAT64, col_29 FLOAT64, col_30 FLOAT64,
  col_31 NUMERIC(38, 9), col_32 NUMERIC(38, 9), col_33 NUMERIC(38, 9), col_34 NUMERIC(38, 9), col_35 NUMERIC(38, 9),
  col_36 DATE, col_37 DATE, col_38 DATE, col_39 DATE, col_40 DATE,
  col_41 TIMESTAMP, col_42 TIMESTAMP, col_43 TIMESTAMP, col_44 TIMESTAMP, col_45 TIMESTAMP,
  col_46 BOOLEAN, col_47 BOOLEAN, col_48 BOOLEAN, col_49 BOOLEAN, col_50 BOOLEAN,
  partition_dt STRING NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
OPTIONS(
  description="Wide test table with 50+ columns for edge case testing",
  labels=[("environment", "test"), ("purpose", "unit_testing")]
);

-- ============================================================================
-- End of Schema Definitions
-- ============================================================================
