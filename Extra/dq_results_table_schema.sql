-- BigQuery Table Schema for DQ Results
-- This table stores the results of all data quality validation checks

CREATE TABLE `project.dataset.dq_results` (
  -- Check identification
  check_id STRING NOT NULL OPTIONS(description="Unique identifier for the DQ check (e.g., CHK_001_NULL_EMPLOYEE_ID)"),
  
  -- Check metadata
  category STRING NOT NULL OPTIONS(description="Category of the check (e.g., Null Check, Value Check, Threshold Check, Row Match)"),
  description STRING OPTIONS(description="Human-readable description of what the check validates"),
  severity STRING NOT NULL OPTIONS(description="Severity level: Critical, High, Medium, or Low"),
  
  -- Query and validation details
  sql_query STRING NOT NULL OPTIONS(description="BigQuery SQL query that was executed for validation"),
  expected_output STRING OPTIONS(description="Expected output serialized as JSON string"),
  actual_result STRING OPTIONS(description="Actual query result serialized as JSON string"),
  comparison_type STRING NOT NULL OPTIONS(description="Type of comparison: numeric_condition, set_match, not_in_result, row_match"),
  
  -- Validation results
  validation_status STRING NOT NULL OPTIONS(description="Validation result: PASS or FAIL"),
  error_message STRING OPTIONS(description="Error message or failure reason if validation failed"),
  
  -- Impact and categorization
  impacted_downstream ARRAY<STRING> OPTIONS(description="List of downstream systems or reports impacted by this check"),
  tags ARRAY<STRING> OPTIONS(description="Tags for categorizing and filtering checks"),
  
  -- Execution metadata
  execution_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the check was executed"),
  execution_duration FLOAT64 OPTIONS(description="Execution time in seconds"),
  
  -- Date tracking
  run_dt STRING NOT NULL OPTIONS(description="Business date for the validation run (YYYY-MM-DD)"),
  partition_dt STRING NOT NULL OPTIONS(description="Partition date for data organization (YYYY-MM-DD)")
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
CLUSTER BY validation_status, severity, category
OPTIONS(
  description="Data Quality validation results table storing all DQ check execution results and validation outcomes",
  require_partition_filter=true
);

-- Create indexes for common query patterns
CREATE INDEX idx_check_id ON `project.dataset.dq_results`(check_id);
CREATE INDEX idx_validation_status ON `project.dataset.dq_results`(validation_status);
CREATE INDEX idx_severity ON `project.dataset.dq_results`(severity);

-- Create a view for easy querying of recent results
CREATE OR REPLACE VIEW `project.dataset.dq_results_latest` AS
SELECT
  check_id,
  category,
  description,
  severity,
  validation_status,
  error_message,
  impacted_downstream,
  tags,
  execution_timestamp,
  execution_duration,
  run_dt,
  partition_dt,
  -- Parse expected and actual for display
  SAFE.PARSE_JSON(expected_output) AS expected_output_json,
  SAFE.PARSE_JSON(actual_result) AS actual_result_json
FROM `project.dataset.dq_results`
WHERE partition_dt = (SELECT MAX(partition_dt) FROM `project.dataset.dq_results`)
ORDER BY 
  CASE validation_status 
    WHEN 'FAIL' THEN 1 
    WHEN 'PASS' THEN 2 
    ELSE 3 
  END,
  CASE severity
    WHEN 'Critical' THEN 1
    WHEN 'High' THEN 2
    WHEN 'Medium' THEN 3
    WHEN 'Low' THEN 4
    ELSE 5
  END,
  check_id;

-- Create a view for failed checks summary
CREATE OR REPLACE VIEW `project.dataset.dq_results_failed_summary` AS
SELECT
  partition_dt,
  run_dt,
  category,
  severity,
  COUNT(*) AS failed_count,
  ARRAY_AGG(STRUCT(
    check_id,
    description,
    error_message,
    execution_timestamp
  ) ORDER BY execution_timestamp DESC) AS failed_checks
FROM `project.dataset.dq_results`
WHERE validation_status = 'FAIL'
GROUP BY partition_dt, run_dt, category, severity
ORDER BY partition_dt DESC, severity, category;

-- Create a view for check execution trends
CREATE OR REPLACE VIEW `project.dataset.dq_results_trends` AS
SELECT
  check_id,
  category,
  severity,
  DATE(PARSE_DATE('%Y-%m-%d', partition_dt)) AS check_date,
  COUNT(*) AS execution_count,
  COUNTIF(validation_status = 'PASS') AS pass_count,
  COUNTIF(validation_status = 'FAIL') AS fail_count,
  ROUND(AVG(execution_duration), 2) AS avg_execution_duration,
  ROUND(MIN(execution_duration), 2) AS min_execution_duration,
  ROUND(MAX(execution_duration), 2) AS max_execution_duration
FROM `project.dataset.dq_results`
GROUP BY check_id, category, severity, check_date
ORDER BY check_date DESC, check_id;

-- Create a view for daily summary statistics
CREATE OR REPLACE VIEW `project.dataset.dq_results_daily_summary` AS
SELECT
  partition_dt,
  run_dt,
  COUNT(DISTINCT check_id) AS total_checks,
  COUNTIF(validation_status = 'PASS') AS passed_checks,
  COUNTIF(validation_status = 'FAIL') AS failed_checks,
  ROUND(COUNTIF(validation_status = 'PASS') / COUNT(*) * 100, 2) AS pass_rate_pct,
  ROUND(AVG(execution_duration), 2) AS avg_execution_duration,
  ROUND(SUM(execution_duration), 2) AS total_execution_duration,
  -- Breakdown by severity
  COUNTIF(severity = 'Critical' AND validation_status = 'FAIL') AS critical_failures,
  COUNTIF(severity = 'High' AND validation_status = 'FAIL') AS high_failures,
  COUNTIF(severity = 'Medium' AND validation_status = 'FAIL') AS medium_failures,
  COUNTIF(severity = 'Low' AND validation_status = 'FAIL') AS low_failures,
  -- Breakdown by category
  COUNTIF(category LIKE '%Null%') AS null_checks,
  COUNTIF(category LIKE '%Value%' OR category LIKE '%Set%') AS value_checks,
  COUNTIF(category LIKE '%Threshold%') AS threshold_checks,
  COUNTIF(category LIKE '%Row%') AS row_match_checks,
  MIN(execution_timestamp) AS first_execution,
  MAX(execution_timestamp) AS last_execution
FROM `project.dataset.dq_results`
GROUP BY partition_dt, run_dt
ORDER BY partition_dt DESC;

-- Create a view for impacted downstream systems
CREATE OR REPLACE VIEW `project.dataset.dq_results_downstream_impact` AS
SELECT
  partition_dt,
  run_dt,
  downstream_system,
  COUNT(DISTINCT check_id) AS total_checks,
  COUNTIF(validation_status = 'FAIL') AS failed_checks,
  ARRAY_AGG(
    IF(validation_status = 'FAIL',
      STRUCT(
        check_id,
        category,
        severity,
        description,
        error_message
      ),
      NULL
    ) IGNORE NULLS
  ) AS failed_check_details
FROM `project.dataset.dq_results`,
UNNEST(impacted_downstream) AS downstream_system
GROUP BY partition_dt, run_dt, downstream_system
HAVING failed_checks > 0
ORDER BY partition_dt DESC, failed_checks DESC;
