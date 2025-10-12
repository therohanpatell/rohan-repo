-- BigQuery Table Schema for DQ Configuration
-- This table stores all data quality validation check configurations
-- python main.py --mode dq --gcs_path gs://bucket/config.json --run_date 2024-01-15 --dq_target_table project.dataset.dq_results

CREATE TABLE `project.dataset.dq_config` (
  -- Primary identifier
  check_id STRING NOT NULL OPTIONS(description="Unique identifier for the DQ check (e.g., CHK_001_NULL_EMPLOYEE_ID)"),
  
  -- Check metadata
  category STRING NOT NULL OPTIONS(description="Category of the check (e.g., Null Check, Value Check, Threshold Check)"),
  description STRING NOT NULL OPTIONS(description="Human-readable description of what the check validates"),
  severity STRING NOT NULL OPTIONS(description="Severity level: Critical, High, Medium, or Low"),
  
  -- Query configuration
  sql_query STRING NOT NULL OPTIONS(description="BigQuery SQL query to execute for validation"),
  
  -- Expected output and comparison
  expected_output_type STRING NOT NULL OPTIONS(description="Type of expected output: numeric, array, object_array"),
  expected_output_string STRING OPTIONS(description="Expected output for numeric conditions (e.g., '0', '>=10', '<100')"),
  expected_output_array ARRAY<STRING> OPTIONS(description="Expected output for set_match or not_in_result (array of strings)"),
  expected_output_json STRING OPTIONS(description="Expected output for row_match (JSON string of array of objects)"),
  
  comparison_type STRING NOT NULL OPTIONS(description="Comparison type: numeric_condition, set_match, not_in_result, row_match"),
  
  -- Optional metadata
  impacted_downstream ARRAY<STRING> OPTIONS(description="List of downstream systems or reports impacted by this check"),
  tags ARRAY<STRING> OPTIONS(description="Tags for categorizing and filtering checks"),
  
  -- Status and tracking
  active BOOLEAN NOT NULL DEFAULT TRUE OPTIONS(description="Whether this check is active and should be executed"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the check was created"),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the check was last updated"),
  created_by STRING OPTIONS(description="User who created the check"),
  updated_by STRING OPTIONS(description="User who last updated the check")
)
OPTIONS(
  description="Data Quality validation configuration table storing all DQ check definitions"
);

-- Create a view for easier querying with parsed expected_output
CREATE OR REPLACE VIEW `project.dataset.dq_config_view` AS
SELECT
  check_id,
  category,
  description,
  severity,
  sql_query,
  comparison_type,
  
  -- Parse expected output based on type
  CASE 
    WHEN expected_output_type = 'numeric' THEN expected_output_string
    WHEN expected_output_type = 'array' THEN TO_JSON_STRING(expected_output_array)
    WHEN expected_output_type = 'object_array' THEN expected_output_json
    ELSE NULL
  END AS expected_output,
  
  expected_output_type,
  impacted_downstream,
  tags,
  active,
  created_at,
  updated_at,
  created_by,
  updated_by
FROM `project.dataset.dq_config`
WHERE active = TRUE
ORDER BY check_id;
