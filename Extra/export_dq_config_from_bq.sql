-- Query to export DQ configuration from BigQuery table to JSON format
-- This query formats the data to match the JSON configuration file structure
-- Run this in BigQuery Console and export results as JSON

SELECT
  check_id,
  category,
  sql_query,
  description,
  severity,
  
  -- Format expected_output based on type
  CASE 
    WHEN expected_output_type = 'numeric' THEN expected_output_string
    WHEN expected_output_type = 'array' THEN TO_JSON_STRING(expected_output_array)
    WHEN expected_output_type = 'object_array' THEN expected_output_json
    ELSE NULL
  END AS expected_output,
  
  comparison_type,
  impacted_downstream,
  tags,
  active
  
FROM `project.dataset.dq_config`
WHERE active = TRUE
ORDER BY check_id;

-- Alternative: Export as formatted JSON string
SELECT TO_JSON_STRING(
  ARRAY_AGG(
    STRUCT(
      check_id,
      category,
      sql_query,
      description,
      severity,
      CASE 
        WHEN expected_output_type = 'numeric' THEN expected_output_string
        WHEN expected_output_type = 'array' THEN TO_JSON_STRING(expected_output_array)
        WHEN expected_output_type = 'object_array' THEN expected_output_json
      END AS expected_output,
      comparison_type,
      impacted_downstream,
      tags,
      active
    )
  )
) AS json_config
FROM `project.dataset.dq_config`
WHERE active = TRUE
ORDER BY check_id;
