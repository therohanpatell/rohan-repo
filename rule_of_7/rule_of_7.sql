/*
Headcount Masking Validation Query
This query simulates the masking logic without UPDATE statements.
Use this to validate your logic before running the production script.

INSTRUCTIONS:
1. Replace the parameter values in the DECLARE section
2. Run this query to see what records WOULD be masked
3. Verify the results match your expectations
4. Then run the Python script in production
*/

-- ============================================================================
-- PARAMETERS - Update these values
-- ============================================================================
DECLARE partition_date STRING DEFAULT '2024-01-15';
DECLARE metric_ids ARRAY<STRING> DEFAULT ['metric_001', 'metric_002'];
DECLARE low_headcount_threshold INT64 DEFAULT 7;

-- Update with your actual table reference
DECLARE project_id STRING DEFAULT 'your-project-id';
DECLARE dataset_id STRING DEFAULT 'your-dataset-id';
DECLARE table_id STRING DEFAULT 'your-table-id';

-- ============================================================================
-- MAIN QUERY - Simulates masking logic
-- ============================================================================

WITH 
-- Get base data for the specified partition and metrics
base_data AS (
  SELECT
    partition_dt,
    metric_id,
    org_level_1,
    org_level_2,
    org_level_3,
    org_level_4,
    org_level_5,
    org_level_6,
    org_level_7,
    region,
    location,
    headcount,
    stage_1_masking AS current_stage_1_masking,
    stage_2_masking AS current_stage_2_masking
  FROM `your-project-id.your-dataset-id.your-table-id`
  WHERE partition_dt = partition_date
    AND metric_id IN UNNEST(metric_ids)
),

-- STAGE 1 LOGIC: Identify records with headcount < threshold
stage_1_masking_logic AS (
  SELECT
    *,
    CASE 
      WHEN headcount < low_headcount_threshold THEN 'Yes'
      ELSE NULL
    END AS proposed_stage_1_masking
  FROM base_data
),

-- Calculate group-level statistics
group_statistics AS (
  SELECT
    metric_id,
    org_level_1,
    org_level_2,
    org_level_3,
    org_level_4,
    org_level_5,
    org_level_6,
    org_level_7,
    region,
    location,
    COUNT(*) AS total_records_in_group,
    COUNTIF(headcount < low_headcount_threshold) AS low_headcount_count,
    MIN(CASE WHEN headcount >= low_headcount_threshold THEN headcount END) AS second_lowest_headcount
  FROM base_data
  GROUP BY
    metric_id,
    org_level_1,
    org_level_2,
    org_level_3,
    org_level_4,
    org_level_5,
    org_level_6,
    org_level_7,
    region,
    location
),

-- Identify groups that qualify for Stage 2 masking
qualifying_groups AS (
  SELECT
    metric_id,
    org_level_1,
    org_level_2,
    org_level_3,
    org_level_4,
    org_level_5,
    org_level_6,
    org_level_7,
    region,
    location,
    second_lowest_headcount,
    low_headcount_count
  FROM group_statistics
  WHERE low_headcount_count = 1  -- Exactly one low-headcount record
    AND second_lowest_headcount IS NOT NULL  -- Has at least one record >= threshold
),

-- STAGE 2 LOGIC: Apply to second-lowest headcount in qualifying groups
stage_2_masking_logic AS (
  SELECT
    s1.*,
    CASE
      WHEN qg.metric_id IS NOT NULL 
        AND s1.headcount = qg.second_lowest_headcount
        AND s1.headcount >= low_headcount_threshold
      THEN 'Yes'
      ELSE NULL
    END AS proposed_stage_2_masking,
    qg.second_lowest_headcount AS group_second_lowest,
    gs.low_headcount_count AS group_low_count,
    gs.total_records_in_group
  FROM stage_1_masking_logic s1
  LEFT JOIN group_statistics gs
    ON s1.metric_id = gs.metric_id
    AND s1.org_level_1 = gs.org_level_1
    AND s1.org_level_2 = gs.org_level_2
    AND s1.org_level_3 = gs.org_level_3
    AND s1.org_level_4 = gs.org_level_4
    AND s1.org_level_5 = gs.org_level_5
    AND s1.org_level_6 = gs.org_level_6
    AND s1.org_level_7 = gs.org_level_7
    AND s1.region = gs.region
    AND s1.location = gs.location
  LEFT JOIN qualifying_groups qg
    ON s1.metric_id = qg.metric_id
    AND s1.org_level_1 = qg.org_level_1
    AND s1.org_level_2 = qg.org_level_2
    AND s1.org_level_3 = qg.org_level_3
    AND s1.org_level_4 = qg.org_level_4
    AND s1.org_level_5 = qg.org_level_5
    AND s1.org_level_6 = qg.org_level_6
    AND s1.org_level_7 = qg.org_level_7
    AND s1.region = qg.region
    AND s1.location = qg.location
),

-- Final results with comparison
final_results AS (
  SELECT
    *,
    -- Flags to show what will change
    CASE
      WHEN current_stage_1_masking IS NULL AND proposed_stage_1_masking = 'Yes' THEN '✓ WILL BE MASKED'
      WHEN current_stage_1_masking = 'Yes' AND proposed_stage_1_masking = 'Yes' THEN 'Already Masked'
      ELSE 'No Change'
    END AS stage_1_change,
    CASE
      WHEN current_stage_2_masking IS NULL AND proposed_stage_2_masking = 'Yes' THEN '✓ WILL BE MASKED'
      WHEN current_stage_2_masking = 'Yes' AND proposed_stage_2_masking = 'Yes' THEN 'Already Masked'
      ELSE 'No Change'
    END AS stage_2_change,
    -- Validation flags
    CASE
      WHEN headcount < low_headcount_threshold AND proposed_stage_1_masking IS NULL 
      THEN '❌ ERROR: Low headcount not masked!'
      WHEN headcount < low_headcount_threshold AND proposed_stage_2_masking = 'Yes'
      THEN '❌ ERROR: Low headcount should not have Stage 2!'
      ELSE '✓ Valid'
    END AS validation_status
  FROM stage_2_masking_logic
)

-- ============================================================================
-- OUTPUT: Detailed record-level view
-- ============================================================================
SELECT
  partition_dt,
  metric_id,
  org_level_1,
  org_level_2,
  org_level_3,
  org_level_4,
  org_level_5,
  org_level_6,
  org_level_7,
  region,
  location,
  headcount,
  -- Current state
  current_stage_1_masking,
  current_stage_2_masking,
  -- Proposed changes
  proposed_stage_1_masking,
  proposed_stage_2_masking,
  -- What will change
  stage_1_change,
  stage_2_change,
  -- Group context
  total_records_in_group,
  group_low_count,
  group_second_lowest,
  -- Validation
  validation_status
FROM final_results
ORDER BY
  metric_id,
  org_level_1,
  org_level_2,
  org_level_3,
  org_level_4,
  org_level_5,
  org_level_6,
  org_level_7,
  region,
  location,
  headcount;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================
-- Uncomment the section below to see summary statistics instead

/*
WITH base_data AS (
  SELECT
    partition_dt,
    metric_id,
    org_level_1,
    org_level_2,
    org_level_3,
    org_level_4,
    org_level_5,
    org_level_6,
    org_level_7,
    region,
    location,
    headcount,
    stage_1_masking AS current_stage_1_masking,
    stage_2_masking AS current_stage_2_masking
  FROM `your-project-id.your-dataset-id.your-table-id`
  WHERE partition_dt = '2024-01-15'
    AND metric_id IN ('metric_001', 'metric_002')
),

stage_1_masking_logic AS (
  SELECT
    *,
    CASE WHEN headcount < 7 THEN 'Yes' ELSE NULL END AS proposed_stage_1_masking
  FROM base_data
),

group_statistics AS (
  SELECT
    metric_id,
    org_level_1, org_level_2, org_level_3, org_level_4,
    org_level_5, org_level_6, org_level_7,
    region, location,
    COUNT(*) AS total_records_in_group,
    COUNTIF(headcount < 7) AS low_headcount_count,
    MIN(CASE WHEN headcount >= 7 THEN headcount END) AS second_lowest_headcount
  FROM base_data
  GROUP BY metric_id, org_level_1, org_level_2, org_level_3, org_level_4,
           org_level_5, org_level_6, org_level_7, region, location
),

qualifying_groups AS (
  SELECT *
  FROM group_statistics
  WHERE low_headcount_count = 1
    AND second_lowest_headcount IS NOT NULL
),

stage_2_masking_logic AS (
  SELECT
    s1.*,
    CASE
      WHEN qg.metric_id IS NOT NULL 
        AND s1.headcount = qg.second_lowest_headcount
        AND s1.headcount >= 7
      THEN 'Yes'
      ELSE NULL
    END AS proposed_stage_2_masking
  FROM stage_1_masking_logic s1
  LEFT JOIN group_statistics gs
    ON s1.metric_id = gs.metric_id
    AND s1.org_level_1 = gs.org_level_1
    AND s1.org_level_2 = gs.org_level_2
    AND s1.org_level_3 = gs.org_level_3
    AND s1.org_level_4 = gs.org_level_4
    AND s1.org_level_5 = gs.org_level_5
    AND s1.org_level_6 = gs.org_level_6
    AND s1.org_level_7 = gs.org_level_7
    AND s1.region = gs.region
    AND s1.location = gs.location
  LEFT JOIN qualifying_groups qg
    ON s1.metric_id = qg.metric_id
    AND s1.org_level_1 = qg.org_level_1
    AND s1.org_level_2 = qg.org_level_2
    AND s1.org_level_3 = qg.org_level_3
    AND s1.org_level_4 = qg.org_level_4
    AND s1.org_level_5 = qg.org_level_5
    AND s1.org_level_6 = qg.org_level_6
    AND s1.org_level_7 = qg.org_level_7
    AND s1.region = qg.region
    AND s1.location = qg.location
)

SELECT
  'Total Records' AS metric,
  COUNT(*) AS count
FROM stage_2_masking_logic

UNION ALL

SELECT
  'Records to be Stage 1 Masked' AS metric,
  COUNTIF(current_stage_1_masking IS NULL AND proposed_stage_1_masking = 'Yes') AS count
FROM stage_2_masking_logic

UNION ALL

SELECT
  'Records to be Stage 2 Masked' AS metric,
  COUNTIF(current_stage_2_masking IS NULL AND proposed_stage_2_masking = 'Yes') AS count
FROM stage_2_masking_logic

UNION ALL

SELECT
  'Records Already Stage 1 Masked' AS metric,
  COUNTIF(current_stage_1_masking = 'Yes') AS count
FROM stage_2_masking_logic

UNION ALL

SELECT
  'Records Already Stage 2 Masked' AS metric,
  COUNTIF(current_stage_2_masking = 'Yes') AS count
FROM stage_2_masking_logic

UNION ALL

SELECT
  'Total Unique Groups' AS metric,
  COUNT(DISTINCT CONCAT(
    metric_id, '|', org_level_1, '|', org_level_2, '|', org_level_3, '|',
    org_level_4, '|', org_level_5, '|', org_level_6, '|', org_level_7, '|',
    region, '|', location
  )) AS count
FROM stage_2_masking_logic

UNION ALL

SELECT
  'Groups Qualifying for Stage 2' AS metric,
  COUNT(*) AS count
FROM qualifying_groups

UNION ALL

SELECT
  'Validation Errors (Low HC not masked)' AS metric,
  COUNTIF(headcount < 7 AND proposed_stage_1_masking IS NULL) AS count
FROM stage_2_masking_logic

UNION ALL

SELECT
  'Validation Errors (Stage 2 on Low HC)' AS metric,
  COUNTIF(headcount < 7 AND proposed_stage_2_masking = 'Yes') AS count
FROM stage_2_masking_logic

ORDER BY metric;
*/