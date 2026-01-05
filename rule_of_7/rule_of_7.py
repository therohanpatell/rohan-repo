"""
Production-Ready Hierarchy-Based Headcount Masking for BigQuery
Designed to run on Dataproc cluster with Airflow orchestration.

Implements two-stage masking logic based on headcount thresholds and organizational hierarchy.
Masking is evaluated at org_level_1 through org_level_7 group level.
Multiple records may exist per org hierarchy (due to region, gender, etc.)
"""

import argparse
import sys
import json
from google.cloud import bigquery
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class HeadcountMaskingJob:
    """
    Production-ready headcount masking processor for partitioned BigQuery tables.
    Masking decisions are made at the org_level_1 through org_level_7 group level.
    """
    
    # Default hierarchy columns for grouping
    DEFAULT_HIERARCHY_COLUMNS = [
        'org_level_1', 'org_level_2', 'org_level_3', 'org_level_4',
        'org_level_5', 'org_level_6', 'org_level_7'
    ]
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        partition_column: str = 'partition_dt',
        hierarchy_columns: Optional[List[str]] = None
    ):
        """
        Initialize the masking job processor.
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            partition_column: Name of the partition column (default: 'partition_dt')
            hierarchy_columns: List of hierarchy columns (defaults to org_level_1 through org_level_7)
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        self.partition_column = partition_column
        
        # Set hierarchy columns (only these are used for grouping)
        self.hierarchy_columns = hierarchy_columns or self.DEFAULT_HIERARCHY_COLUMNS
        
        logger.info(f"Initialized HeadcountMaskingJob")
        logger.info(f"Target table: {self.full_table_id}")
        logger.info(f"Partition column: {self.partition_column}")
        logger.info(f"Hierarchy columns for grouping: {self.hierarchy_columns}")
    
    def execute(
        self,
        partition_date: str,
        metric_ids: List[str],
        headcount_column: str = 'headcount',
        low_headcount_threshold: int = 7,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Execute the masking job for a specific partition and metric IDs.
        
        Args:
            partition_date: Partition date to process (format: YYYY-MM-DD)
            metric_ids: List of metric IDs to process
            headcount_column: Name of the headcount column (default: 'headcount')
            low_headcount_threshold: Threshold for low headcount (default: 7)
            dry_run: If True, only preview without applying changes
            
        Returns:
            Dictionary with execution results and statistics
        """
        start_time = datetime.now()
        
        logger.info("=" * 80)
        logger.info("STARTING HEADCOUNT MASKING JOB")
        logger.info("=" * 80)
        logger.info(f"Partition date: {partition_date}")
        logger.info(f"Metric IDs: {metric_ids}")
        logger.info(f"Low headcount threshold: {low_headcount_threshold}")
        logger.info(f"Dry run mode: {dry_run}")
        
        # Validate inputs
        self._validate_inputs(partition_date, metric_ids, headcount_column, low_headcount_threshold)
        
        # Build metric_ids SQL string
        metric_ids_str = ", ".join([f"'{mid}'" for mid in metric_ids])
        
        # Check partition exists
        self._verify_partition_exists(partition_date)
        
        # Preview impact
        logger.info("Analyzing masking impact...")
        preview = self._preview_impact(partition_date, metric_ids_str, headcount_column, low_headcount_threshold)
        logger.info(f"Impact preview: {json.dumps(preview, indent=2)}")
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be applied")
            return {
                'status': 'dry_run_complete',
                'partition_date': partition_date,
                'preview': preview,
                'execution_time_seconds': (datetime.now() - start_time).total_seconds()
            }
        
        # Apply Stage 1 masking
        logger.info("Applying Stage 1 masking...")
        stage1_count = self._apply_stage1_masking(
            partition_date, metric_ids_str, headcount_column, low_headcount_threshold
        )
        logger.info(f"Stage 1: {stage1_count} records masked")
        
        # Apply Stage 2 masking
        logger.info("Applying Stage 2 masking...")
        stage2_count = self._apply_stage2_masking(
            partition_date, metric_ids_str, headcount_column, low_headcount_threshold
        )
        logger.info(f"Stage 2: {stage2_count} records masked")
        
        # Verify results
        logger.info("Verifying masking results...")
        verification = self._verify_masking(
            partition_date, metric_ids_str, headcount_column, low_headcount_threshold
        )
        
        # Check for errors
        has_errors = self._check_verification_errors(verification)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            'status': 'failed' if has_errors else 'success',
            'partition_date': partition_date,
            'metric_ids': metric_ids,
            'stage1_masked_count': stage1_count,
            'stage2_masked_count': stage2_count,
            'verification': verification,
            'preview': preview,
            'execution_time_seconds': execution_time
        }
        
        logger.info("=" * 80)
        logger.info(f"JOB COMPLETED - Status: {result['status'].upper()}")
        logger.info("=" * 80)
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        logger.info(f"Results: {json.dumps(result, indent=2)}")
        
        if has_errors:
            raise RuntimeError(f"Masking verification failed. See logs for details.")
        
        return result
    
    def _validate_inputs(
        self,
        partition_date: str,
        metric_ids: List[str],
        headcount_column: str,
        low_headcount_threshold: int
    ):
        """Validate input parameters."""
        if not partition_date:
            raise ValueError("partition_date is required")
        
        if not metric_ids or len(metric_ids) == 0:
            raise ValueError("At least one metric_id is required")
        
        if low_headcount_threshold < 1:
            raise ValueError("low_headcount_threshold must be >= 1")
        
        # Validate date format
        try:
            datetime.strptime(partition_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Invalid partition_date format: {partition_date}. Expected YYYY-MM-DD")
        
        logger.info("Input validation passed")
    
    def _verify_partition_exists(self, partition_date: str):
        """Verify that the specified partition exists."""
        query = f"""
        SELECT COUNT(*) as record_count
        FROM `{self.full_table_id}`
        WHERE {self.partition_column} = '{partition_date}'
        LIMIT 1
        """
        
        try:
            result = list(self.client.query(query).result())
            if not result or result[0].record_count == 0:
                logger.warning(f"Partition {partition_date} exists but contains no records")
        except Exception as e:
            raise RuntimeError(f"Failed to verify partition: {str(e)}")
    
    def _build_group_by_clause(self, alias: str = "") -> str:
        """Build GROUP BY clause with hierarchy columns only."""
        prefix = f"{alias}." if alias else ""
        return ", ".join([f"{prefix}{col}" for col in self.hierarchy_columns])
    
    def _build_join_conditions(self, left_alias: str, right_alias: str, metric_included: bool = True) -> str:
        """Build JOIN conditions for hierarchy columns."""
        conditions = []
        
        if metric_included:
            conditions.append(f"{left_alias}.metric_id = {right_alias}.metric_id")
        
        for col in self.hierarchy_columns:
            conditions.append(f"{left_alias}.{col} = {right_alias}.{col}")
        
        return " AND ".join(conditions)
    
    def _preview_impact(
        self,
        partition_date: str,
        metric_ids_str: str,
        headcount_column: str,
        threshold: int
    ) -> Dict[str, Any]:
        """Preview the impact of masking without applying changes."""
        group_by_cols = self._build_group_by_clause()
        
        query = f"""
        WITH hierarchy_stats AS (
          SELECT
            metric_id,
            {group_by_cols},
            COUNT(*) AS total_records_in_group,
            COUNTIF({headcount_column} < {threshold}) AS low_headcount_count,
            MIN(CASE WHEN {headcount_column} >= {threshold} THEN {headcount_column} END) AS second_lowest_headcount
          FROM `{self.full_table_id}`
          WHERE {self.partition_column} = '{partition_date}'
            AND metric_id IN ({metric_ids_str})
          GROUP BY metric_id, {group_by_cols}
        )
        
        SELECT
          COUNT(*) AS total_groups,
          SUM(total_records_in_group) AS total_records,
          SUM(CASE WHEN low_headcount_count > 0 THEN 1 ELSE 0 END) AS groups_with_low_headcount,
          SUM(CASE WHEN low_headcount_count = 1 THEN 1 ELSE 0 END) AS groups_qualifying_stage2,
          SUM(CASE WHEN low_headcount_count >= 2 THEN 1 ELSE 0 END) AS groups_excluded_stage2,
          SUM(low_headcount_count) AS estimated_stage1_records
        FROM hierarchy_stats
        """
        
        query_job = self.client.query(query)
        results = list(query_job.result())
        
        if results:
            row = results[0]
            return {
                'total_groups': row.total_groups,
                'total_records': row.total_records,
                'groups_with_low_headcount': row.groups_with_low_headcount,
                'groups_qualifying_stage2': row.groups_qualifying_stage2,
                'groups_excluded_stage2': row.groups_excluded_stage2,
                'estimated_stage1_records': row.estimated_stage1_records
            }
        
        return {}
    
    def _apply_stage1_masking(
        self,
        partition_date: str,
        metric_ids_str: str,
        headcount_column: str,
        threshold: int
    ) -> int:
        """
        Apply Stage 1 masking: Mark all records with headcount < threshold.
        
        This applies to individual records regardless of group membership.
        """
        query = f"""
        UPDATE `{self.full_table_id}`
        SET stage_1_masked = "Yes"
        WHERE {self.partition_column} = '{partition_date}'
          AND metric_id IN ({metric_ids_str})
          AND {headcount_column} < {threshold}
          AND stage_1_masked IS NULL
        """
        
        logger.info("Executing Stage 1 masking query...")
        query_job = self.client.query(query)
        query_job.result()  # Wait for completion
        
        return query_job.num_dml_affected_rows
    
    def _apply_stage2_masking(
        self,
        partition_date: str,
        metric_ids_str: str,
        headcount_column: str,
        threshold: int
    ) -> int:
        """
        Apply Stage 2 masking: Mark second-lowest headcount in qualifying groups.
        
        Groups are defined by metric_id + org_level_1 through org_level_7.
        Multiple records may exist per group (due to region, gender, etc.) and ALL
        records with the second-lowest headcount value will be masked.
        """
        group_by_cols = self._build_group_by_clause()
        group_by_cols_t = self._build_group_by_clause("t")
        join_conditions = self._build_join_conditions("t", "hg")
        join_conditions_sl = self._build_join_conditions("t", "sl")
        
        query = f"""
        -- Step 1: Identify hierarchy groups with exactly one low-headcount record
        WITH hierarchy_groups AS (
          SELECT
            metric_id,
            {group_by_cols},
            COUNTIF({headcount_column} < {threshold}) AS low_headcount_records
          FROM `{self.full_table_id}`
          WHERE {self.partition_column} = '{partition_date}'
            AND metric_id IN ({metric_ids_str})
          GROUP BY metric_id, {group_by_cols}
          HAVING low_headcount_records = 1
        ),
        
        -- Step 2: Find the second-lowest headcount (lowest >= threshold) for qualifying groups
        second_lowest_headcount AS (
          SELECT
            t.metric_id,
            {group_by_cols_t},
            MIN(t.{headcount_column}) AS target_headcount
          FROM `{self.full_table_id}` t
          INNER JOIN hierarchy_groups hg
            ON {join_conditions}
          WHERE t.{self.partition_column} = '{partition_date}'
            AND t.{headcount_column} >= {threshold}
          GROUP BY t.metric_id, {group_by_cols_t}
        ),
        
        -- Step 3: Identify ALL records with the target headcount in qualifying groups
        -- This captures all records even if region/gender differ within same org hierarchy
        records_to_mask AS (
          SELECT
            t.metric_id,
            {group_by_cols_t},
            t.{headcount_column}
          FROM `{self.full_table_id}` t
          INNER JOIN second_lowest_headcount sl
            ON {join_conditions_sl}
            AND t.{headcount_column} = sl.target_headcount
          WHERE t.{self.partition_column} = '{partition_date}'
        )
        
        -- Step 4: Update ALL records matching the criteria
        UPDATE `{self.full_table_id}` AS target
        SET stage_2_masked = "Yes"
        WHERE target.{self.partition_column} = '{partition_date}'
          AND EXISTS (
            SELECT 1
            FROM records_to_mask rtm
            WHERE target.metric_id = rtm.metric_id
              AND target.{headcount_column} = rtm.{headcount_column}
              AND {self._build_join_conditions('target', 'rtm', metric_included=False)}
          )
          AND target.stage_2_masked IS NULL
        """
        
        logger.info("Executing Stage 2 masking query...")
        query_job = self.client.query(query)
        query_job.result()  # Wait for completion
        
        return query_job.num_dml_affected_rows
    
    def _verify_masking(
        self,
        partition_date: str,
        metric_ids_str: str,
        headcount_column: str,
        threshold: int
    ) -> Dict[str, Any]:
        """Verify the masking results."""
        query = f"""
        SELECT
          COUNT(*) AS total_records,
          COUNTIF(stage_1_masked = "Yes") AS stage1_masked,
          COUNTIF(stage_2_masked = "Yes") AS stage2_masked,
          COUNTIF(stage_1_masked = "Yes" AND stage_2_masked = "Yes") AS both_masked,
          COUNTIF({headcount_column} < {threshold} AND stage_1_masked IS NULL) AS unmasked_low_headcount,
          COUNTIF({headcount_column} < {threshold} AND stage_2_masked = "Yes") AS invalid_stage2_on_low,
          COUNTIF(stage_1_masked NOT IN ("Yes") AND stage_1_masked IS NOT NULL) AS invalid_stage1_values,
          COUNTIF(stage_2_masked NOT IN ("Yes") AND stage_2_masked IS NOT NULL) AS invalid_stage2_values
        FROM `{self.full_table_id}`
        WHERE {self.partition_column} = '{partition_date}'
          AND metric_id IN ({metric_ids_str})
        """
        
        query_job = self.client.query(query)
        results = list(query_job.result())
        
        if results:
            row = results[0]
            return {
                'total_records': row.total_records,
                'stage1_masked': row.stage1_masked,
                'stage2_masked': row.stage2_masked,
                'both_masked': row.both_masked,
                'unmasked_low_headcount': row.unmasked_low_headcount,
                'invalid_stage2_on_low': row.invalid_stage2_on_low,
                'invalid_stage1_values': row.invalid_stage1_values,
                'invalid_stage2_values': row.invalid_stage2_values
            }
        
        return {}
    
    def _check_verification_errors(self, verification: Dict[str, Any]) -> bool:
        """Check verification results for errors and log warnings."""
        has_errors = False
        
        if verification.get('unmasked_low_headcount', 0) > 0:
            logger.error(f"VALIDATION ERROR: {verification['unmasked_low_headcount']} records with low headcount are not masked!")
            has_errors = True
        
        if verification.get('invalid_stage2_on_low', 0) > 0:
            logger.error(f"VALIDATION ERROR: {verification['invalid_stage2_on_low']} low-headcount records incorrectly marked for Stage 2!")
            has_errors = True
        
        if verification.get('invalid_stage1_values', 0) > 0:
            logger.error(f"VALIDATION ERROR: {verification['invalid_stage1_values']} records have invalid stage_1_masked values!")
            has_errors = True
        
        if verification.get('invalid_stage2_values', 0) > 0:
            logger.error(f"VALIDATION ERROR: {verification['invalid_stage2_values']} records have invalid stage_2_masked values!")
            has_errors = True
        
        if verification.get('both_masked', 0) > 0:
            logger.warning(f"Note: {verification['both_masked']} records are masked at both stages (this is valid)")
        
        return has_errors


def parse_arguments():
    """Parse command line arguments for Airflow/Dataproc execution."""
    parser = argparse.ArgumentParser(
        description='Execute headcount masking job on BigQuery partitioned table'
    )
    
    # Required arguments
    parser.add_argument(
        '--project_id',
        required=True,
        help='GCP project ID'
    )
    parser.add_argument(
        '--dataset_id',
        required=True,
        help='BigQuery dataset ID'
    )
    parser.add_argument(
        '--table_id',
        required=True,
        help='BigQuery table ID'
    )
    parser.add_argument(
        '--partition_date',
        required=True,
        help='Partition date to process (format: YYYY-MM-DD)'
    )
    parser.add_argument(
        '--metric_ids',
        required=True,
        help='Comma-separated list of metric IDs to process'
    )
    
    # Optional arguments
    parser.add_argument(
        '--partition_column',
        default='partition_dt',
        help='Name of the partition column (default: partition_dt)'
    )
    parser.add_argument(
        '--headcount_column',
        default='headcount',
        help='Name of the headcount column (default: headcount)'
    )
    parser.add_argument(
        '--low_headcount_threshold',
        type=int,
        default=7,
        help='Threshold for low headcount (default: 7)'
    )
    parser.add_argument(
        '--hierarchy_columns',
        default='org_level_1,org_level_2,org_level_3,org_level_4,org_level_5,org_level_6,org_level_7',
        help='Comma-separated list of hierarchy columns'
    )
    parser.add_argument(
        '--dry_run',
        action='store_true',
        help='Preview impact without applying changes'
    )
    
    return parser.parse_args()


def main():
    """Main entry point for the masking job."""
    try:
        # Parse arguments
        args = parse_arguments()
        
        # Parse comma-separated values
        metric_ids = [mid.strip() for mid in args.metric_ids.split(',')]
        hierarchy_columns = [col.strip() for col in args.hierarchy_columns.split(',')]
        
        # Initialize job
        job = HeadcountMaskingJob(
            project_id=args.project_id,
            dataset_id=args.dataset_id,
            table_id=args.table_id,
            partition_column=args.partition_column,
            hierarchy_columns=hierarchy_columns
        )
        
        # Execute job
        result = job.execute(
            partition_date=args.partition_date,
            metric_ids=metric_ids,
            headcount_column=args.headcount_column,
            low_headcount_threshold=args.low_headcount_threshold,
            dry_run=args.dry_run
        )
        
        # Print result for Airflow to capture
        print(json.dumps(result, indent=2))
        
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
