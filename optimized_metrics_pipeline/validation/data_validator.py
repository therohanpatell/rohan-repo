"""Data validation utilities for the metrics pipeline"""

from typing import Dict, Optional
from ..config.constants import ERROR_MESSAGES
from ..utils.exceptions import ValidationError
from ..utils.logging import LoggerMixin
from ..utils.numeric_utils import validate_denominator


class DataValidator(LoggerMixin):
    """Validator for data integrity and business rules"""
    
    def __init__(self, config=None):
        super().__init__()
        self.config = config
    
    def validate_sql_results(self, sql_results: Dict, metric_id: str) -> None:
        """
        Validate SQL execution results
        
        Args:
            sql_results: Dictionary with query results
            metric_id: Metric ID for error reporting
            
        Raises:
            ValidationError: If results are invalid
        """
        # Check for required business_data_date
        if sql_results.get('business_data_date') is None:
            raise ValidationError(ERROR_MESSAGES['missing_business_date'])
        
        # Validate denominator if present
        if self.config and self.config.validate_denominator:
            denominator_value = sql_results.get('denominator_value')
            if denominator_value is not None:
                validate_denominator(
                    denominator_value,
                    metric_id,
                    allow_negative=self.config.allow_negative_denominator,
                    log_small_values=self.config.log_small_denominator
                )
    
    def validate_metric_record(self, record: Dict) -> None:
        """
        Validate a complete metric record
        
        Args:
            record: Metric record dictionary
            
        Raises:
            ValidationError: If record is invalid
        """
        required_fields = [
            'metric_id', 'metric_name', 'metric_type',
            'business_data_date', 'partition_dt', 'pipeline_execution_ts'
        ]
        
        for field in required_fields:
            if field not in record or record[field] is None:
                raise ValidationError(f"Missing required field: {field}")
        
        # Validate numeric fields are properly formatted
        numeric_fields = ['numerator_value', 'denominator_value', 'metric_output']
        for field in numeric_fields:
            if field in record and record[field] is not None:
                try:
                    # Try to convert to ensure it's a valid numeric value
                    float(str(record[field]))
                except (ValueError, TypeError):
                    raise ValidationError(f"Invalid numeric value for {field}: {record[field]}")
    
    def validate_target_table_schema(self, df, expected_columns: list) -> None:
        """
        Validate DataFrame schema matches expected columns
        
        Args:
            df: Spark DataFrame
            expected_columns: List of expected column names
            
        Raises:
            ValidationError: If schema doesn't match
        """
        actual_columns = df.columns
        missing_columns = set(expected_columns) - set(actual_columns)
        
        if missing_columns:
            raise ValidationError(f"Missing columns in DataFrame: {missing_columns}")
        
        self.log_debug(f"Schema validation passed for columns: {actual_columns}")
    
    def validate_dependencies_consistency(self, filtered_data: list, dependencies: list) -> None:
        """
        Validate that filtered data contains all requested dependencies
        
        Args:
            filtered_data: List of filtered metric records
            dependencies: List of requested dependencies
            
        Raises:
            ValidationError: If dependencies are inconsistent
        """
        if not filtered_data:
            raise ValidationError(f"No records found for dependencies: {dependencies}")
        
        found_dependencies = set(record['dependency'] for record in filtered_data)
        missing_dependencies = set(dependencies) - found_dependencies
        
        if missing_dependencies:
            raise ValidationError(f"No records found for dependencies: {missing_dependencies}")
        
        self.log_info(f"Dependencies validation passed: {found_dependencies}")
    
    def validate_table_name_format(self, table_name: str) -> None:
        """
        Validate BigQuery table name format
        
        Args:
            table_name: Table name to validate
            
        Raises:
            ValidationError: If table name format is invalid
        """
        if not table_name or not isinstance(table_name, str):
            raise ValidationError("Table name must be a non-empty string")
        
        parts = table_name.split('.')
        if len(parts) != 3:
            raise ValidationError(f"Invalid table name format: {table_name}. Expected format: project.dataset.table")
        
        project, dataset, table = parts
        if not all([project.strip(), dataset.strip(), table.strip()]):
            raise ValidationError(f"Table name cannot have empty parts: {table_name}")
        
        self.log_debug(f"Table name validation passed: {table_name}")
    
    def validate_date_consistency(self, business_date: str, partition_date: str) -> None:
        """
        Validate date consistency between business date and partition date
        
        Args:
            business_date: Business data date
            partition_date: Partition date
            
        Raises:
            ValidationError: If dates are inconsistent
        """
        # This is a basic validation - you might want to implement more sophisticated
        # business rules for date consistency
        if not business_date or not partition_date:
            raise ValidationError("Business date and partition date cannot be empty")
        
        # Additional business rules can be added here
        self.log_debug(f"Date consistency validation passed: business={business_date}, partition={partition_date}")