"""JSON validation utilities for the metrics pipeline"""

import re
from typing import Dict, List, Set
from ..config.constants import REQUIRED_JSON_FIELDS, PLACEHOLDER_PATTERNS, ERROR_MESSAGES
from ..utils.exceptions import ValidationError
from ..utils.logging import LoggerMixin


class JsonValidator(LoggerMixin):
    """Validator for JSON metric definitions"""
    
    def __init__(self):
        super().__init__()
        self.required_fields = REQUIRED_JSON_FIELDS
        self.placeholder_patterns = PLACEHOLDER_PATTERNS
    
    def validate_json_data(self, json_data: List[Dict]) -> List[Dict]:
        """
        Validate JSON data for required fields and duplicates
        
        Args:
            json_data: List of metric definitions
            
        Returns:
            List of validated metric definitions
            
        Raises:
            ValidationError: If validation fails
        """
        self.log_info("Validating JSON data")
        
        # Track metric IDs to check for duplicates
        metric_ids: Set[str] = set()
        
        for i, record in enumerate(json_data):
            self._validate_record_fields(record, i)
            self._validate_metric_id_uniqueness(record, i, metric_ids)
            self._validate_target_table_format(record, i)
            self._validate_sql_placeholders(record, i)
        
        self.log_info(f"Successfully validated {len(json_data)} records with {len(metric_ids)} unique metric IDs")
        return json_data
    
    def _validate_record_fields(self, record: Dict, index: int) -> None:
        """Validate required fields in a record"""
        for field in self.required_fields:
            if field not in record:
                raise ValidationError(
                    ERROR_MESSAGES['missing_field'].format(index=index, field=field)
                )
            
            value = record[field]
            # Enhanced validation for empty/whitespace-only strings
            if value is None or (isinstance(value, str) and value.strip() == ""):
                raise ValidationError(
                    ERROR_MESSAGES['empty_field'].format(index=index, field=field)
                )
    
    def _validate_metric_id_uniqueness(self, record: Dict, index: int, metric_ids: Set[str]) -> None:
        """Validate metric ID uniqueness"""
        metric_id = record['metric_id'].strip()
        if metric_id in metric_ids:
            raise ValidationError(
                ERROR_MESSAGES['duplicate_metric'].format(index=index, metric_id=metric_id)
            )
        metric_ids.add(metric_id)
    
    def _validate_target_table_format(self, record: Dict, index: int) -> None:
        """Validate target table format"""
        target_table = record['target_table'].strip()
        if not target_table:
            raise ValidationError(
                ERROR_MESSAGES['empty_field'].format(index=index, field='target_table')
            )
        
        # Basic validation for BigQuery table format
        table_parts = target_table.split('.')
        if len(table_parts) != 3:
            raise ValidationError(
                ERROR_MESSAGES['invalid_table_format'].format(index=index, table=target_table)
            )
        
        # Check each part is not empty
        part_names = ['project', 'dataset', 'table']
        for part_idx, part in enumerate(table_parts):
            if not part.strip():
                raise ValidationError(
                    ERROR_MESSAGES['empty_table_part'].format(
                        index=index, 
                        table=target_table, 
                        part=part_names[part_idx]
                    )
                )
    
    def _validate_sql_placeholders(self, record: Dict, index: int) -> None:
        """Validate SQL placeholders"""
        sql_query = record['sql'].strip()
        if not sql_query:
            return
        
        # Check for valid placeholders
        currently_count = len(self.placeholder_patterns['currently'].findall(sql_query))
        partition_info_count = len(self.placeholder_patterns['partition_info'].findall(sql_query))
        
        if currently_count == 0 and partition_info_count == 0:
            self.log_warning(
                f"Record {index}: SQL query contains no date placeholders "
                f"({{currently}} or {{partition_info}})"
            )
        else:
            self.log_debug(
                f"Record {index}: Found {currently_count} {{currently}} and "
                f"{partition_info_count} {{partition_info}} placeholders in SQL"
            )
    
    def check_dependencies_exist(self, json_data: List[Dict], dependencies: List[str]) -> None:
        """
        Check if all specified dependencies exist in the JSON data
        
        Args:
            json_data: List of metric definitions
            dependencies: List of dependencies to check
            
        Raises:
            ValidationError: If any dependency is missing
        """
        available_dependencies = set(record['dependency'] for record in json_data)
        missing_dependencies = set(dependencies) - available_dependencies
        
        if missing_dependencies:
            raise ValidationError(
                f"Missing dependencies in JSON data: {missing_dependencies}. "
                f"Available dependencies: {available_dependencies}"
            )
        
        self.log_info(f"All dependencies found: {dependencies}")
    
    def filter_by_dependencies(self, json_data: List[Dict], dependencies: List[str]) -> List[Dict]:
        """
        Filter records by dependencies
        
        Args:
            json_data: List of metric definitions
            dependencies: List of dependencies to filter by
            
        Returns:
            Filtered list of records
        """
        return [
            record for record in json_data 
            if record['dependency'] in dependencies
        ]