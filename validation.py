"""
Validation module for DQ configuration
"""

from typing import List, Dict, Any
from exceptions import ValidationError


class ValidationEngine:
    """Validates DQ configuration and rules"""
    
    # Valid values for validation
    VALID_SEVERITIES = ["High", "Medium", "Low"]
    VALID_COMPARISON_TYPES = ["numeric_condition", "set_match", "not_in_result", "row_match"]
    REQUIRED_FIELDS = [
        "check_id", "category", "sql_query", "description", 
        "severity", "expected_output", "comparison_type", "active"
    ]
    
    @staticmethod
    def validate_dq_json(json_data: List[Dict]) -> List[Dict]:
        """
        Validate DQ JSON configuration
        
        Args:
            json_data: List of DQ check configurations
            
        Returns:
            Validated DQ check configurations
            
        Raises:
            ValidationError: If validation fails
        """
        if not isinstance(json_data, list):
            raise ValidationError("DQ configuration must be a list of check configurations")
        
        if len(json_data) == 0:
            raise ValidationError("DQ configuration cannot be empty")
        
        # Validate each record
        for index, record in enumerate(json_data):
            ValidationEngine.validate_dq_record(record, index)
        
        return json_data
    
    @staticmethod
    def validate_dq_record(record: Dict, index: int) -> None:
        """
        Validate a single DQ record
        
        Args:
            record: DQ check configuration
            index: Record index for error reporting
            
        Raises:
            ValidationError: If validation fails
        """
        if not isinstance(record, dict):
            raise ValidationError(f"Record at index {index} must be a dictionary")
        
        # Validate required fields
        missing_fields = []
        for field in ValidationEngine.REQUIRED_FIELDS:
            if field not in record:
                missing_fields.append(field)
        
        if missing_fields:
            raise ValidationError(
                f"Record at index {index} missing required fields: {', '.join(missing_fields)}"
            )
        
        # Validate severity
        ValidationEngine.validate_severity(record["severity"])
        
        # Validate comparison_type and expected_output compatibility
        ValidationEngine.validate_comparison_type(
            record["comparison_type"], 
            record["expected_output"]
        )
        
        # Validate active flag is boolean
        if not isinstance(record["active"], bool):
            raise ValidationError(
                f"Record at index {index}: 'active' field must be a boolean, got {type(record['active']).__name__}"
            )
        
        # Validate check_id is non-empty string
        if not isinstance(record["check_id"], str) or not record["check_id"].strip():
            raise ValidationError(
                f"Record at index {index}: 'check_id' must be a non-empty string"
            )
        
        # Validate sql_query is non-empty string
        if not isinstance(record["sql_query"], str) or not record["sql_query"].strip():
            raise ValidationError(
                f"Record at index {index}: 'sql_query' must be a non-empty string"
            )
    
    @staticmethod
    def validate_comparison_type(comparison_type: str, expected_output: Any) -> None:
        """
        Validate comparison_type and expected_output compatibility
        
        Args:
            comparison_type: Type of comparison
            expected_output: Expected result value
            
        Raises:
            ValidationError: If incompatible
        """
        # Validate comparison_type is valid
        if comparison_type not in ValidationEngine.VALID_COMPARISON_TYPES:
            raise ValidationError(
                f"Invalid comparison_type '{comparison_type}'. "
                f"Must be one of: {', '.join(ValidationEngine.VALID_COMPARISON_TYPES)}"
            )
        
        # Validate expected_output format based on comparison_type
        if comparison_type == "numeric_condition":
            # Expected output should be a string representing a number or condition
            if not isinstance(expected_output, str):
                raise ValidationError(
                    f"For comparison_type 'numeric_condition', expected_output must be a string, "
                    f"got {type(expected_output).__name__}"
                )
            
            # Validate it's either a number or a condition with operator
            expected_output_stripped = expected_output.strip()
            if not expected_output_stripped:
                raise ValidationError(
                    "For comparison_type 'numeric_condition', expected_output cannot be empty"
                )
            
            # Check if it starts with an operator
            operators = [">=", "<=", "==", "!=", ">", "<"]
            has_operator = any(expected_output_stripped.startswith(op) for op in operators)
            
            if has_operator:
                # Extract the numeric part after the operator
                for op in operators:
                    if expected_output_stripped.startswith(op):
                        numeric_part = expected_output_stripped[len(op):].strip()
                        try:
                            float(numeric_part)
                        except ValueError:
                            raise ValidationError(
                                f"For comparison_type 'numeric_condition', expected_output '{expected_output}' "
                                f"has invalid numeric value after operator"
                            )
                        break
            else:
                # Should be a plain number
                try:
                    float(expected_output_stripped)
                except ValueError:
                    raise ValidationError(
                        f"For comparison_type 'numeric_condition', expected_output '{expected_output}' "
                        f"must be a valid number or condition (e.g., '0', '>=10')"
                    )
        
        elif comparison_type in ["set_match", "not_in_result"]:
            # Expected output should be a list
            if not isinstance(expected_output, list):
                raise ValidationError(
                    f"For comparison_type '{comparison_type}', expected_output must be a list, "
                    f"got {type(expected_output).__name__}"
                )
            
            if len(expected_output) == 0:
                raise ValidationError(
                    f"For comparison_type '{comparison_type}', expected_output list cannot be empty"
                )
        
        elif comparison_type == "row_match":
            # Expected output should be a list of dictionaries
            if not isinstance(expected_output, list):
                raise ValidationError(
                    f"For comparison_type 'row_match', expected_output must be a list, "
                    f"got {type(expected_output).__name__}"
                )
            
            if len(expected_output) == 0:
                raise ValidationError(
                    "For comparison_type 'row_match', expected_output list cannot be empty"
                )
            
            # Validate each item is a dictionary
            for idx, item in enumerate(expected_output):
                if not isinstance(item, dict):
                    raise ValidationError(
                        f"For comparison_type 'row_match', expected_output must be a list of objects. "
                        f"Item at index {idx} is {type(item).__name__}, not a dictionary"
                    )
    
    @staticmethod
    def validate_severity(severity: str) -> None:
        """
        Validate severity value
        
        Args:
            severity: Severity level
            
        Raises:
            ValidationError: If invalid severity
        """
        if not isinstance(severity, str):
            raise ValidationError(
                f"Severity must be a string, got {type(severity).__name__}"
            )
        
        if severity not in ValidationEngine.VALID_SEVERITIES:
            raise ValidationError(
                f"Invalid severity '{severity}'. Must be one of: {', '.join(ValidationEngine.VALID_SEVERITIES)}"
            )
