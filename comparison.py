"""
Comparison engine for DQ validation logic
"""

from typing import Any, List, Dict, Tuple
import re


class ComparisonEngine:
    """Performs validation comparisons"""
    
    # Supported operators for numeric conditions
    NUMERIC_OPERATORS = [">=", "<=", "==", "!=", ">", "<"]
    
    @staticmethod
    def compare(actual_result: Any, expected_output: Any, 
                comparison_type: str) -> Tuple[str, str]:
        """
        Compare actual result against expected output
        
        Args:
            actual_result: Query result
            expected_output: Expected value from config
            comparison_type: Type of comparison
            
        Returns:
            Tuple of (validation_status, failure_reason)
            validation_status: "PASS" or "FAIL"
            failure_reason: Explanation if FAIL, empty if PASS
        """
        try:
            if comparison_type == "numeric_condition":
                return ComparisonEngine.compare_numeric_condition(actual_result, expected_output)
            elif comparison_type == "set_match":
                return ComparisonEngine.compare_set_match(actual_result, expected_output)
            elif comparison_type == "not_in_result":
                return ComparisonEngine.compare_not_in_result(actual_result, expected_output)
            elif comparison_type == "row_match":
                return ComparisonEngine.compare_row_match(actual_result, expected_output)
            else:
                return "FAIL", f"Unknown comparison_type: {comparison_type}"
        except Exception as e:
            return "FAIL", f"Comparison error: {str(e)}"
    
    @staticmethod
    def compare_numeric_condition(actual: Any, expected: str) -> Tuple[str, str]:
        """
        Compare numeric value against condition
        
        Args:
            actual: Actual numeric result
            expected: Expected condition (e.g., ">=10", "==0")
            
        Returns:
            Tuple of (validation_status, failure_reason)
        """
        # Parse the expected condition
        operator, expected_value = ComparisonEngine._parse_numeric_condition(expected)
        
        # Extract numeric value from actual result
        actual_value = ComparisonEngine._extract_numeric_value(actual)
        
        if actual_value is None:
            return "FAIL", f"Could not extract numeric value from actual result: {actual}"
        
        # Perform comparison based on operator
        result = False
        if operator == "==":
            result = actual_value == expected_value
        elif operator == "!=":
            result = actual_value != expected_value
        elif operator == "<":
            result = actual_value < expected_value
        elif operator == "<=":
            result = actual_value <= expected_value
        elif operator == ">":
            result = actual_value > expected_value
        elif operator == ">=":
            result = actual_value >= expected_value
        
        if result:
            return "PASS", ""
        else:
            return "FAIL", f"Expected {operator}{expected_value}, but got {actual_value}"
    
    @staticmethod
    def compare_set_match(actual: List, expected: List) -> Tuple[str, str]:
        """
        Compare result set against expected set
        
        Args:
            actual: Actual result set
            expected: Expected value set
            
        Returns:
            Tuple of (validation_status, failure_reason)
        """
        # Normalize actual result to list
        actual_list = ComparisonEngine._normalize_to_list(actual)
        
        # Convert to sets for order-independent comparison
        actual_set = set(ComparisonEngine._normalize_values(actual_list))
        expected_set = set(ComparisonEngine._normalize_values(expected))
        
        if actual_set == expected_set:
            return "PASS", ""
        else:
            # Identify differences
            missing = expected_set - actual_set
            extra = actual_set - expected_set
            
            failure_parts = []
            if missing:
                failure_parts.append(f"Missing values: {sorted(missing)}")
            if extra:
                failure_parts.append(f"Extra values: {sorted(extra)}")
            
            failure_reason = "; ".join(failure_parts)
            return "FAIL", failure_reason
    
    @staticmethod
    def compare_not_in_result(actual: List, expected: List) -> Tuple[str, str]:
        """
        Verify disallowed values not in result
        
        Args:
            actual: Actual result set
            expected: Disallowed values
            
        Returns:
            Tuple of (validation_status, failure_reason)
        """
        # Normalize actual result to list
        actual_list = ComparisonEngine._normalize_to_list(actual)
        
        # Convert to sets for efficient lookup
        actual_set = set(ComparisonEngine._normalize_values(actual_list))
        disallowed_set = set(ComparisonEngine._normalize_values(expected))
        
        # Find disallowed values that appear in actual
        found_disallowed = actual_set & disallowed_set
        
        if not found_disallowed:
            return "PASS", ""
        else:
            return "FAIL", f"Found disallowed values: {sorted(found_disallowed)}"
    
    @staticmethod
    def compare_row_match(actual: List[Dict], expected: List[Dict]) -> Tuple[str, str]:
        """
        Compare result rows against expected rows
        
        Args:
            actual: Actual result rows
            expected: Expected rows
            
        Returns:
            Tuple of (validation_status, failure_reason)
        """
        # Normalize actual result to list of dicts
        actual_rows = ComparisonEngine._normalize_to_row_list(actual)
        
        # Normalize rows for comparison (convert to comparable format)
        actual_normalized = [ComparisonEngine._normalize_row(row) for row in actual_rows]
        expected_normalized = [ComparisonEngine._normalize_row(row) for row in expected]
        
        # Convert to sets of tuples for order-independent comparison
        actual_set = set(actual_normalized)
        expected_set = set(expected_normalized)
        
        if actual_set == expected_set:
            return "PASS", ""
        else:
            # Identify differences
            missing_rows = expected_set - actual_set
            extra_rows = actual_set - expected_set
            
            failure_parts = []
            if missing_rows:
                missing_dicts = [ComparisonEngine._tuple_to_dict(row) for row in missing_rows]
                failure_parts.append(f"Missing rows: {missing_dicts}")
            if extra_rows:
                extra_dicts = [ComparisonEngine._tuple_to_dict(row) for row in extra_rows]
                failure_parts.append(f"Extra rows: {extra_dicts}")
            
            failure_reason = "; ".join(failure_parts)
            return "FAIL", failure_reason
    
    # Helper methods
    
    @staticmethod
    def _parse_numeric_condition(condition: str) -> Tuple[str, float]:
        """
        Parse numeric condition string to extract operator and value
        
        Args:
            condition: Condition string (e.g., ">=10", "0")
            
        Returns:
            Tuple of (operator, numeric_value)
        """
        condition = condition.strip()
        
        # Check for operators (order matters - check longer operators first)
        for op in ComparisonEngine.NUMERIC_OPERATORS:
            if condition.startswith(op):
                numeric_part = condition[len(op):].strip()
                try:
                    value = float(numeric_part)
                    return op, value
                except ValueError:
                    raise ValueError(f"Invalid numeric value in condition: {condition}")
        
        # No operator found, assume equality
        try:
            value = float(condition)
            return "==", value
        except ValueError:
            raise ValueError(f"Invalid numeric condition: {condition}")
    
    @staticmethod
    def _extract_numeric_value(result: Any) -> float:
        """
        Extract numeric value from query result
        
        Args:
            result: Query result (can be various formats)
            
        Returns:
            Numeric value or None if extraction fails
        """
        # Handle different result formats
        if isinstance(result, (int, float)):
            return float(result)
        
        if isinstance(result, str):
            try:
                return float(result)
            except ValueError:
                return None
        
        # Handle list of rows (extract first row, first column)
        if isinstance(result, list) and len(result) > 0:
            first_row = result[0]
            
            # If row is a dict, get first value
            if isinstance(first_row, dict):
                if len(first_row) > 0:
                    first_value = list(first_row.values())[0]
                    return ComparisonEngine._extract_numeric_value(first_value)
            
            # If row is a list/tuple, get first element
            elif isinstance(first_row, (list, tuple)) and len(first_row) > 0:
                return ComparisonEngine._extract_numeric_value(first_row[0])
            
            # If row is a primitive, use it directly
            else:
                return ComparisonEngine._extract_numeric_value(first_row)
        
        return None
    
    @staticmethod
    def _normalize_to_list(result: Any) -> List:
        """
        Normalize query result to a list of values
        
        Args:
            result: Query result
            
        Returns:
            List of values
        """
        if isinstance(result, list):
            # If list of dicts, extract first column values
            if len(result) > 0 and isinstance(result[0], dict):
                values = []
                for row in result:
                    if len(row) > 0:
                        values.append(list(row.values())[0])
                return values
            # If list of lists/tuples, extract first element
            elif len(result) > 0 and isinstance(result[0], (list, tuple)):
                return [row[0] if len(row) > 0 else None for row in result]
            # Already a list of primitives
            else:
                return result
        
        # Single value
        return [result]
    
    @staticmethod
    def _normalize_values(values: List) -> List:
        """
        Normalize values for comparison (handle type conversions)
        
        Args:
            values: List of values
            
        Returns:
            Normalized list
        """
        normalized = []
        for val in values:
            # Convert to string for consistent comparison
            if val is None:
                normalized.append(None)
            elif isinstance(val, (int, float)):
                # Keep numeric types as-is for proper comparison
                normalized.append(val)
            else:
                normalized.append(str(val))
        return normalized
    
    @staticmethod
    def _normalize_to_row_list(result: Any) -> List[Dict]:
        """
        Normalize query result to list of dictionaries
        
        Args:
            result: Query result
            
        Returns:
            List of dictionaries
        """
        if isinstance(result, list):
            # Already list of dicts
            if len(result) > 0 and isinstance(result[0], dict):
                return result
            # Convert list of lists/tuples to dicts (not ideal but handle it)
            elif len(result) > 0 and isinstance(result[0], (list, tuple)):
                return [{"col_" + str(i): val for i, val in enumerate(row)} for row in result]
        
        # Single dict
        if isinstance(result, dict):
            return [result]
        
        return []
    
    @staticmethod
    def _normalize_row(row: Dict) -> Tuple:
        """
        Normalize a row dictionary to a comparable tuple
        
        Args:
            row: Row dictionary
            
        Returns:
            Tuple of sorted key-value pairs
        """
        # Sort by keys and convert values to comparable types
        items = []
        for key in sorted(row.keys()):
            val = row[key]
            # Normalize value types
            if isinstance(val, float):
                # Handle floating point comparison
                val = round(val, 10)
            items.append((key, val))
        return tuple(items)
    
    @staticmethod
    def _tuple_to_dict(row_tuple: Tuple) -> Dict:
        """
        Convert normalized row tuple back to dictionary
        
        Args:
            row_tuple: Tuple of key-value pairs
            
        Returns:
            Dictionary
        """
        return dict(row_tuple)
