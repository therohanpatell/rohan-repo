"""Numeric utilities for the metrics pipeline"""

from decimal import Decimal
from typing import Union, Optional
from .logging import get_logger
from .exceptions import ValidationError
from ..config.constants import DEFAULT_VALUES

logger = get_logger(__name__)


def normalize_numeric_value(value: Union[int, float, Decimal, str, None]) -> Optional[str]:
    """
    Normalize numeric values to string representation to preserve precision
    
    Args:
        value: Numeric value of any type
        
    Returns:
        String representation of the number or None
    """
    if value is None:
        return None
    
    try:
        # Handle different numeric types with precision preservation
        if isinstance(value, Decimal):
            # Keep as string to preserve precision
            return str(value)
        elif isinstance(value, (int, float)):
            # Convert to Decimal first to handle large numbers properly
            decimal_val = Decimal(str(value))
            return str(decimal_val)
        elif isinstance(value, str):
            # Try to parse as Decimal to validate it's a valid number
            try:
                decimal_val = Decimal(value)
                return str(decimal_val)
            except:
                logger.warning(f"Could not parse string as number: {value}")
                return None
        else:
            # Try to convert to string and then to Decimal
            decimal_val = Decimal(str(value))
            return str(decimal_val)
            
    except (ValueError, TypeError, OverflowError, Exception) as e:
        logger.warning(f"Could not normalize numeric value: {value}, error: {e}")
        return None


def safe_decimal_conversion(value: Optional[str]) -> Optional[Decimal]:
    """
    Safely convert string to Decimal for BigQuery
    
    Args:
        value: String representation of number
        
    Returns:
        Decimal value or None
    """
    if value is None:
        return None
    
    try:
        return Decimal(value)
    except (ValueError, TypeError, OverflowError):
        logger.warning(f"Could not convert to Decimal: {value}")
        return None


def validate_denominator(denominator_value: Optional[str], metric_id: str, 
                        allow_negative: bool = False, 
                        log_small_values: bool = True) -> None:
    """
    Validate denominator value for metric calculations
    
    Args:
        denominator_value: Denominator value as string
        metric_id: Metric ID for error reporting
        allow_negative: Whether to allow negative denominators
        log_small_values: Whether to log warnings for small values
        
    Raises:
        ValidationError: If denominator is invalid
    """
    if denominator_value is None:
        return
    
    try:
        denominator_decimal = safe_decimal_conversion(denominator_value)
        if denominator_decimal is None:
            logger.warning(f"Could not validate denominator_value: {denominator_value}")
            return
        
        # Check for zero denominator
        if denominator_decimal == 0:
            raise ValidationError(
                f"Metric '{metric_id}': Invalid denominator value: denominator_value is 0. "
                f"Cannot calculate metrics with zero denominator."
            )
        
        # Check for negative denominator
        if not allow_negative and denominator_decimal < 0:
            raise ValidationError(
                f"Metric '{metric_id}': Invalid denominator value: denominator_value is negative "
                f"({denominator_decimal}). Negative denominators are not allowed."
            )
        
        # Log warning for very small positive denominators
        if log_small_values and abs(denominator_decimal) < Decimal(DEFAULT_VALUES['precision_threshold']):
            logger.warning(
                f"Metric '{metric_id}': Very small denominator value detected: {denominator_decimal}. "
                f"This may cause precision issues."
            )
            
    except (ValueError, TypeError):
        logger.warning(f"Could not validate denominator_value: {denominator_value}")


def format_numeric_for_display(value: Union[Decimal, float, int, str, None]) -> str:
    """
    Format numeric value for display purposes
    
    Args:
        value: Numeric value
        
    Returns:
        Formatted string representation
    """
    if value is None:
        return "NULL"
    
    try:
        if isinstance(value, Decimal):
            return str(value)
        elif isinstance(value, (int, float)):
            return str(Decimal(str(value)))
        elif isinstance(value, str):
            # Try to parse and format as decimal
            try:
                return str(Decimal(value))
            except:
                return value
        else:
            return str(value)
    except Exception:
        return str(value)