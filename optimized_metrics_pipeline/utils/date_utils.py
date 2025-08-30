"""Date utilities for the metrics pipeline"""

from datetime import datetime, timedelta
from typing import Optional
from .exceptions import ValidationError
from .logging import get_logger

logger = get_logger(__name__)


def validate_date_format(date_str: str, date_format: str = '%Y-%m-%d') -> None:
    """
    Validate date format
    
    Args:
        date_str: Date string to validate
        date_format: Expected date format
        
    Raises:
        ValidationError: If date format is invalid
    """
    try:
        datetime.strptime(date_str, date_format)
    except ValueError:
        raise ValidationError(
            f"Invalid date format: {date_str}. Expected {date_format}"
        )


def get_current_partition_dt() -> str:
    """
    Get current partition date in YYYY-MM-DD format
    
    Returns:
        Current date as string
    """
    return datetime.now().strftime('%Y-%m-%d')


def format_date_for_sql(date_obj: datetime) -> str:
    """
    Format datetime object for SQL usage
    
    Args:
        date_obj: Datetime object
        
    Returns:
        Formatted date string for SQL
    """
    if isinstance(date_obj, datetime):
        return date_obj.strftime('%Y-%m-%d')
    return str(date_obj)


def parse_date_string(date_str: str, date_format: str = '%Y-%m-%d') -> Optional[datetime]:
    """
    Parse date string to datetime object
    
    Args:
        date_str: Date string to parse
        date_format: Date format to use
        
    Returns:
        Datetime object or None if parsing fails
    """
    try:
        return datetime.strptime(date_str, date_format)
    except ValueError:
        logger.warning(f"Could not parse date string: {date_str} with format {date_format}")
        return None


def get_date_range(start_date: str, end_date: str, 
                   date_format: str = '%Y-%m-%d') -> list:
    """
    Get list of dates between start and end date
    
    Args:
        start_date: Start date string
        end_date: End date string
        date_format: Date format
        
    Returns:
        List of date strings
    """
    try:
        start_dt = datetime.strptime(start_date, date_format)
        end_dt = datetime.strptime(end_date, date_format)
        
        dates = []
        current_date = start_dt
        
        while current_date <= end_dt:
            dates.append(current_date.strftime(date_format))
            current_date += timedelta(days=1)
        
        return dates
        
    except ValueError as e:
        logger.error(f"Error generating date range: {e}")
        return []


def get_current_timestamp() -> datetime:
    """
    Get current UTC timestamp
    
    Returns:
        Current UTC datetime
    """
    return datetime.utcnow()


def format_timestamp_for_bq(timestamp: datetime) -> str:
    """
    Format timestamp for BigQuery
    
    Args:
        timestamp: Datetime object
        
    Returns:
        Formatted timestamp string
    """
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')


def get_year_from_date(date_str: str, date_format: str = '%Y-%m-%d') -> Optional[int]:
    """
    Extract year from date string
    
    Args:
        date_str: Date string
        date_format: Date format
        
    Returns:
        Year as integer or None
    """
    try:
        date_obj = datetime.strptime(date_str, date_format)
        return date_obj.year
    except ValueError:
        logger.warning(f"Could not extract year from date: {date_str}")
        return None