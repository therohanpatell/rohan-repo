"""
Utility functions and helpers for the Metrics Pipeline
"""

import re
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal
from contextlib import contextmanager
from pyspark.sql import SparkSession
from config import PipelineConfig, setup_logging
from exceptions import MetricsPipelineError, ValidationError

logger = setup_logging()


class DateUtils:
    """Date utility functions"""
    
    @staticmethod
    def validate_date_format(date_str: str) -> None:
        """Validate date format YYYY-MM-DD"""
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            raise ValidationError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")
    
    @staticmethod
    def get_current_partition_dt() -> str:
        """Get current date as partition_dt"""
        return datetime.now().strftime('%Y-%m-%d')
    
    @staticmethod
    def get_current_timestamp() -> datetime:
        """Get current UTC timestamp"""
        return datetime.utcnow()


class NumericUtils:
    """Numeric value handling utilities"""
    
    @staticmethod
    def normalize_numeric_value(value: Union[int, float, Decimal, None]) -> Optional[str]:
        """
        Normalize numeric values to string representation to preserve precision
        
        Args:
            value: Numeric value of any type
            
        Returns:
            String representation of the number or None
            
        Raises:
            MetricsPipelineError: If value is too large to be represented as a Decimal.
        """
        if value is None:
            return None
        
        try:
            if isinstance(value, Decimal):
                return str(value)
            elif isinstance(value, (int, float)):
                decimal_val = Decimal(str(value))
                return str(decimal_val)
            elif isinstance(value, str):
                try:
                    decimal_val = Decimal(value)
                    return str(decimal_val)
                except:
                    logger.warning(f"Could not parse string as number: {value}")
                    return None
            else:
                decimal_val = Decimal(str(value))
                return str(decimal_val)
                
        except OverflowError:
            raise MetricsPipelineError(f"Numeric value {value} is out of range for Decimal type.")
        except (ValueError, TypeError, Exception) as e:
            logger.warning(f"Could not normalize numeric value: {value}, error: {e}")
            return None
    
    @staticmethod
    def safe_decimal_conversion(value: Optional[str]) -> Optional[Decimal]:
        """
        Safely convert string to Decimal for BigQuery
        
        Args:
            value: String representation of number
            
        Returns:
            Decimal value or None
            
        Raises:
            MetricsPipelineError: If value is too large to be represented as a Decimal.
        """
        if value is None:
            return None
        
        try:
            return Decimal(value)
        except OverflowError:
            raise MetricsPipelineError(f"Numeric value {value} is out of range for Decimal type.")
        except (ValueError, TypeError):
            logger.warning(f"Could not convert to Decimal: {value}")
            return None
    
    @staticmethod
    def validate_denominator(denominator_str: Optional[str], metric_id: Optional[str] = None) -> None:
        """
        Validate denominator value
        
        Args:
            denominator_str: String representation of denominator
            metric_id: Optional metric ID for error reporting
            
        Raises:
            MetricsPipelineError: If denominator is invalid
        """
        if denominator_str is not None:
            try:
                denominator_decimal = NumericUtils.safe_decimal_conversion(denominator_str)
                if denominator_decimal is not None:
                    if denominator_decimal == 0:
                        error_msg = "Invalid denominator value: denominator_value is 0. Cannot calculate metrics with zero denominator."
                        raise MetricsPipelineError(error_msg, metric_id)
                    elif denominator_decimal < 0:
                        error_msg = f"Invalid denominator value: denominator_value is negative ({denominator_decimal}). Negative denominators are not allowed."
                        raise MetricsPipelineError(error_msg, metric_id)
            except (ValueError, TypeError):
                logger.warning(f"Could not validate denominator_value: {denominator_str}")


class SQLUtils:
    """SQL processing utilities"""
    
    @staticmethod
    def find_placeholder_positions(sql: str) -> List[Tuple[str, int, int]]:
        """
        Find all {currently} and {partition_info} placeholders in SQL with their positions
        
        Args:
            sql: SQL query string
            
        Returns:
            List of tuples (placeholder_type, start_pos, end_pos)
        """
        placeholders = []
        
        # Find {currently} placeholders
        currently_pattern = r'\{currently\}'
        for match in re.finditer(currently_pattern, sql):
            placeholders.append(('currently', match.start(), match.end()))
        
        # Find {partition_info} placeholders
        partition_info_pattern = r'\{partition_info\}'
        for match in re.finditer(partition_info_pattern, sql):
            placeholders.append(('partition_info', match.start(), match.end()))
        
        # Sort by position for consistent replacement
        placeholders.sort(key=lambda x: x[1])
        
        return placeholders
    
    @staticmethod
    def get_table_for_placeholder(sql: str, placeholder_pos: int) -> Optional[Tuple[str, str]]:
        """
        Find the table associated with a placeholder based on its position in the SQL
        
        Args:
            sql: SQL query string
            placeholder_pos: Position of the placeholder in the SQL
            
        Returns:
            Tuple (dataset, table_name) or None if not found
        """
        table_pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
        
        best_table = None
        best_distance = float('inf')
        
        for match in re.finditer(table_pattern, sql):
            table_end_pos = match.end()
            
            if table_end_pos < placeholder_pos:
                distance = placeholder_pos - table_end_pos
                if distance < best_distance:
                    best_distance = distance
                    project, dataset, table = match.groups()
                    best_table = (dataset, table)
        
        return best_table
    
    @staticmethod
    def get_source_table_info(sql: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract source table dataset and table name from SQL query
        
        Args:
            sql: SQL query string
            
        Returns:
            Tuple of (dataset_name, table_name) or (None, None) if not found
        """
        try:
            table_pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
            matches = re.findall(table_pattern, sql)
            
            if matches:
                project, dataset, table = matches[0]
                logger.debug(f"Extracted source table info: dataset={dataset}, table={table}")
                return dataset, table
            else:
                logger.warning("No source table found in SQL query")
                return None, None
                
        except Exception as e:
            logger.error(f"Failed to extract source table info: {str(e)}")
            return None, None


class ValidationUtils:
    """Data validation utilities"""
    
    @staticmethod
    def validate_json_record(record: Dict, index: int, existing_metric_ids: set) -> None:
        """
        Validate a single JSON record
        
        Args:
            record: JSON record to validate
            index: Record index for error reporting
            existing_metric_ids: Set of already seen metric IDs
            
        Raises:
            ValidationError: If validation fails
        """
        # Check for required fields
        for field in PipelineConfig.REQUIRED_JSON_FIELDS:
            if field not in record:
                raise ValidationError(f"Record {index}: Missing required field '{field}'")
            
            value = record[field]
            if value is None or (isinstance(value, str) and value.strip() == ""):
                raise ValidationError(f"Record {index}: Field '{field}' is null, empty, or contains only whitespace")
        
        # Check for duplicate metric IDs
        metric_id = record['metric_id'].strip()
        if metric_id in existing_metric_ids:
            raise ValidationError(f"Record {index}: Duplicate metric_id '{metric_id}' found")
        existing_metric_ids.add(metric_id)
        
        # Validate target_table format
        ValidationUtils._validate_target_table(record['target_table'], index)
        
        # Validate SQL placeholders
        ValidationUtils._validate_sql_placeholders(record['sql'], index)
    
    @staticmethod
    def _validate_target_table(target_table: str, index: int) -> None:
        """Validate target table format"""
        target_table = target_table.strip()
        if not target_table:
            raise ValidationError(f"Record {index}: target_table cannot be empty")
        
        table_parts = target_table.split('.')
        if len(table_parts) != 3:
            raise ValidationError(f"Record {index}: target_table '{target_table}' must be in format 'project.dataset.table'")
        
        for part_idx, part in enumerate(table_parts):
            if not part.strip():
                part_names = ['project', 'dataset', 'table']
                raise ValidationError(f"Record {index}: target_table '{target_table}' has empty {part_names[part_idx]} part")
    
    @staticmethod
    def _validate_sql_placeholders(sql: str, index: int) -> None:
        """Validate SQL placeholders"""
        sql_query = sql.strip()
        if sql_query:
            currently_count = len(re.findall(r'\{currently\}', sql_query))
            partition_info_count = len(re.findall(r'\{partition_info\}', sql_query))
            
            if currently_count == 0 and partition_info_count == 0:
                logger.warning(f"Record {index}: SQL query contains no date placeholders ({{currently}} or {{partition_info}})")
            else:
                logger.debug(f"Record {index}: Found {currently_count} {{currently}} and {partition_info_count} {{partition_info}} placeholders in SQL")


class StringUtils:
    """String processing utilities"""
    
    @staticmethod
    def clean_error_message(error_message: str) -> str:
        """
        Clean and limit error message length
        
        Args:
            error_message: Raw error message
            
        Returns:
            Cleaned error message
        """
        clean_error = error_message.replace('\n', ' ').replace('\r', ' ').strip()
        if len(clean_error) > PipelineConfig.MAX_ERROR_MESSAGE_LENGTH:
            clean_error = clean_error[:PipelineConfig.MAX_ERROR_MESSAGE_LENGTH - 3] + '...'
        return clean_error
    
    @staticmethod
    def format_error_with_category(error_message: str, error_category: str) -> str:
        """
        Format error message with category for better debugging
        
        Args:
            error_message: Raw error message
            error_category: Error category code
            
        Returns:
            Formatted error message with category
        """
        clean_error = StringUtils.clean_error_message(error_message)
        category_description = PipelineConfig.ERROR_CATEGORIES.get(error_category, error_category)
        return f"[{error_category}] {category_description}: {clean_error}"
    
    @staticmethod
    def escape_sql_string(value: str) -> str:
        """Escape single quotes in SQL strings"""
        return value.replace("'", "''")


class ExecutionUtils:
    """Execution tracking utilities"""
    
    @staticmethod
    def generate_execution_id() -> str:
        """Generate unique execution ID"""
        return str(uuid.uuid4())


@contextmanager
def managed_spark_session(app_name: str = "MetricsPipeline"):
    """
    Context manager for Spark session with proper cleanup
    
    Args:
        app_name: Spark application name
        
    Yields:
        SparkSession instance
    """
    spark = None
    try:
        builder = SparkSession.builder.appName(app_name)
        
        # Apply configurations
        for key, value in PipelineConfig.SPARK_CONFIGS.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        logger.info(f"Spark session created successfully: {app_name}")
        yield spark
        
    except Exception as e:
        logger.error(f"Error in Spark session: {str(e)}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")