import argparse
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any, Callable
import sys
from decimal import Decimal
import os
import tempfile
from contextlib import contextmanager
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, 
    when, isnan, isnull, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    TimestampType, DecimalType, IntegerType, DoubleType
)

from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    pass

class MetricsPipeline:
    """Main pipeline class for processing metrics"""
    
    # Class constants
    REQUIRED_FIELDS = ['metric_id', 'metric_name', 'metric_type', 'sql', 'dependency', 'target_table']
    NUMERIC_FIELDS = ['metric_output', 'numerator_value', 'denominator_value']
    PLACEHOLDER_PATTERNS = {
        'currently': r'\{currently\}',
        'partition_info': r'\{partition_info\}'
    }
    
    # Quote handling patterns
    QUOTE_PATTERNS = {
        'double_quoted_strings': r'"([^"]*)"',
        'single_quoted_strings': r"'([^']*)'",
        'double_quoted_identifiers': r'`([^`]*)`',
        'table_references': r'`([^.]+)\.([^.]+)\.([^`]+)`'
    }
    
    def __init__(self, spark: SparkSession, bq_client: bigquery.Client):
        self.spark = spark
        self.bq_client = bq_client
        self.execution_id = str(uuid.uuid4())
        self.processed_metrics = []
        self.overwritten_metrics = []
        self.target_tables = set()
        self.quote_conversions = []  # Track quote conversions for debugging
        
    def _handle_operation(self, operation: Callable, operation_name: str, *args, **kwargs) -> Any:
        """Generic error handling wrapper for operations"""
        logger.debug(f"Starting operation: {operation_name}")
        start_time = datetime.utcnow()
        
        try:
            result = operation(*args, **kwargs)
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Operation '{operation_name}' completed successfully in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"{operation_name} failed after {execution_time:.2f} seconds: {str(e)}"
            logger.error(error_msg)
            raise MetricsPipelineError(error_msg)
    
    def _detect_quote_issues(self, sql: str, metric_id: Optional[str] = None) -> Dict[str, Any]:
        """Detect potential quote issues in SQL query"""
        prefix = f"Metric '{metric_id}': " if metric_id else ""
        logger.debug(f"{prefix}Starting quote issue detection for SQL query")
        
        issues = {
            'has_double_quotes': False,
            'has_single_quotes': False,
            'has_backticks': False,
            'double_quote_count': 0,
            'single_quote_count': 0,
            'backtick_count': 0,
            'potential_string_literals': [],
            'potential_identifiers': []
        }
        
        # Count different quote types
        issues['double_quote_count'] = sql.count('"')
        issues['single_quote_count'] = sql.count("'")
        issues['backtick_count'] = sql.count('`')
        
        issues['has_double_quotes'] = issues['double_quote_count'] > 0
        issues['has_single_quotes'] = issues['single_quote_count'] > 0
        issues['has_backticks'] = issues['backtick_count'] > 0
        
        logger.debug(f"{prefix}Quote analysis - Double: {issues['double_quote_count']}, Single: {issues['single_quote_count']}, Backticks: {issues['backtick_count']}")
        
        # Find potential string literals in double quotes
        double_quoted_matches = re.findall(self.QUOTE_PATTERNS['double_quoted_strings'], sql)
        issues['potential_string_literals'] = double_quoted_matches
        
        # Find potential identifiers in backticks
        backtick_matches = re.findall(self.QUOTE_PATTERNS['double_quoted_identifiers'], sql)
        issues['potential_identifiers'] = backtick_matches
        
        # Log findings
        if issues['has_double_quotes']:
            logger.info(f"{prefix}Found {issues['double_quote_count']} double quotes in SQL")
            if issues['potential_string_literals']:
                logger.info(f"{prefix}Potential string literals: {issues['potential_string_literals']}")
        
        if issues['has_backticks']:
            logger.debug(f"{prefix}Found {issues['backtick_count']} backticks for identifiers")
            if issues['potential_identifiers']:
                logger.debug(f"{prefix}Potential identifiers: {issues['potential_identifiers']}")
        
        logger.debug(f"{prefix}Quote issue detection completed")
        return issues
    
    def _normalize_quotes_in_sql(self, sql: str, metric_id: Optional[str] = None) -> str:
        """Normalize quotes in SQL query - convert double quotes to single quotes for string literals"""
        prefix = f"Metric '{metric_id}': " if metric_id else ""
        
        if not sql or not sql.strip():
            logger.debug(f"{prefix}SQL query is empty or whitespace only, skipping normalization")
            return sql
        
        logger.debug(f"{prefix}Starting quote normalization process")
        
        # Detect quote issues first
        quote_issues = self._detect_quote_issues(sql, metric_id)
        
        # If no double quotes, return as is
        if not quote_issues['has_double_quotes']:
            logger.debug(f"{prefix}No double quotes found, skipping normalization")
            return sql
        
        logger.info(f"{prefix}Normalizing quotes in SQL query")
        
        # Store original for comparison
        original_sql = sql
        normalized_sql = sql
        
        # Strategy: Replace double quotes with single quotes, but preserve backticks for identifiers
        # This is a simplified approach - for production, you might want more sophisticated parsing
        
        # First, protect table references (backticks) by temporarily replacing them
        table_references = re.findall(self.QUOTE_PATTERNS['table_references'], normalized_sql)
        temp_replacements = {}
        
        logger.debug(f"{prefix}Found {len(table_references)} table references to protect")
        for i, (project, dataset, table) in enumerate(table_references):
            temp_key = f"__TABLE_REF_{i}__"
            original_ref = f"`{project}.{dataset}.{table}`"
            temp_replacements[temp_key] = original_ref
            normalized_sql = normalized_sql.replace(original_ref, temp_key)
            logger.debug(f"{prefix}Protected table reference {i+1}: {original_ref} -> {temp_key}")
        
        # Now replace double quotes with single quotes for string literals
        # This regex finds double-quoted strings that are not part of identifiers
        def replace_double_quotes(match):
            content = match.group(1)
            # Escape single quotes within the string content
            escaped_content = content.replace("'", "''")
            return f"'{escaped_content}'"
        
        # Replace double-quoted strings with single-quoted strings
        before_replacement = normalized_sql
        normalized_sql = re.sub(self.QUOTE_PATTERNS['double_quoted_strings'], replace_double_quotes, normalized_sql)
        
        if before_replacement != normalized_sql:
            logger.debug(f"{prefix}Replaced double-quoted strings with single-quoted strings")
        
        # Restore table references
        for temp_key, original_ref in temp_replacements.items():
            normalized_sql = normalized_sql.replace(temp_key, original_ref)
            logger.debug(f"{prefix}Restored table reference: {temp_key} -> {original_ref}")
        
        # Track the conversion for debugging
        if normalized_sql != original_sql:
            conversion_info = {
                'metric_id': metric_id,
                'original_sql': original_sql,
                'normalized_sql': normalized_sql,
                'quote_issues': quote_issues
            }
            self.quote_conversions.append(conversion_info)
            logger.info(f"{prefix}Quote normalization completed successfully")
            logger.debug(f"{prefix}Original SQL length: {len(original_sql)}")
            logger.debug(f"{prefix}Normalized SQL length: {len(normalized_sql)}")
            logger.debug(f"{prefix}Original: {original_sql}")
            logger.debug(f"{prefix}Normalized: {normalized_sql}")
        else:
            logger.debug(f"{prefix}No changes made during quote normalization")
        
        return normalized_sql
    
    def _validate_sql_syntax(self, sql: str, metric_id: Optional[str] = None) -> bool:
        """Basic SQL syntax validation after quote normalization"""
        prefix = f"Metric '{metric_id}': " if metric_id else ""
        
        if not sql or not sql.strip():
            logger.warning(f"{prefix}SQL query is empty or contains only whitespace")
            return False
        
        logger.debug(f"{prefix}Starting SQL syntax validation")
        
        # Basic checks
        sql_lower = sql.lower().strip()
        
        # Check for basic SQL structure
        expected_keywords = ['select', 'with', '(']
        if not any(sql_lower.startswith(keyword) for keyword in expected_keywords):
            logger.warning(f"{prefix}SQL query doesn't start with expected keywords: {expected_keywords}")
            logger.debug(f"{prefix}SQL starts with: {sql_lower[:50]}...")
            return False
        
        logger.debug(f"{prefix}SQL starts with valid keyword")
        
        # Check for balanced quotes
        single_quote_count = sql.count("'")
        double_quote_count = sql.count('"')
        backtick_count = sql.count('`')
        
        logger.debug(f"{prefix}Quote balance check - Single: {single_quote_count}, Double: {double_quote_count}, Backticks: {backtick_count}")
        
        if single_quote_count % 2 != 0:
            logger.warning(f"{prefix}Unbalanced single quotes in SQL (count: {single_quote_count})")
            return False
        
        if double_quote_count % 2 != 0:
            logger.warning(f"{prefix}Unbalanced double quotes in SQL (count: {double_quote_count})")
            return False
        
        if backtick_count % 2 != 0:
            logger.warning(f"{prefix}Unbalanced backticks in SQL (count: {backtick_count})")
            return False
        
        logger.debug(f"{prefix}SQL syntax validation passed")
        return True
    
    def _preprocess_sql_query(self, sql: str, metric_id: Optional[str] = None) -> str:
        """Preprocess SQL query to handle quotes and other issues"""
        prefix = f"Metric '{metric_id}': " if metric_id else ""
        
        if not sql or not sql.strip():
            raise MetricsPipelineError(f"{prefix}SQL query is empty")
        
        logger.info(f"{prefix}Starting SQL preprocessing")
        logger.debug(f"{prefix}Original SQL length: {len(sql)} characters")
        
        # Step 1: Normalize quotes
        logger.debug(f"{prefix}Step 1: Normalizing quotes")
        normalized_sql = self._normalize_quotes_in_sql(sql, metric_id)
        logger.debug(f"{prefix}Quote normalization completed, SQL length: {len(normalized_sql)} characters")
        
        # Step 2: Validate syntax
        logger.debug(f"{prefix}Step 2: Validating SQL syntax")
        syntax_valid = self._validate_sql_syntax(normalized_sql, metric_id)
        if not syntax_valid:
            logger.warning(f"{prefix}SQL syntax validation failed, proceeding with caution")
        else:
            logger.debug(f"{prefix}SQL syntax validation passed")
        
        # Step 3: Clean up extra whitespace
        logger.debug(f"{prefix}Step 3: Cleaning whitespace")
        cleaned_sql = ' '.join(normalized_sql.split())
        logger.debug(f"{prefix}Whitespace cleaning completed, final SQL length: {len(cleaned_sql)} characters")
        
        if len(cleaned_sql) != len(sql):
            logger.info(f"{prefix}SQL preprocessing completed - length changed from {len(sql)} to {len(cleaned_sql)} characters")
        else:
            logger.debug(f"{prefix}SQL preprocessing completed - no length change")
        
        return cleaned_sql
    
    def _execute_bq_query(self, query: str, operation_name: str = "BigQuery query") -> Any:
        """Execute BigQuery query with error handling"""
        def execute():
            logger.info(f"Executing {operation_name}")
            logger.debug(f"Query length: {len(query)} characters")
            if len(query) > 200:
                logger.debug(f"Query preview: {query[:200]}...")
            else:
                logger.debug(f"Full query: {query}")
            
            query_job = self.bq_client.query(query)
            logger.debug(f"Query job created with job_id: {getattr(query_job, 'job_id', 'unknown')}")
            
            result = query_job.result()
            logger.debug(f"Query execution completed successfully")
            return result
        
        return self._handle_operation(execute, operation_name)
    
    def _validate_table_format(self, table_name: str, record_index: Optional[int] = None) -> List[str]:
        """Validate BigQuery table format and return parts"""
        if not table_name or not table_name.strip():
            raise MetricsPipelineError(f"Record {record_index}: target_table cannot be empty")
        
        table_parts = table_name.strip().split('.')
        if len(table_parts) != 3:
            raise MetricsPipelineError(
                f"Record {record_index}: target_table '{table_name}' must be in format 'project.dataset.table'"
            )
        
        part_names = ['project', 'dataset', 'table']
        for i, part in enumerate(table_parts):
            if not part.strip():
                raise MetricsPipelineError(
                    f"Record {record_index}: target_table '{table_name}' has empty {part_names[i]} part"
                )
        
        return table_parts
    
    def _validate_field_content(self, record: Dict, field: str, record_index: int) -> None:
        """Validate that a field exists and has valid content"""
        if field not in record:
            raise MetricsPipelineError(f"Record {record_index}: Missing required field '{field}'")
        
        value = record[field]
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise MetricsPipelineError(
                f"Record {record_index}: Field '{field}' is null, empty, or contains only whitespace"
            )
    
    def _normalize_numeric_value(self, value: Union[int, float, Decimal, str, None]) -> Optional[str]:
        """Normalize numeric values to string representation to preserve precision"""
        if value is None:
            logger.debug("Normalizing None value - returning None")
            return None
        
        logger.debug(f"Normalizing numeric value: {value} (type: {type(value).__name__})")
        
        try:
            if isinstance(value, Decimal):
                result = str(value)
                logger.debug(f"Decimal value normalized: {value} -> {result}")
                return result
            elif isinstance(value, (int, float)):
                result = str(Decimal(str(value)))
                logger.debug(f"Numeric value normalized: {value} -> {result}")
                return result
            elif isinstance(value, str):
                result = str(Decimal(value))
                logger.debug(f"String value normalized: {value} -> {result}")
                return result
            else:
                result = str(Decimal(str(value)))
                logger.debug(f"Other type normalized: {value} -> {result}")
                return result
        except (ValueError, TypeError, OverflowError, Exception) as e:
            logger.warning(f"Could not normalize numeric value: {value} (type: {type(value).__name__}), error: {e}")
            return None
    
    def _safe_decimal_conversion(self, value: Optional[str]) -> Optional[Decimal]:
        """Safely convert string to Decimal for BigQuery"""
        if value is None:
            logger.debug("Converting None value to Decimal - returning None")
            return None
        
        logger.debug(f"Converting to Decimal: {value} (type: {type(value).__name__})")
        
        try:
            result = Decimal(value)
            logger.debug(f"Successfully converted to Decimal: {value} -> {result}")
            return result
        except (ValueError, TypeError, OverflowError) as e:
            logger.warning(f"Could not convert to Decimal: {value} (type: {type(value).__name__}), error: {e}")
            return None
    
    def _find_table_references(self, sql: str) -> List[Tuple[str, str, str]]:
        """Find all table references in SQL"""
        logger.debug(f"Finding table references in SQL (length: {len(sql)} characters)")
        table_pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
        table_refs = re.findall(table_pattern, sql)
        logger.debug(f"Found {len(table_refs)} table references: {table_refs}")
        return table_refs
    
    def _build_metric_record(self, record: Dict, sql_results: Dict, partition_dt: str) -> Dict:
        """Build final metric record with proper type conversion"""
        metric_id = record['metric_id']
        logger.debug(f"Building metric record for metric_id: {metric_id}")
        
        # Convert numeric values
        numerator_value = self._safe_decimal_conversion(sql_results['numerator_value'])
        denominator_value = self._safe_decimal_conversion(sql_results['denominator_value'])
        metric_output = self._safe_decimal_conversion(sql_results['metric_output'])
        
        logger.debug(f"Metric {metric_id} - Numeric conversions:")
        logger.debug(f"  numerator_value: {sql_results['numerator_value']} -> {numerator_value}")
        logger.debug(f"  denominator_value: {sql_results['denominator_value']} -> {denominator_value}")
        logger.debug(f"  metric_output: {sql_results['metric_output']} -> {metric_output}")
        
        metric_record = {
            'metric_id': record['metric_id'],
            'metric_name': record['metric_name'],
            'metric_type': record['metric_type'],
            'numerator_value': numerator_value,
            'denominator_value': denominator_value,
            'metric_output': metric_output,
            'business_data_date': sql_results['business_data_date'],
            'partition_dt': partition_dt,
            'pipeline_execution_ts': datetime.utcnow()
        }
        
        logger.debug(f"Metric record built successfully for {metric_id}")
        return metric_record
    
    def _create_dataframe_schema(self) -> StructType:
        """Create standard DataFrame schema for metrics"""
        logger.debug("Creating DataFrame schema for metrics")
        schema = StructType([
            StructField("metric_id", StringType(), False),
            StructField("metric_name", StringType(), False),
            StructField("metric_type", StringType(), False),
            StructField("numerator_value", DecimalType(38, 9), True),
            StructField("denominator_value", DecimalType(38, 9), True),
            StructField("metric_output", DecimalType(38, 9), True),
            StructField("business_data_date", StringType(), False),
            StructField("partition_dt", StringType(), False),
            StructField("pipeline_execution_ts", TimestampType(), False)
        ])
        logger.debug(f"DataFrame schema created with {len(schema.fields)} fields")
        return schema
    
    def validate_gcs_path(self, gcs_path: str) -> str:
        """Validate GCS path format and accessibility"""
        logger.debug(f"Validating GCS path: {gcs_path}")
        
        if not gcs_path.startswith('gs://'):
            raise MetricsPipelineError(f"Invalid GCS path format: {gcs_path}. Must start with 'gs://'")
        
        path_parts = gcs_path.replace('gs://', '').split('/')
        logger.debug(f"GCS path parts: {path_parts}")
        
        if len(path_parts) < 2:
            raise MetricsPipelineError(f"Invalid GCS path structure: {gcs_path}")
        
        def test_access():
            logger.debug(f"Testing GCS path accessibility: {gcs_path}")
            test_df = self.spark.read.option("multiline", "true").json(gcs_path).limit(0)
            row_count = test_df.count()
            logger.debug(f"GCS path test successful - found {row_count} rows (limited to 0)")
            logger.info(f"GCS path validated successfully: {gcs_path}")
            return gcs_path
        
        return self._handle_operation(test_access, f"GCS path validation for {gcs_path}")
    
    def read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """Read JSON file from GCS and return as list of dictionaries"""
        def read_and_convert():
            validated_path = self.validate_gcs_path(gcs_path)
            logger.info(f"Reading JSON from GCS: {validated_path}")
            
            logger.debug("Creating Spark DataFrame from JSON")
            df = self.spark.read.option("multiline", "true").json(validated_path)
            
            logger.debug("Counting rows in DataFrame")
            row_count = df.count()
            logger.debug(f"DataFrame contains {row_count} rows")
            
            if row_count == 0:
                raise MetricsPipelineError(f"No data found in JSON file: {validated_path}")
            
            logger.debug("Converting DataFrame to list of dictionaries")
            json_data = [row.asDict() for row in df.collect()]
            logger.debug(f"Converted {len(json_data)} rows to dictionaries")
            
            logger.info(f"Successfully read {len(json_data)} records from JSON")
            return json_data
        
        return self._handle_operation(read_and_convert, "JSON reading from GCS")
    
    def validate_json(self, json_data: List[Dict]) -> List[Dict]:
        """Validate JSON data for required fields and duplicates"""
        logger.info(f"Starting JSON validation for {len(json_data)} records")
        metric_ids = set()
        validation_errors = []
        
        for i, record in enumerate(json_data):
            logger.debug(f"Validating record {i+1}/{len(json_data)}")
            
            try:
                # Validate required fields
                for field in self.REQUIRED_FIELDS:
                    self._validate_field_content(record, field, i)
                
                # Check for duplicate metric IDs
                metric_id = record['metric_id'].strip()
                if metric_id in metric_ids:
                    error_msg = f"Record {i}: Duplicate metric_id '{metric_id}' found"
                    logger.error(error_msg)
                    raise MetricsPipelineError(error_msg)
                metric_ids.add(metric_id)
                logger.debug(f"Record {i}: Validated metric_id '{metric_id}'")
                
                # Validate target table format
                self._validate_table_format(record['target_table'], i)
                logger.debug(f"Record {i}: Validated target_table '{record['target_table']}'")
                
                # Validate SQL query and check for quote issues
                sql_query = record['sql'].strip()
                if sql_query:
                    logger.debug(f"Record {i}: Validating SQL query (length: {len(sql_query)} characters)")
                    
                    # Check for quote issues in SQL
                    quote_issues = self._detect_quote_issues(sql_query, metric_id)
                    if quote_issues['has_double_quotes']:
                        logger.info(f"Record {i}: SQL query contains double quotes - will be normalized during processing")
                    
                    # Validate SQL placeholders
                    placeholder_counts = {
                        name: len(re.findall(pattern, sql_query))
                        for name, pattern in self.PLACEHOLDER_PATTERNS.items()
                    }
                    
                    total_placeholders = sum(placeholder_counts.values())
                    logger.debug(f"Record {i}: Found {total_placeholders} placeholders: {placeholder_counts}")
                    
                    if total_placeholders == 0:
                        logger.warning(f"Record {i}: SQL query contains no date placeholders")
                    else:
                        logger.debug(f"Record {i}: Placeholder validation passed")
                    
                    # Basic SQL syntax validation
                    if not self._validate_sql_syntax(sql_query, metric_id):
                        logger.warning(f"Record {i}: SQL syntax validation failed")
                    else:
                        logger.debug(f"Record {i}: SQL syntax validation passed")
                else:
                    logger.warning(f"Record {i}: SQL query is empty or contains only whitespace")
                
                logger.debug(f"Record {i}: Validation completed successfully")
                
            except Exception as e:
                error_msg = f"Record {i}: Validation failed - {str(e)}"
                logger.error(error_msg)
                validation_errors.append(error_msg)
                raise MetricsPipelineError(error_msg)
        
        logger.info(f"Successfully validated {len(json_data)} records with {len(metric_ids)} unique metric IDs")
        if validation_errors:
            logger.warning(f"Validation completed with {len(validation_errors)} errors")
        else:
            logger.info("All records passed validation successfully")
        
        return json_data
    
    def find_placeholder_positions(self, sql: str) -> List[Tuple[str, int, int]]:
        """Find all placeholders in SQL with their positions"""
        logger.debug(f"Finding placeholder positions in SQL (length: {len(sql)} characters)")
        placeholders = []
        
        for placeholder_type, pattern in self.PLACEHOLDER_PATTERNS.items():
            logger.debug(f"Searching for placeholder type: {placeholder_type}")
            matches = list(re.finditer(pattern, sql))
            logger.debug(f"Found {len(matches)} matches for {placeholder_type}")
            
            for match in matches:
                placeholder_info = (placeholder_type, match.start(), match.end())
                placeholders.append(placeholder_info)
                logger.debug(f"Found {placeholder_type} at position {match.start()}-{match.end()}: '{sql[match.start():match.end()]}'")
        
        sorted_placeholders = sorted(placeholders, key=lambda x: x[1])
        logger.debug(f"Total placeholders found: {len(sorted_placeholders)}")
        return sorted_placeholders
    
    def get_table_for_placeholder(self, sql: str, placeholder_pos: int) -> Optional[Tuple[str, str]]:
        """Find the table associated with a placeholder based on its position"""
        logger.debug(f"Finding table for placeholder at position {placeholder_pos}")
        
        table_references = []
        for match in re.finditer(r'`([^.]+)\.([^.]+)\.([^`]+)`', sql):
            table_references.append((match.end(), match.groups()))
        
        logger.debug(f"Found {len(table_references)} table references in SQL")
        
        # Find closest table before placeholder
        best_table = None
        best_distance = float('inf')
        
        for table_end_pos, (project, dataset, table) in table_references:
            logger.debug(f"Checking table reference: {project}.{dataset}.{table} at position {table_end_pos}")
            
            if table_end_pos < placeholder_pos:
                distance = placeholder_pos - table_end_pos
                logger.debug(f"Table is before placeholder, distance: {distance}")
                
                if distance < best_distance:
                    best_distance = distance
                    best_table = (dataset, table)
                    logger.debug(f"New best table: {dataset}.{table} (distance: {distance})")
            else:
                logger.debug(f"Table is after placeholder, skipping")
        
        if best_table:
            logger.debug(f"Selected table for placeholder: {best_table[0]}.{best_table[1]} (distance: {best_distance})")
        else:
            logger.debug("No suitable table found for placeholder")
        
        return best_table
    
    def get_partition_dt(self, dataset: str, table_name: str, partition_info_table: str) -> Optional[str]:
        """Get latest partition_dt from metadata table"""
        logger.debug(f"Getting partition_dt for {dataset}.{table_name} from {partition_info_table}")
        
        query = f"""
        SELECT partition_dt 
        FROM `{partition_info_table}` 
        WHERE project_dataset = '{dataset}' 
        AND table_name = '{table_name}'
        ORDER BY partition_dt DESC
        LIMIT 1
        """
        
        logger.debug(f"Partition query: {query}")
        logger.info(f"Querying partition info for {dataset}.{table_name}")
        
        try:
            results = self._execute_bq_query(query, f"partition info query for {dataset}.{table_name}")
            
            row_count = 0
            for row in results:
                row_count += 1
                partition_dt = row.partition_dt
                formatted_dt = partition_dt.strftime('%Y-%m-%d') if isinstance(partition_dt, datetime) else str(partition_dt)
                logger.debug(f"Found partition_dt: {formatted_dt} for {dataset}.{table_name}")
                return formatted_dt
            
            logger.warning(f"No partition info found for {dataset}.{table_name} (checked {row_count} rows)")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get partition_dt for {dataset}.{table_name}: {str(e)}")
            return None
    
    def replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str) -> str:
        """Replace placeholders in SQL with appropriate dates"""
        logger.debug(f"Starting placeholder replacement for SQL (length: {len(sql)} characters)")
        logger.debug(f"Run date: {run_date}, Partition info table: {partition_info_table}")
        
        placeholders = self.find_placeholder_positions(sql)
        
        if not placeholders:
            logger.info("No placeholders found in SQL query")
            return sql
        
        logger.info(f"Found {len(placeholders)} placeholders in SQL: {[p[0] for p in placeholders]}")
        
        final_sql = sql
        replacements_made = 0
        
        for placeholder_type, start_pos, end_pos in reversed(placeholders):
            logger.debug(f"Processing placeholder '{placeholder_type}' at position {start_pos}-{end_pos}")
            
            if placeholder_type == 'currently':
                replacement_date = run_date
                logger.info(f"Replacing {{currently}} placeholder with run_date: {replacement_date}")
                
            elif placeholder_type == 'partition_info':
                logger.debug(f"Finding table for {{partition_info}} placeholder at position {start_pos}")
                table_info = self.get_table_for_placeholder(sql, start_pos)
                
                if not table_info:
                    error_msg = f"Could not find table reference for {{partition_info}} placeholder at position {start_pos}"
                    logger.error(error_msg)
                    raise MetricsPipelineError(error_msg)
                
                dataset, table_name = table_info
                logger.debug(f"Getting partition_dt for table {dataset}.{table_name}")
                replacement_date = self.get_partition_dt(dataset, table_name, partition_info_table)
                
                if not replacement_date:
                    error_msg = f"Could not determine partition_dt for table {dataset}.{table_name}"
                    logger.error(error_msg)
                    raise MetricsPipelineError(error_msg)
                
                logger.info(f"Replacing {{partition_info}} placeholder with partition_dt: {replacement_date} for table {dataset}.{table_name}")
            else:
                logger.warning(f"Unknown placeholder type: {placeholder_type}")
                continue
            
            # Perform replacement
            original_placeholder = final_sql[start_pos:end_pos]
            replacement_value = f"'{replacement_date}'"
            final_sql = final_sql[:start_pos] + replacement_value + final_sql[end_pos:]
            
            logger.debug(f"Replaced '{original_placeholder}' with '{replacement_value}'")
            replacements_made += 1
        
        logger.info(f"Successfully replaced {replacements_made} placeholders in SQL")
        logger.debug(f"Final SQL length: {len(final_sql)} characters")
        return final_sql
    
    def _validate_denominator(self, result_dict: Dict, metric_id: Optional[str] = None) -> None:
        """Validate denominator value"""
        prefix = f"Metric '{metric_id}': " if metric_id else ""
        
        denominator_value = result_dict['denominator_value']
        logger.debug(f"{prefix}Validating denominator value: {denominator_value}")
        
        if denominator_value is None:
            logger.debug(f"{prefix}Denominator value is None, skipping validation")
            return
        
        try:
            denominator_decimal = self._safe_decimal_conversion(denominator_value)
            if denominator_decimal is None:
                logger.warning(f"{prefix}Could not convert denominator value to Decimal: {denominator_value}")
                return
            
            logger.debug(f"{prefix}Denominator converted to Decimal: {denominator_decimal}")
            
            if denominator_decimal == 0:
                error_msg = f"{prefix}Invalid denominator value: denominator_value is 0. Cannot calculate metrics with zero denominator."
                logger.error(error_msg)
                raise MetricsPipelineError(error_msg)
            elif denominator_decimal < 0:
                error_msg = f"{prefix}Invalid denominator value: denominator_value is negative ({denominator_decimal}). Negative denominators are not allowed."
                logger.error(error_msg)
                raise MetricsPipelineError(error_msg)
            elif abs(denominator_decimal) < Decimal('0.0000001'):
                warning_msg = f"{prefix}Very small denominator value detected: {denominator_decimal}. This may cause precision issues."
                logger.warning(warning_msg)
            else:
                logger.debug(f"{prefix}Denominator validation passed: {denominator_decimal}")
                
        except (ValueError, TypeError) as e:
            logger.warning(f"{prefix}Could not validate denominator_value: {denominator_value}, error: {e}")
    
    def execute_sql(self, sql: str, run_date: str, partition_info_table: str, metric_id: Optional[str] = None) -> Dict:
        """Execute SQL query with dynamic placeholder replacement and quote handling"""
        prefix = f"Metric '{metric_id}': " if metric_id else ""
        logger.info(f"{prefix}Starting SQL execution")
        
        def execute_and_process():
            logger.debug(f"{prefix}Step 1: Preprocessing SQL query")
            # Step 1: Preprocess SQL to handle quotes and other issues
            preprocessed_sql = self._preprocess_sql_query(sql, metric_id)
            logger.debug(f"{prefix}SQL preprocessing completed")
            
            logger.debug(f"{prefix}Step 2: Replacing placeholders")
            # Step 2: Replace placeholders
            final_sql = self.replace_sql_placeholders(preprocessed_sql, run_date, partition_info_table)
            logger.debug(f"{prefix}Placeholder replacement completed")
            
            logger.info(f"{prefix}Executing SQL query with placeholder replacements and quote normalization")
            
            # Log final SQL for debugging (truncated)
            if len(final_sql) > 200:
                logger.debug(f"{prefix}Final SQL (truncated): {final_sql[:200]}...")
            else:
                logger.debug(f"{prefix}Final SQL: {final_sql}")
            
            logger.debug(f"{prefix}Executing BigQuery query")
            results = self._execute_bq_query(final_sql, "SQL execution")
            
            result_dict = {
                'metric_output': None,
                'numerator_value': None,
                'denominator_value': None,
                'business_data_date': None
            }
            
            logger.debug(f"{prefix}Processing query results")
            row_count = 0
            for row in results:
                row_count += 1
                row_dict = dict(row)
                logger.debug(f"{prefix}Processing row {row_count}: {row_dict}")
                
                for key in result_dict.keys():
                    if key in row_dict:
                        value = row_dict[key]
                        logger.debug(f"{prefix}Processing field '{key}': {value} (type: {type(value).__name__})")
                        
                        if key in self.NUMERIC_FIELDS:
                            normalized_value = self._normalize_numeric_value(value)
                            result_dict[key] = normalized_value
                            logger.debug(f"{prefix}Normalized {key}: {value} -> {normalized_value}")
                        else:
                            result_dict[key] = value
                            logger.debug(f"{prefix}Set {key}: {value}")
                break
            
            logger.debug(f"{prefix}Processed {row_count} rows from query results")
            logger.debug(f"{prefix}Final result dict: {result_dict}")
            
            logger.debug(f"{prefix}Validating denominator")
            self._validate_denominator(result_dict, metric_id)
            
            if result_dict['business_data_date'] is not None:
                original_date = result_dict['business_data_date']
                result_dict['business_data_date'] = result_dict['business_data_date'].strftime('%Y-%m-%d')
                logger.debug(f"{prefix}Formatted business_data_date: {original_date} -> {result_dict['business_data_date']}")
            else:
                error_msg = f"{prefix}business_data_date is required but was not returned by the SQL query"
                logger.error(error_msg)
                raise MetricsPipelineError(error_msg)
            
            logger.info(f"{prefix}SQL execution completed successfully")
            return result_dict
        
        operation_name = f"SQL execution for metric {metric_id}" if metric_id else "SQL execution"
        return self._handle_operation(execute_and_process, operation_name)
    
    def _rollback_single_metric(self, metric_id: str, target_table: str, partition_dt: str) -> None:
        """Rollback a specific metric from the target table"""
        query = f"""
        DELETE FROM `{target_table}` 
        WHERE metric_id = '{metric_id}' 
        AND partition_dt = '{partition_dt}'
        AND pipeline_execution_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """
        
        logger.info(f"Rolling back metric {metric_id} from {target_table}")
        
        try:
            self._execute_bq_query(query, f"rollback metric {metric_id}")
            logger.info(f"Successfully rolled back metric {metric_id}")
        except Exception as e:
            logger.error(f"Failed to rollback metric {metric_id}: {str(e)}")
    
    def check_dependencies_exist(self, json_data: List[Dict], dependencies: List[str]) -> None:
        """Check if all specified dependencies exist in the JSON data"""
        available_dependencies = set(record['dependency'] for record in json_data)
        missing_dependencies = set(dependencies) - available_dependencies
        
        if missing_dependencies:
            raise MetricsPipelineError(
                f"Missing dependencies in JSON data: {missing_dependencies}. "
                f"Available dependencies: {available_dependencies}"
            )
        
        logger.info(f"All dependencies found: {dependencies}")
    
    def process_metrics(self, json_data: List[Dict], run_date: str, 
                       dependencies: List[str], partition_info_table: str) -> Tuple[Dict[str, DataFrame], List[Dict], List[Dict]]:
        """Process metrics and create Spark DataFrames grouped by target_table"""
        logger.info(f"Starting metrics processing for dependencies: {dependencies}")
        logger.debug(f"Input data: {len(json_data)} total records, {len(dependencies)} dependencies")
        
        self.check_dependencies_exist(json_data, dependencies)
        partition_dt = datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Using pipeline run date as partition_dt: {partition_dt}")
        
        # Filter and group records
        logger.debug("Filtering records by dependencies")
        filtered_data = [record for record in json_data if record['dependency'] in dependencies]
        
        if not filtered_data:
            raise MetricsPipelineError(f"No records found for dependencies: {dependencies}")
        
        logger.info(f"Found {len(filtered_data)} records to process")
        logger.debug(f"Filtered records by dependency: {[r['dependency'] for r in filtered_data]}")
        
        # Group by target table
        logger.debug("Grouping records by target table")
        records_by_table = {}
        for record in filtered_data:
            target_table = record['target_table'].strip()
            records_by_table.setdefault(target_table, []).append(record)
            logger.debug(f"Added record {record['metric_id']} to target table: {target_table}")
        
        logger.info(f"Records grouped into {len(records_by_table)} target tables: {list(records_by_table.keys())}")
        
        # Process each group
        result_dfs = {}
        successful_metrics = []
        failed_metrics = []
        
        for target_table, records in records_by_table.items():
            logger.info(f"Processing {len(records)} metrics for target table: {target_table}")
            logger.debug(f"Target table {target_table} records: {[r['metric_id'] for r in records]}")
            
            processed_records = []
            table_success_count = 0
            table_fail_count = 0
            
            for i, record in enumerate(records):
                metric_id = record['metric_id']
                logger.debug(f"Processing metric {i+1}/{len(records)}: {metric_id}")
                
                try:
                    logger.debug(f"Executing SQL for metric {metric_id}")
                    sql_results = self.execute_sql(
                        record['sql'], run_date, partition_info_table, metric_id
                    )
                    
                    logger.debug(f"Building metric record for {metric_id}")
                    final_record = self._build_metric_record(record, sql_results, partition_dt)
                    processed_records.append(final_record)
                    successful_metrics.append(record)
                    table_success_count += 1
                    logger.info(f"Successfully processed metric_id: {metric_id} for table: {target_table}")
                    
                except Exception as e:
                    error_message = str(e)
                    table_fail_count += 1
                    logger.error(f"Failed to process metric_id {metric_id} for table {target_table}: {error_message}")
                    failed_metrics.append({
                        'metric_record': record,
                        'error_message': error_message
                    })
                    continue
            
            logger.info(f"Target table {target_table} processing complete: {table_success_count} successful, {table_fail_count} failed")
            
            # Create DataFrame if we have successful records
            if processed_records:
                logger.debug(f"Creating DataFrame for {target_table} with {len(processed_records)} records")
                schema = self._create_dataframe_schema()
                df = self.spark.createDataFrame(processed_records, schema)
                row_count = df.count()
                result_dfs[target_table] = df
                logger.info(f"Created DataFrame for {target_table} with {row_count} records")
                logger.debug(f"DataFrame schema for {target_table}:")
                df.printSchema()
            else:
                logger.warning(f"No records processed successfully for target table: {target_table}")
        
        total_successful = len(successful_metrics)
        total_failed = len(failed_metrics)
        logger.info(f"Processing complete: {total_successful} successful, {total_failed} failed")
        
        if failed_metrics:
            failed_ids = [fm['metric_record']['metric_id'] for fm in failed_metrics]
            logger.warning(f"Failed metrics: {failed_ids}")
        
        logger.debug(f"Result DataFrames: {list(result_dfs.keys())}")
        return result_dfs, successful_metrics, failed_metrics
    
    def get_bq_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Get BigQuery table schema"""
        logger.debug(f"Getting BigQuery table schema for: {table_name}")
        
        def get_schema():
            logger.info(f"Getting schema for table: {table_name}")
            table = self.bq_client.get_table(table_name)
            schema = table.schema
            logger.debug(f"Retrieved schema with {len(schema)} fields for table: {table_name}")
            for field in schema:
                logger.debug(f"  Field: {field.name}, Type: {field.field_type}, Mode: {field.mode}")
            return schema
        
        try:
            return get_schema()
        except NotFound:
            error_msg = f"Table not found: {table_name}"
            logger.error(error_msg)
            raise MetricsPipelineError(error_msg)
        except Exception as e:
            error_msg = f"Failed to get table schema for {table_name}: {str(e)}"
            logger.error(error_msg)
            raise MetricsPipelineError(error_msg)
    
    def align_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame:
        """Align Spark DataFrame with BigQuery table schema"""
        logger.info(f"Aligning DataFrame schema with BigQuery table: {target_table}")
        logger.debug(f"Original DataFrame columns: {df.columns}")
        logger.debug(f"Original DataFrame row count: {df.count()}")
        
        bq_schema = self.get_bq_table_schema(target_table)
        current_columns = df.columns
        bq_columns = [field.name for field in bq_schema]
        
        logger.debug(f"BigQuery table columns: {bq_columns}")
        
        # Filter columns and reorder
        columns_to_keep = [col for col in current_columns if col in bq_columns]
        columns_to_drop = [col for col in current_columns if col not in bq_columns]
        
        logger.debug(f"Columns to keep: {columns_to_keep}")
        logger.debug(f"Columns to drop: {columns_to_drop}")
        
        if columns_to_drop:
            logger.info(f"Dropping extra columns: {columns_to_drop}")
            df = df.drop(*columns_to_drop)
        
        df = df.select(*[col(c) for c in bq_columns if c in columns_to_keep])
        logger.debug(f"DataFrame after column filtering: {df.columns}")
        
        # Handle type conversions
        type_conversions = {
            'DATE': lambda field: df.withColumn(field.name, to_date(col(field.name))),
            'TIMESTAMP': lambda field: df.withColumn(field.name, col(field.name).cast(TimestampType())),
            'NUMERIC': lambda field: df.withColumn(field.name, col(field.name).cast(DecimalType(38, 9))),
            'FLOAT': lambda field: df.withColumn(field.name, col(field.name).cast(DoubleType()))
        }
        
        conversion_count = 0
        for field in bq_schema:
            if field.name in df.columns and field.field_type in type_conversions:
                logger.debug(f"Converting field '{field.name}' from type '{field.field_type}'")
                df = type_conversions[field.field_type](field)
                conversion_count += 1
        
        logger.debug(f"Applied {conversion_count} type conversions")
        logger.info(f"Schema alignment complete. Final columns: {df.columns}")
        logger.debug(f"Final DataFrame row count: {df.count()}")
        return df
    
    def _manage_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> Tuple[List[str], List[str]]:
        """Check and delete existing metrics, return existing and new metrics"""
        if not metric_ids:
            return [], []
        
        # Check existing metrics
        escaped_ids = [mid.replace("'", "''") for mid in metric_ids]
        ids_str = "', '".join(escaped_ids)
        
        query = f"""
        SELECT DISTINCT metric_id 
        FROM `{target_table}` 
        WHERE metric_id IN ('{ids_str}') 
        AND partition_dt = '{partition_dt}'
        """
        
        logger.info(f"Checking existing metrics for partition_dt: {partition_dt}")
        
        try:
            results = self._execute_bq_query(query, "existing metrics check")
            existing_metrics = [row.metric_id for row in results]
            
            if existing_metrics:
                logger.info(f"Found {len(existing_metrics)} existing metrics: {existing_metrics}")
                
                # Delete existing metrics
                delete_query = f"""
                DELETE FROM `{target_table}` 
                WHERE metric_id IN ('{ids_str}') 
                AND partition_dt = '{partition_dt}'
                """
                
                logger.info(f"Deleting existing metrics: {existing_metrics}")
                delete_results = self._execute_bq_query(delete_query, "existing metrics deletion")
                
                deleted_count = getattr(delete_results, 'num_dml_affected_rows', 0)
                logger.info(f"Successfully deleted {deleted_count} existing records")
                
                self.overwritten_metrics.extend(existing_metrics)
            else:
                logger.info("No existing metrics found")
            
            new_metrics = [mid for mid in metric_ids if mid not in existing_metrics]
            return existing_metrics, new_metrics
            
        except Exception as e:
            logger.error(f"Failed to manage existing metrics: {str(e)}")
            raise MetricsPipelineError(f"Failed to manage existing metrics: {str(e)}")
    
    def write_to_bq_with_overwrite(self, df: DataFrame, target_table: str) -> Tuple[List[str], List[Dict]]:
        """Write DataFrame to BigQuery table with overwrite capability"""
        try:
            logger.info(f"Writing DataFrame to BigQuery table with overwrite: {target_table}")
            
            self.target_tables.add(target_table)
            
            # Get metric IDs and partition date
            metric_records = df.select('metric_id', 'partition_dt').distinct().collect()
            
            if not metric_records:
                logger.warning("No records to process")
                return [], []
            
            partition_dt = metric_records[0]['partition_dt']
            metric_ids = [row['metric_id'] for row in metric_records]
            
            logger.info(f"Processing {len(metric_ids)} metrics for partition_dt: {partition_dt}")
            
            # Manage existing metrics
            existing_metrics, new_metrics = self._manage_existing_metrics(metric_ids, partition_dt, target_table)
            
            if new_metrics:
                logger.info(f"Adding {len(new_metrics)} new metrics: {new_metrics}")
            
            # Track processed metrics
            self.processed_metrics.extend(metric_ids)
            
            # Write to BigQuery
            df.write \
                .format("bigquery") \
                .option("table", target_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} records to {target_table}")
            logger.info(f"Summary: {len(existing_metrics)} overwritten, {len(new_metrics)} new metrics")
            
            return metric_ids, []
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"Failed to write to BigQuery with overwrite: {error_message}")
            
            # Return failed metrics
            failed_metrics = []
            metric_records = df.select('metric_id').distinct().collect()
            for row in metric_records:
                failed_metrics.append({
                    'metric_id': row['metric_id'],
                    'error_message': error_message
                })
            
            return [], failed_metrics
    
    def rollback_all_processed_metrics(self, partition_dt: str) -> None:
        """Rollback all processed metrics from all target tables"""
        logger.info("Starting rollback of processed metrics from all target tables")
        
        if not self.processed_metrics:
            logger.info("No metrics to rollback")
            return
        
        if not self.target_tables:
            logger.info("No target tables to rollback from")
            return
        
        # Only rollback newly inserted metrics
        new_metrics = [mid for mid in self.processed_metrics if mid not in self.overwritten_metrics]
        
        if new_metrics:
            logger.info(f"Rolling back {len(new_metrics)} newly inserted metrics from {len(self.target_tables)} tables")
            
            for target_table in self.target_tables:
                logger.info(f"Rolling back metrics from table: {target_table}")
                
                for metric_id in new_metrics:
                    try:
                        self._rollback_single_metric(metric_id, target_table, partition_dt)
                    except Exception as e:
                        logger.error(f"Failed to rollback metric {metric_id} from table {target_table}: {str(e)}")
        else:
            logger.info("No newly inserted metrics to rollback")
        
        if self.overwritten_metrics:
            logger.warning(f"Note: {len(self.overwritten_metrics)} overwritten metrics cannot be automatically restored: {self.overwritten_metrics}")
        
        logger.info("Rollback process completed")
    
    def _extract_table_info(self, table_reference: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract dataset and table name from table reference"""
        try:
            parts = table_reference.split('.')
            if len(parts) >= 3:
                return parts[1], parts[2]
            elif len(parts) == 2:
                return parts[0], parts[1]
            else:
                return None, None
        except Exception:
            return None, None
    
    def _build_base_recon_record(self, metric_record: Dict, run_date: str, env: str, 
                                execution_status: str, partition_dt: str) -> Dict:
        """Build base recon record with common fields"""
        current_timestamp = datetime.utcnow()
        is_success = execution_status == 'success'
        
        # Extract table info
        target_dataset, target_table = self._extract_table_info(metric_record['target_table'])
        
        return {
            'module_id': '103',
            'module_type_nm': 'Metrics',
            'source_server_nm': env,
            'target_server_nm': env,
            'target_databs_nm': target_dataset or 'UNKNOWN',
            'target_table_nm': target_table or 'UNKNOWN',
            'source_vl': '0',
            'target_vl': '0' if is_success else '1',
            'excldd_vl': '0' if is_success else '1',
            'rcncln_exact_pass_in': 'Passed' if is_success else 'Failed',
            'latest_source_parttn_dt': run_date,
            'latest_target_parttn_dt': run_date,
            'load_ts': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'schdld_dt': datetime.strptime(partition_dt, '%Y-%m-%d').date(),
            'source_system_id': metric_record['metric_id'],
            'schdld_yr': current_timestamp.year,
            'Job_Name': metric_record['metric_name']
        }
    
    def build_recon_record(self, metric_record: Dict, sql: str, run_date: str, 
                          env: str, execution_status: str, partition_dt: str, 
                          error_message: Optional[str] = None) -> Dict:
        """Build a reconciliation record for a metric"""
        try:
            # Get base record
            recon_record = self._build_base_recon_record(metric_record, run_date, env, execution_status, partition_dt)
            
            # Add source table info
            table_refs = self._find_table_references(sql)
            if table_refs:
                project, dataset, table = table_refs[0]
                recon_record.update({
                    'source_databs_nm': dataset,
                    'source_table_nm': table
                })
            else:
                recon_record.update({
                    'source_databs_nm': 'UNKNOWN',
                    'source_table_nm': 'UNKNOWN'
                })
            
            # Add status-dependent fields
            is_success = execution_status == 'success'
            if is_success:
                exclusion_reason = 'Metric data was successfully written.'
                clcltn_ds = 'Success'
            else:
                exclusion_reason = 'Metric data was failed written.'
                clcltn_ds = 'Failed'
                if error_message:
                    clean_error = error_message.replace('\n', ' ').replace('\r', ' ').strip()
                    if len(clean_error) > 500:
                        clean_error = clean_error[:497] + '...'
                    exclusion_reason += f' Error: {clean_error}'
            
            # Add remaining fields
            recon_record.update({
                'source_column_nm': 'NA',
                'source_file_nm': 'NA',
                'source_contrl_file_nm': 'NA',
                'target_column_nm': 'NA',
                'target_file_nm': 'NA',
                'target_contrl_file_nm': 'NA',
                'clcltn_ds': clcltn_ds,
                'excldd_reason_tx': exclusion_reason,
                'tolrnc_pc': 'NA',
                'rcncln_tolrnc_pass_in': 'NA'
            })
            
            logger.debug(f"Built recon record for metric {metric_record['metric_id']}: {execution_status}")
            return recon_record
            
        except Exception as e:
            logger.error(f"Failed to build recon record for metric {metric_record['metric_id']}: {str(e)}")
            raise MetricsPipelineError(f"Failed to build recon record: {str(e)}")
    
    def _create_recon_schema(self) -> StructType:
        """Create schema for recon table"""
        return StructType([
            StructField("module_id", StringType(), False),
            StructField("module_type_nm", StringType(), False),
            StructField("source_databs_nm", StringType(), True),
            StructField("source_table_nm", StringType(), True),
            StructField("source_column_nm", StringType(), True),
            StructField("source_file_nm", StringType(), True),
            StructField("source_contrl_file_nm", StringType(), True),
            StructField("source_server_nm", StringType(), False),
            StructField("target_databs_nm", StringType(), True),
            StructField("target_table_nm", StringType(), True),
            StructField("target_column_nm", StringType(), True),
            StructField("target_file_nm", StringType(), True),
            StructField("target_contrl_file_nm", StringType(), True),
            StructField("target_server_nm", StringType(), False),
            StructField("source_vl", StringType(), False),
            StructField("target_vl", StringType(), False),
            StructField("clcltn_ds", StringType(), True),
            StructField("excldd_vl", StringType(), True),
            StructField("excldd_reason_tx", StringType(), True),
            StructField("tolrnc_pc", StringType(), True),
            StructField("rcncln_exact_pass_in", StringType(), False),
            StructField("rcncln_tolrnc_pass_in", StringType(), True),
            StructField("latest_source_parttn_dt", StringType(), False),
            StructField("latest_target_parttn_dt", StringType(), False),
            StructField("load_ts", StringType(), False),
            StructField("schdld_dt", DateType(), False),
            StructField("source_system_id", StringType(), False),
            StructField("schdld_yr", IntegerType(), False),
            StructField("Job_Name", StringType(), False)
        ])
    
    def write_recon_to_bq(self, recon_records: List[Dict], recon_table: str) -> None:
        """Write reconciliation records to BigQuery recon table"""
        if not recon_records:
            logger.info("No recon records to write")
            return
        
        def write_recon():
            logger.info(f"Writing {len(recon_records)} recon records to {recon_table}")
            
            recon_schema = self._create_recon_schema()
            recon_df = self.spark.createDataFrame(recon_records, recon_schema)
            
            logger.info(f"Recon Schema for {recon_table}:")
            recon_df.printSchema()
            logger.info(f"Recon Data for {recon_table}:")
            recon_df.show(truncate=False)
            
            recon_df.write \
                .format("bigquery") \
                .option("table", recon_table) \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {len(recon_records)} recon records to {recon_table}")
        
        self._handle_operation(write_recon, "recon records write")
    
    def create_recon_records_from_write_results(self, json_data: List[Dict], run_date: str, 
                                              dependencies: List[str], partition_info_table: str,
                                              env: str, successful_writes: Dict[str, List[str]],
                                              failed_execution_metrics: List[Dict], 
                                              failed_write_metrics: Dict[str, List[Dict]],
                                              partition_dt: str) -> List[Dict]:
        """Create recon records based on execution results and write success/failure"""
        logger.info("Creating recon records based on execution and write results")
        
        # Filter records and create lookups
        filtered_data = [record for record in json_data if record['dependency'] in dependencies]
        
        failed_execution_lookup = {
            fm['metric_record']['metric_id']: fm['error_message'] 
            for fm in failed_execution_metrics
        }
        
        failed_write_lookup = {}
        for target_table, failed_metrics in failed_write_metrics.items():
            for failed_metric in failed_metrics:
                failed_write_lookup[failed_metric['metric_id']] = failed_metric['error_message']
        
        all_recon_records = []
        
        for record in filtered_data:
            metric_id = record['metric_id']
            target_table = record['target_table'].strip()
            
            # Determine status and error message
            is_success = target_table in successful_writes and metric_id in successful_writes[target_table]
            
            error_message = None
            if not is_success:
                error_message = (failed_execution_lookup.get(metric_id) or 
                               failed_write_lookup.get(metric_id) or 
                               "Unknown failure occurred during processing")
            
            execution_status = 'success' if is_success else 'failed'
            
            try:
                # Preprocess SQL with quote handling before placeholder replacement
                preprocessed_sql = self._preprocess_sql_query(record['sql'], metric_id)
                final_sql = self.replace_sql_placeholders(preprocessed_sql, run_date, partition_info_table)
                recon_record = self.build_recon_record(
                    record, final_sql, run_date, env, execution_status, partition_dt, error_message
                )
                all_recon_records.append(recon_record)
                logger.debug(f"Created recon record for metric {metric_id}: {execution_status}")
                
            except Exception as recon_error:
                logger.error(f"Failed to create recon record for metric {metric_id}: {str(recon_error)}")
                continue
        
        logger.info(f"Created {len(all_recon_records)} recon records based on execution and write results")
        return all_recon_records
    
    def log_quote_conversion_summary(self) -> None:
        """Log summary of quote conversions performed during pipeline execution"""
        if not self.quote_conversions:
            logger.info("No quote conversions were performed during pipeline execution")
            return
        
        logger.info(f"Quote Conversion Summary: {len(self.quote_conversions)} SQL queries had quotes normalized")
        
        for i, conversion in enumerate(self.quote_conversions, 1):
            metric_id = conversion['metric_id']
            quote_issues = conversion['quote_issues']
            
            logger.info(f"Conversion {i}: Metric '{metric_id}'")
            logger.info(f"  - Double quotes found: {quote_issues['double_quote_count']}")
            logger.info(f"  - String literals converted: {len(quote_issues['potential_string_literals'])}")
            if quote_issues['potential_string_literals']:
                logger.info(f"  - Converted literals: {quote_issues['potential_string_literals']}")
            
            # Log original and normalized SQL (truncated for readability)
            original_truncated = conversion['original_sql'][:100] + "..." if len(conversion['original_sql']) > 100 else conversion['original_sql']
            normalized_truncated = conversion['normalized_sql'][:100] + "..." if len(conversion['normalized_sql']) > 100 else conversion['normalized_sql']
            
            logger.debug(f"  - Original SQL: {original_truncated}")
            logger.debug(f"  - Normalized SQL: {normalized_truncated}")
        
        logger.info(f"Quote conversion summary complete. Total conversions: {len(self.quote_conversions)}")
    
    def get_quote_conversion_report(self) -> Dict[str, Any]:
        """Get detailed report of quote conversions for external analysis"""
        return {
            'total_conversions': len(self.quote_conversions),
            'conversions': self.quote_conversions,
            'summary': {
                'metrics_with_double_quotes': len(self.quote_conversions),
                'total_double_quotes_converted': sum(
                    conv['quote_issues']['double_quote_count'] for conv in self.quote_conversions
                ),
                'total_string_literals_converted': sum(
                    len(conv['quote_issues']['potential_string_literals']) for conv in self.quote_conversions
                )
            }
        }

@contextmanager
def managed_spark_session(app_name: str = "MetricsPipeline"):
    """Context manager for Spark session with proper cleanup"""
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
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

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='PySpark BigQuery Metrics Pipeline')
    
    required_args = [
        ('--gcs_path', 'GCS path to JSON input file'),
        ('--run_date', 'Run date in YYYY-MM-DD format'),
        ('--dependencies', 'Comma-separated list of dependencies to process'),
        ('--partition_info_table', 'BigQuery table for partition info (project.dataset.table)'),
        ('--env', 'Environment name (e.g., BLD, INT, PRE, PRD)'),
        ('--recon_table', 'BigQuery table for reconciliation data (project.dataset.table)')
    ]
    
    for arg, help_text in required_args:
        parser.add_argument(arg, required=True, help=help_text)
    
    return parser.parse_args()

def validate_date_format(date_str: str) -> None:
    """Validate date format"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise MetricsPipelineError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")

def log_pipeline_summary(pipeline: MetricsPipeline, successful_writes: Dict[str, List[str]], 
                        failed_execution_metrics: List[Dict], failed_write_metrics: Dict[str, List[Dict]]) -> None:
    """Log comprehensive pipeline summary"""
    logger.info("Pipeline completed successfully!")
    
    # Processing summary
    if pipeline.processed_metrics:
        logger.info(f"Total metrics processed: {len(pipeline.processed_metrics)}")
        if pipeline.overwritten_metrics:
            logger.info(f"Metrics overwritten: {len(pipeline.overwritten_metrics)}")
            logger.info(f"New metrics added: {len(pipeline.processed_metrics) - len(pipeline.overwritten_metrics)}")
        else:
            logger.info("All metrics were new (no existing metrics overwritten)")
    else:
        logger.info("No metrics were processed")
    
    # Execution summary
    logger.info(f"Execution results: {len([m for m in pipeline.processed_metrics if m not in [fm['metric_record']['metric_id'] for fm in failed_execution_metrics]])} successful, {len(failed_execution_metrics)} failed")
    
    if failed_execution_metrics:
        logger.warning(f"Failed to execute metrics: {[fm['metric_record']['metric_id'] for fm in failed_execution_metrics]}")
    
    # Write summary
    total_successful = sum(len(metrics) for metrics in successful_writes.values())
    total_failed_writes = sum(len(metrics) for metrics in failed_write_metrics.values())
    
    logger.info(f"Write results: {total_successful} successful, {total_failed_writes} failed")
    
    if successful_writes:
        logger.info("Successfully written metrics by table:")
        for table, metrics in successful_writes.items():
            logger.info(f"  {table}: {len(metrics)} metrics")
    
    if failed_write_metrics:
        logger.warning("Failed write metrics by table:")
        for table, metrics in failed_write_metrics.items():
            logger.warning(f"  {table}: {len(metrics)} metrics")

def main():
    """Main function with improved error handling and resource management"""
    pipeline = None
    partition_dt = None
    start_time = datetime.utcnow()
    
    try:
        logger.info("=" * 80)
        logger.info("STARTING METRICS PIPELINE")
        logger.info("=" * 80)
        
        args = parse_arguments()
        logger.debug(f"Parsed arguments: {vars(args)}")
        
        validate_date_format(args.run_date)
        logger.debug(f"Date format validation passed: {args.run_date}")
        
        dependencies = [dep.strip() for dep in args.dependencies.split(',') if dep.strip()]
        logger.debug(f"Parsed dependencies: {dependencies}")
        
        if not dependencies:
            raise MetricsPipelineError("No valid dependencies provided")
        
        # Log startup info
        startup_info = [
            "Starting Metrics Pipeline",
            f"GCS Path: {args.gcs_path}",
            f"Run Date: {args.run_date}",
            f"Dependencies: {dependencies}",
            f"Partition Info Table: {args.partition_info_table}",
            f"Environment: {args.env}",
            f"Recon Table: {args.recon_table}",
            "Pipeline will check for existing metrics and overwrite them if found",
            "Target tables will be read from JSON configuration",
            "JSON must contain: metric_id, metric_name, metric_type, sql, dependency, target_table",
            "SQL placeholders: {currently} = run_date, {partition_info} = partition_dt from metadata table",
            "Reconciliation records will be written to recon table for each metric with detailed error messages"
        ]
        
        for info in startup_info:
            logger.info(info)
        
        logger.info("Initializing Spark session and BigQuery client")
        with managed_spark_session("MetricsPipeline") as spark:
            logger.debug("Creating BigQuery client")
            bq_client = bigquery.Client()
            logger.debug("BigQuery client created successfully")
            
            logger.debug("Creating MetricsPipeline instance")
            pipeline = MetricsPipeline(spark, bq_client)
            logger.debug("MetricsPipeline instance created successfully")
            
            # Execute pipeline steps
            logger.info("=" * 50)
            logger.info("STEP 1: Reading JSON from GCS")
            logger.info("=" * 50)
            json_data = pipeline.read_json_from_gcs(args.gcs_path)
            
            logger.info("=" * 50)
            logger.info("STEP 2: Validating JSON data")
            logger.info("=" * 50)
            validated_data = pipeline.validate_json(json_data)
            
            logger.info("=" * 50)
            logger.info("STEP 3: Processing metrics")
            logger.info("=" * 50)
            metrics_dfs, successful_execution_metrics, failed_execution_metrics = pipeline.process_metrics(
                validated_data, args.run_date, dependencies, args.partition_info_table
            )
            
            partition_dt = datetime.now().strftime('%Y-%m-%d')
            logger.debug(f"Using partition_dt: {partition_dt}")
            
            logger.info("=" * 50)
            logger.info("STEP 4: Writing metrics to target tables")
            logger.info("=" * 50)
            successful_writes = {}
            failed_write_metrics = {}
            
            if metrics_dfs:
                logger.info(f"Found {len(metrics_dfs)} target tables with successful metrics to write")
                for target_table, df in metrics_dfs.items():
                    logger.info(f"Processing target table: {target_table}")
                    logger.debug(f"DataFrame for {target_table}: {df.count()} rows, {len(df.columns)} columns")
                    
                    logger.debug(f"Aligning schema for {target_table}")
                    aligned_df = pipeline.align_schema_with_bq(df, target_table)
                    
                    logger.info(f"Schema for {target_table}:")
                    aligned_df.printSchema()
                    aligned_df.show(truncate=False)
                    
                    logger.info(f"Writing to BigQuery table: {target_table}")
                    written_metric_ids, failed_metrics_for_table = pipeline.write_to_bq_with_overwrite(aligned_df, target_table)
                    
                    if written_metric_ids:
                        successful_writes[target_table] = written_metric_ids
                        logger.info(f"Successfully wrote {len(written_metric_ids)} metrics to {target_table}")
                    
                    if failed_metrics_for_table:
                        failed_write_metrics[target_table] = failed_metrics_for_table
                        logger.error(f"Failed to write {len(failed_metrics_for_table)} metrics to {target_table}")
            else:
                logger.warning("No metrics were successfully executed, skipping target table writes")
            
            logger.info("=" * 50)
            logger.info("STEP 5: Creating and writing reconciliation records")
            logger.info("=" * 50)
            recon_records = pipeline.create_recon_records_from_write_results(
                validated_data, args.run_date, dependencies, args.partition_info_table,
                args.env, successful_writes, failed_execution_metrics, failed_write_metrics, partition_dt
            )
            
            logger.debug(f"Writing {len(recon_records)} recon records to {args.recon_table}")
            pipeline.write_recon_to_bq(recon_records, args.recon_table)
            
            # Log comprehensive summary
            logger.info("=" * 50)
            logger.info("PIPELINE SUMMARY")
            logger.info("=" * 50)
            log_pipeline_summary(pipeline, successful_writes, failed_execution_metrics, failed_write_metrics)
            
            # Log quote conversion summary
            logger.info("=" * 50)
            logger.info("QUOTE CONVERSION SUMMARY")
            logger.info("=" * 50)
            pipeline.log_quote_conversion_summary()
            
            # Log recon summary
            logger.info("=" * 50)
            logger.info("RECONCILIATION SUMMARY")
            logger.info("=" * 50)
            if recon_records:
                logger.info(f"Total recon records created: {len(recon_records)}")
                success_count = sum(1 for r in recon_records if r.get('rcncln_exact_pass_in') == 'Passed')
                failed_count = len(recon_records) - success_count
                logger.info(f"Successful metric reconciliations: {success_count}")
                if failed_count > 0:
                    logger.info(f"Failed metric reconciliations: {failed_count}")
            else:
                logger.info("No recon records were created")
            
            # Final execution time
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            logger.info("=" * 80)
            logger.info(f"PIPELINE COMPLETED SUCCESSFULLY IN {execution_time:.2f} SECONDS")
            logger.info("=" * 80)
        
    except MetricsPipelineError as e:
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        logger.error("=" * 80)
        logger.error(f"PIPELINE FAILED AFTER {execution_time:.2f} SECONDS")
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 80)
        
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("Attempting to rollback processed metrics")
                pipeline.rollback_all_processed_metrics(partition_dt)
            except Exception as rollback_error:
                logger.error(f"Rollback failed: {str(rollback_error)}")
        
        sys.exit(1)
        
    except Exception as e:
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        logger.error("=" * 80)
        logger.error(f"UNEXPECTED ERROR AFTER {execution_time:.2f} SECONDS")
        logger.error(f"Error: {str(e)}")
        logger.error("=" * 80)
        
        if pipeline and pipeline.processed_metrics and partition_dt:
            try:
                logger.info("Attempting to rollback processed metrics")
                pipeline.rollback_all_processed_metrics(partition_dt)
            except Exception as rollback_error:
                logger.error(f"Rollback failed: {str(rollback_error)}")
        
        sys.exit(1)

if __name__ == "__main__":
    main()
