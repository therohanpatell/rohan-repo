"""Schema management utilities for BigQuery operations"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import TimestampType, DecimalType, DoubleType, DateType
from google.cloud import bigquery
from .client import BigQueryClient
from ..utils.logging import LoggerMixin


class SchemaManager(LoggerMixin):
    """Manager for BigQuery schema operations"""
    
    def __init__(self, bq_client: BigQueryClient):
        super().__init__()
        self.bq_client = bq_client
    
    def align_dataframe_with_bq_schema(self, df: DataFrame, target_table: str) -> DataFrame:
        """
        Align Spark DataFrame with BigQuery table schema
        
        Args:
            df: Spark DataFrame
            target_table: BigQuery table name
            
        Returns:
            Schema-aligned DataFrame
        """
        self.log_info(f"Aligning DataFrame schema with BigQuery table: {target_table}")
        
        # Get BigQuery schema
        bq_schema = self.bq_client.get_table_schema(target_table)
        
        # Get current DataFrame columns
        current_columns = df.columns
        
        # Build list of columns in BigQuery schema order
        bq_columns = [field.name for field in bq_schema]
        
        # Drop extra columns not in BigQuery schema
        columns_to_keep = [col for col in current_columns if col in bq_columns]
        columns_to_drop = [col for col in current_columns if col not in bq_columns]
        
        if columns_to_drop:
            self.log_info(f"Dropping extra columns: {columns_to_drop}")
            df = df.drop(*columns_to_drop)
        
        # Reorder columns to match BigQuery schema
        df = df.select(*[col(c) for c in bq_columns if c in columns_to_keep])
        
        # Handle type conversions for BigQuery compatibility
        df = self._apply_type_conversions(df, bq_schema)
        
        self.log_info(f"Schema alignment complete. Final columns: {df.columns}")
        return df
    
    def _apply_type_conversions(self, df: DataFrame, bq_schema: List[bigquery.SchemaField]) -> DataFrame:
        """
        Apply type conversions to match BigQuery schema
        
        Args:
            df: Spark DataFrame
            bq_schema: BigQuery schema fields
            
        Returns:
            DataFrame with converted types
        """
        for field in bq_schema:
            if field.name in df.columns:
                if field.field_type == 'DATE':
                    df = df.withColumn(field.name, to_date(col(field.name)))
                elif field.field_type == 'TIMESTAMP':
                    df = df.withColumn(field.name, col(field.name).cast(TimestampType()))
                elif field.field_type == 'NUMERIC':
                    df = df.withColumn(field.name, col(field.name).cast(DecimalType(38, 9)))
                elif field.field_type == 'FLOAT':
                    df = df.withColumn(field.name, col(field.name).cast(DoubleType()))
        
        return df
    
    def validate_schema_compatibility(self, df: DataFrame, target_table: str) -> bool:
        """
        Validate if DataFrame schema is compatible with BigQuery table
        
        Args:
            df: Spark DataFrame
            target_table: BigQuery table name
            
        Returns:
            True if compatible, False otherwise
        """
        try:
            bq_schema = self.bq_client.get_table_schema(target_table)
            bq_columns = set(field.name for field in bq_schema)
            df_columns = set(df.columns)
            
            # Check if all required columns are present
            required_columns = {field.name for field in bq_schema if field.mode == 'REQUIRED'}
            missing_required = required_columns - df_columns
            
            if missing_required:
                self.log_error(f"Missing required columns: {missing_required}")
                return False
            
            # Check for extra columns that will be dropped
            extra_columns = df_columns - bq_columns
            if extra_columns:
                self.log_warning(f"Extra columns that will be dropped: {extra_columns}")
            
            self.log_info("Schema compatibility validation passed")
            return True
            
        except Exception as e:
            self.log_error(f"Schema compatibility validation failed: {str(e)}")
            return False
    
    def get_table_column_info(self, target_table: str) -> dict:
        """
        Get detailed column information for a BigQuery table
        
        Args:
            target_table: BigQuery table name
            
        Returns:
            Dictionary with column information
        """
        try:
            bq_schema = self.bq_client.get_table_schema(target_table)
            
            column_info = {}
            for field in bq_schema:
                column_info[field.name] = {
                    'type': field.field_type,
                    'mode': field.mode,
                    'description': field.description
                }
            
            return column_info
            
        except Exception as e:
            self.log_error(f"Failed to get table column info: {str(e)}")
            return {}
    
    def suggest_schema_fixes(self, df: DataFrame, target_table: str) -> List[str]:
        """
        Suggest fixes for schema alignment issues
        
        Args:
            df: Spark DataFrame
            target_table: BigQuery table name
            
        Returns:
            List of suggested fixes
        """
        suggestions = []
        
        try:
            bq_schema = self.bq_client.get_table_schema(target_table)
            bq_columns = {field.name: field for field in bq_schema}
            df_columns = set(df.columns)
            
            # Check for missing required columns
            required_columns = {field.name for field in bq_schema if field.mode == 'REQUIRED'}
            missing_required = required_columns - df_columns
            
            for col_name in missing_required:
                suggestions.append(f"Add required column: {col_name}")
            
            # Check for type mismatches (basic check)
            for col_name in df.columns:
                if col_name in bq_columns:
                    bq_field = bq_columns[col_name]
                    df_field = df.schema[col_name]
                    
                    # Add type conversion suggestions based on field types
                    if bq_field.field_type == 'NUMERIC' and 'decimal' not in str(df_field.dataType).lower():
                        suggestions.append(f"Convert {col_name} to DECIMAL type")
                    elif bq_field.field_type == 'TIMESTAMP' and 'timestamp' not in str(df_field.dataType).lower():
                        suggestions.append(f"Convert {col_name} to TIMESTAMP type")
            
            return suggestions
            
        except Exception as e:
            self.log_error(f"Failed to generate schema suggestions: {str(e)}")
            return ["Error analyzing schema - check logs for details"]