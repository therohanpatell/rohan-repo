"""SQL placeholder management utilities for the metrics pipeline"""

import re
from typing import List, Tuple, Optional
from ..config.constants import PLACEHOLDER_PATTERNS
from ..bigquery.client import BigQueryClient
from ..utils.exceptions import SQLExecutionError
from ..utils.logging import LoggerMixin


class PlaceholderManager(LoggerMixin):
    """Manager for SQL placeholder operations"""
    
    def __init__(self, bq_client: BigQueryClient):
        super().__init__()
        self.bq_client = bq_client
        self.patterns = PLACEHOLDER_PATTERNS
    
    def find_placeholder_positions(self, sql: str) -> List[Tuple[str, int, int]]:
        """
        Find all {currently} and {partition_info} placeholders in SQL with their positions
        
        Args:
            sql: SQL query string
            
        Returns:
            List of tuples (placeholder_type, start_pos, end_pos)
        """
        placeholders = []
        
        # Find {currently} placeholders
        for match in self.patterns['currently'].finditer(sql):
            placeholders.append(('currently', match.start(), match.end()))
        
        # Find {partition_info} placeholders
        for match in self.patterns['partition_info'].finditer(sql):
            placeholders.append(('partition_info', match.start(), match.end()))
        
        # Sort by position for consistent replacement
        placeholders.sort(key=lambda x: x[1])
        
        return placeholders
    
    def get_table_for_placeholder(self, sql: str, placeholder_pos: int) -> Optional[Tuple[str, str]]:
        """
        Find the table associated with a placeholder based on its position in the SQL
        
        Args:
            sql: SQL query string
            placeholder_pos: Position of the placeholder in the SQL
            
        Returns:
            Tuple (dataset, table_name) or None if not found
        """
        # Find the table reference that comes before this placeholder
        best_table = None
        best_distance = float('inf')
        
        for match in self.patterns['table_reference'].finditer(sql):
            table_end_pos = match.end()
            
            # Check if this table comes before the placeholder
            if table_end_pos < placeholder_pos:
                distance = placeholder_pos - table_end_pos
                if distance < best_distance:
                    best_distance = distance
                    project, dataset, table = match.groups()
                    best_table = (dataset, table)
        
        return best_table
    
    def replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str) -> str:
        """
        Replace {currently} and {partition_info} placeholders in SQL with appropriate dates
        
        Args:
            sql: SQL query string with placeholders
            run_date: CLI provided run date
            partition_info_table: Metadata table name
            
        Returns:
            SQL with all placeholders replaced
        """
        try:
            # Find all placeholders
            placeholders = self.find_placeholder_positions(sql)
            
            if not placeholders:
                self.log_info("No placeholders found in SQL query")
                return sql
            
            self.log_info(f"Found {len(placeholders)} placeholders in SQL: {[p[0] for p in placeholders]}")
            
            # Process replacements from end to beginning to preserve positions
            final_sql = sql
            
            for placeholder_type, start_pos, end_pos in reversed(placeholders):
                replacement_date = self._get_replacement_date(
                    placeholder_type, 
                    sql, 
                    start_pos, 
                    run_date, 
                    partition_info_table
                )
                
                # Replace the placeholder with the date
                final_sql = final_sql[:start_pos] + f"'{replacement_date}'" + final_sql[end_pos:]
            
            self.log_info(f"Successfully replaced {len(placeholders)} placeholders in SQL")
            self.log_debug(f"Final SQL after placeholder replacement: {final_sql}")
            
            return final_sql
            
        except Exception as e:
            error_msg = f"Failed to replace SQL placeholders: {str(e)}"
            self.log_error(error_msg)
            raise SQLExecutionError(error_msg)
    
    def _get_replacement_date(self, placeholder_type: str, sql: str, start_pos: int, 
                            run_date: str, partition_info_table: str) -> str:
        """
        Get the replacement date for a specific placeholder
        
        Args:
            placeholder_type: Type of placeholder ('currently' or 'partition_info')
            sql: SQL query string
            start_pos: Position of the placeholder
            run_date: CLI provided run date
            partition_info_table: Metadata table name
            
        Returns:
            Replacement date string
        """
        if placeholder_type == 'currently':
            replacement_date = run_date
            self.log_info(f"Replacing {{currently}} placeholder with run_date: {replacement_date}")
            return replacement_date
        
        elif placeholder_type == 'partition_info':
            # Find the table associated with this placeholder
            table_info = self.get_table_for_placeholder(sql, start_pos)
            
            if table_info:
                dataset, table_name = table_info
                replacement_date = self.bq_client.get_partition_info(dataset, table_name, partition_info_table)
                
                if not replacement_date:
                    raise SQLExecutionError(
                        f"Could not determine partition_dt for table {dataset}.{table_name}"
                    )
                
                self.log_info(f"Replacing {{partition_info}} placeholder with partition_dt: {replacement_date} for table {dataset}.{table_name}")
                return replacement_date
            else:
                raise SQLExecutionError(
                    f"Could not find table reference for {{partition_info}} placeholder at position {start_pos}"
                )
        
        else:
            raise SQLExecutionError(f"Unknown placeholder type: {placeholder_type}")
    
    def validate_placeholders(self, sql: str) -> bool:
        """
        Validate that all placeholders in SQL are supported
        
        Args:
            sql: SQL query string
            
        Returns:
            True if all placeholders are valid, False otherwise
        """
        try:
            # Find all placeholder-like patterns
            all_placeholders = re.findall(r'\{([^}]+)\}', sql)
            
            supported_placeholders = {'currently', 'partition_info'}
            unsupported_placeholders = set(all_placeholders) - supported_placeholders
            
            if unsupported_placeholders:
                self.log_error(f"Unsupported placeholders found: {unsupported_placeholders}")
                return False
            
            self.log_debug(f"All placeholders are supported: {all_placeholders}")
            return True
            
        except Exception as e:
            self.log_error(f"Failed to validate placeholders: {str(e)}")
            return False
    
    def get_placeholder_summary(self, sql: str) -> dict:
        """
        Get a summary of placeholders in the SQL
        
        Args:
            sql: SQL query string
            
        Returns:
            Dictionary with placeholder summary
        """
        placeholders = self.find_placeholder_positions(sql)
        
        summary = {
            'total_placeholders': len(placeholders),
            'currently_count': len([p for p in placeholders if p[0] == 'currently']),
            'partition_info_count': len([p for p in placeholders if p[0] == 'partition_info']),
            'positions': placeholders
        }
        
        return summary