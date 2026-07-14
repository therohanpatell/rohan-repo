"""GCS validation utilities for the metrics pipeline"""

from typing import List, Dict
from pyspark.sql import SparkSession
from ..config.constants import ERROR_MESSAGES
from ..utils.exceptions import GCSError
from ..utils.logging import LoggerMixin


class GcsValidator(LoggerMixin):
    """Validator for GCS operations"""
    
    def __init__(self, spark: SparkSession):
        super().__init__()
        self.spark = spark
    
    def validate_gcs_path(self, gcs_path: str) -> str:
        """
        Validate GCS path format and accessibility
        
        Args:
            gcs_path: GCS path to validate
            
        Returns:
            Validated GCS path
            
        Raises:
            GCSError: If path is invalid or inaccessible
        """
        # Check basic format
        if not gcs_path.startswith('gs://'):
            raise GCSError(ERROR_MESSAGES['invalid_gcs_path'].format(path=gcs_path))
        
        # Check path structure
        path_parts = gcs_path.replace('gs://', '').split('/')
        if len(path_parts) < 2:
            raise GCSError(ERROR_MESSAGES['invalid_path_structure'].format(path=gcs_path))
        
        # Test accessibility by attempting to read file info
        try:
            # Try to read just the schema/structure without loading data
            test_df = self.spark.read.option("multiline", "true").json(gcs_path).limit(0)
            # Force execution to actually check if file exists
            test_df.count()  # This will fail if file doesn't exist or is inaccessible
            self.log_info(f"GCS path validated successfully: {gcs_path}")
            return gcs_path
        except Exception as e:
            raise GCSError(ERROR_MESSAGES['file_not_accessible'].format(path=gcs_path, error=str(e)))
    
    def read_json_from_gcs(self, gcs_path: str) -> List[Dict]:
        """
        Read JSON file from GCS and return as list of dictionaries
        
        Args:
            gcs_path: GCS path to JSON file
            
        Returns:
            List of metric definitions
            
        Raises:
            GCSError: If file cannot be read or parsed
        """
        try:
            # Validate GCS path first
            validated_path = self.validate_gcs_path(gcs_path)
            
            self.log_info(f"Reading JSON from GCS: {validated_path}")
            
            # Read JSON file using Spark
            df = self.spark.read.option("multiline", "true").json(validated_path)
            
            if df.count() == 0:
                raise GCSError(f"No data found in JSON file: {validated_path}")
            
            # Convert to list of dictionaries
            json_data = [row.asDict() for row in df.collect()]
            
            self.log_info(f"Successfully read {len(json_data)} records from JSON")
            return json_data
            
        except Exception as e:
            error_msg = f"Failed to read JSON from GCS: {str(e)}"
            self.log_error(error_msg)
            raise GCSError(error_msg)
    
    def validate_gcs_write_access(self, gcs_path: str) -> bool:
        """
        Validate write access to GCS path
        
        Args:
            gcs_path: GCS path to validate
            
        Returns:
            True if write access is available
        """
        try:
            # This is a basic validation - in a real implementation,
            # you might want to test actual write permissions
            return gcs_path.startswith('gs://') and len(gcs_path.split('/')) >= 3
        except Exception:
            return False