"""
Custom exceptions for the Metrics Pipeline
"""


class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    
    def __init__(self, message: str, metric_id: str = None):
        self.metric_id = metric_id  # Store metric ID for error tracking
        if metric_id:
            message = f"Metric '{metric_id}': {message}"  # Add metric context to error
        super().__init__(message)


class ValidationError(MetricsPipelineError):
    """Exception raised for data validation errors"""
    pass  # Inherits all functionality from MetricsPipelineError


class SQLExecutionError(MetricsPipelineError):
    """Exception raised for SQL execution errors"""
    pass  # Used for BigQuery query failures


class BigQueryError(MetricsPipelineError):
    """Exception raised for BigQuery operations errors"""
    pass  # Used for BigQuery connection/write failures


class GCSError(MetricsPipelineError):
    """Exception raised for GCS operations errors"""
    pass  # Used for GCS read/access failures


class SchemaValidationError(MetricsPipelineError):
    """Exception raised for schema validation errors"""
    pass  # Used for schema alignment and validation failures


class SchemaValidationError(MetricsPipelineError):
    """Exception raised for schema validation errors"""
    pass  # Used for schema alignment and validation failures