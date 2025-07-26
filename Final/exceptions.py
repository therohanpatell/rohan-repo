"""
Custom exceptions for the Metrics Pipeline
"""


class MetricsPipelineError(Exception):
    """Custom exception for pipeline errors"""
    
    def __init__(self, message: str, metric_id: str = None):
        self.metric_id = metric_id
        if metric_id:
            message = f"Metric '{metric_id}': {message}"
        super().__init__(message)


class ValidationError(MetricsPipelineError):
    """Exception raised for data validation errors"""
    pass


class SQLExecutionError(MetricsPipelineError):
    """Exception raised for SQL execution errors"""
    pass


class BigQueryError(MetricsPipelineError):
    """Exception raised for BigQuery operations errors"""
    pass


class GCSError(MetricsPipelineError):
    """Exception raised for GCS operations errors"""
    pass