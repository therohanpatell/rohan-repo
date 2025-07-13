# metrics_pipeline/exceptions.py
"""Custom exceptions for the metrics pipeline."""

class MetricsPipelineError(Exception):
    """Base exception for all pipeline errors."""
    pass

class GCSOperationError(MetricsPipelineError):
    """Exception for GCS-related operations."""
    pass

class BigQueryOperationError(MetricsPipelineError):
    """Exception for BigQuery-related operations."""
    pass

class ValidationError(MetricsPipelineError):
    """Exception for validation failures."""
    pass

class RollbackError(MetricsPipelineError):
    """Exception for rollback operations."""
    pass
