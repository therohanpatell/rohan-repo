"""Custom exceptions for the metrics pipeline"""


class MetricsPipelineError(Exception):
    """Base exception for all pipeline-related errors"""
    
    def __init__(self, message: str, error_code: str = None, details: dict = None):
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}
    
    def __str__(self):
        if self.error_code:
            return f"[{self.error_code}] {super().__str__()}"
        return super().__str__()


class ValidationError(MetricsPipelineError):
    """Exception raised during data validation"""
    pass


class SQLExecutionError(MetricsPipelineError):
    """Exception raised during SQL execution"""
    pass


class BigQueryError(MetricsPipelineError):
    """Exception raised during BigQuery operations"""
    pass


class GCSError(MetricsPipelineError):
    """Exception raised during GCS operations"""
    pass


class RollbackError(MetricsPipelineError):
    """Exception raised during rollback operations"""
    pass