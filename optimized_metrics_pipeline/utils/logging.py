"""Logging utilities for the metrics pipeline"""

import logging
from typing import Optional
from ..config.settings import PipelineConfig


def setup_logging(config: PipelineConfig) -> None:
    """
    Setup logging configuration for the pipeline
    
    Args:
        config: Pipeline configuration
    """
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper()),
        format=config.log_format
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class LoggerMixin:
    """Mixin class to provide logging functionality"""
    
    def __init__(self, logger_name: Optional[str] = None):
        self.logger = get_logger(logger_name or self.__class__.__name__)
    
    def log_info(self, message: str, *args, **kwargs):
        """Log info message"""
        self.logger.info(message, *args, **kwargs)
    
    def log_warning(self, message: str, *args, **kwargs):
        """Log warning message"""
        self.logger.warning(message, *args, **kwargs)
    
    def log_error(self, message: str, *args, **kwargs):
        """Log error message"""
        self.logger.error(message, *args, **kwargs)
    
    def log_debug(self, message: str, *args, **kwargs):
        """Log debug message"""
        self.logger.debug(message, *args, **kwargs)