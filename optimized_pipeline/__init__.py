"""
Optimized modular metrics pipeline package.

This package provides a refactored implementation of the original `pysaprk.py`
pipeline with the following goals:
1. Avoid duplication of functionality across the codebase.
2. Clear separation of concerns (I/O, transformations, utilities).
3. Improved readability, testability, and maintainability.

All public primitives are exported through `__all__` for easy consumption.
"""

from .pipeline import MetricsPipeline
from .spark_utils import managed_spark_session
from .exceptions import MetricsPipelineError

__all__ = [
    "MetricsPipeline",
    "managed_spark_session",
    "MetricsPipelineError",
]