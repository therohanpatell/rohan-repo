from decimal import Decimal
from typing import Optional, Union

from .logging_utils import get_logger

logger = get_logger(__name__)

Numeric = Union[int, float, Decimal, str, None]


def normalize_numeric_value(value: Numeric) -> Optional[str]:
    """Return a *string* representation of *value* while preserving precision.

    This helper ensures all numeric values are serialized consistently before
    being written to BigQuery, preventing scientific notation and precision
    loss for large numbers.
    """
    if value is None:
        return None

    try:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (int, float)):
            return str(Decimal(str(value)))
        if isinstance(value, str):
            return str(Decimal(value))
        # Fallback: attempt casting through ``str`` then ``Decimal``
        return str(Decimal(str(value)))
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not normalize numeric value %s: %s", value, exc)
        return None


def safe_decimal_conversion(value: Optional[str]) -> Optional[Decimal]:
    """Convert a *string* to :class:`decimal.Decimal` with graceful failure."""
    if value is None:
        return None
    try:
        return Decimal(value)
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not convert %s to Decimal: %s", value, exc)
        return None