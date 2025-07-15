from datetime import datetime

from .exceptions import MetricsPipelineError


def validate_date_format(date_str: str, *, fmt: str = "%Y-%m-%d") -> None:
    """Validate that *date_str* matches *fmt*.

    Raises
    ------
    MetricsPipelineError
        If the string does not conform to the expected format.
    """
    try:
        datetime.strptime(date_str, fmt)
    except ValueError as exc:  # pragma: no cover
        raise MetricsPipelineError(f"Invalid date format: {date_str}. Expected {fmt}") from exc