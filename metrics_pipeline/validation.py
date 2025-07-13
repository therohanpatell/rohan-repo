# metrics_pipeline/validation.py
"""Validation functions for the metrics pipeline."""

import logging
from typing import List, Dict, Any, Set

from .exceptions import ValidationError

logger = logging.getLogger(__name__)

def validate_json(
    json_data: List[Dict[str, Any]],
    required_fields: List[str] = None
) -> List[Dict[str, Any]]:
    """Validate JSON data for required fields and duplicates.

    Args:
        json_data: List of metric definitions
        required_fields: List of required field names (default includes common fields)

    Returns:
        List of validated metric definitions

    Raises:
        ValidationError: If validation fails
    """
    if required_fields is None:
        required_fields = [
            'metric_id', 'metric_name', 'metric_type',
            'sql', 'dependency', 'partition_mode'
        ]

    logger.info("Validating JSON data")

    # Track metric IDs to check for duplicates
    metric_ids = set()

    for i, record in enumerate(json_data):
        # Check for required fields
        for field in required_fields:
            if field not in record:
                raise ValidationError(
                    f"Record {i}: Missing required field '{field}'"
                )

            value = record[field]
            # Enhanced validation for empty/whitespace-only strings
            if value is None or (isinstance(value, str) and value.strip() == ""):
                raise ValidationError(
                    f"Record {i}: Field '{field}' is null, empty, or contains only whitespace"
                )

        # Check for duplicate metric IDs
        metric_id = record['metric_id'].strip()
        if metric_id in metric_ids:
            raise ValidationError(
                f"Record {i}: Duplicate metric_id '{metric_id}' found"
            )
        metric_ids.add(metric_id)

        # Validate partition_mode values
        partition_mode = record['partition_mode'].strip()
        if not partition_mode:
            raise ValidationError(
                f"Record {i}: partition_mode cannot be empty"
            )

        partition_modes = [mode.strip() for mode in partition_mode.split('|')]
        valid_modes = {'currently', 'partition_info'}

        for mode in partition_modes:
            if not mode:  # Empty mode after split
                raise ValidationError(
                    f"Record {i}: Empty partition_mode found in '{partition_mode}'"
                )
            if mode not in valid_modes:
                raise ValidationError(
                    f"Record {i}: Invalid partition_mode '{mode}'. "
                    f"Must be 'currently' or 'partition_info'"
                )

    logger.info("Successfully validated %d records with %d unique metric IDs",
               len(json_data), len(metric_ids))
    return json_data

def check_dependencies_exist(
    json_data: List[Dict[str, Any]],
    dependencies: List[str]
) -> None:
    """Check if all specified dependencies exist in the JSON data.

    Args:
        json_data: List of metric definitions
        dependencies: List of dependencies to check

    Raises:
        ValidationError: If any dependency is missing
    """
    available_dependencies = set(record['dependency'] for record in json_data)
    missing_dependencies = set(dependencies) - available_dependencies

    if missing_dependencies:
        raise ValidationError(
            f"Missing dependencies in JSON data: {missing_dependencies}. "
            f"Available dependencies: {available_dependencies}"
        )

    logger.info("All dependencies found: %s", dependencies)
