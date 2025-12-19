"""JSON flattening utilities."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def flatten_json(
    nested_dict: dict,
    parent_key: str = "",
    separator: str = "_",
    max_depth: int = 10,
) -> dict:
    """Flatten a nested JSON dictionary.
    
    Args:
        nested_dict: The nested dictionary to flatten
        parent_key: Prefix for flattened keys
        separator: Separator between nested key levels
        max_depth: Maximum nesting depth to flatten
        
    Returns:
        Flattened dictionary with concatenated keys
        
    Example:
        >>> flatten_json({"a": {"b": 1, "c": {"d": 2}}})
        {"a_b": 1, "a_c_d": 2}
    """
    items: list[tuple[str, Any]] = []
    
    for key, value in nested_dict.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        
        if isinstance(value, dict) and max_depth > 0:
            items.extend(
                flatten_json(
                    value,
                    parent_key=new_key,
                    separator=separator,
                    max_depth=max_depth - 1,
                ).items()
            )
        elif isinstance(value, list):
            # Keep lists as-is (they'll be stored as arrays in Snowflake)
            items.append((new_key, value))
        else:
            items.append((new_key, value))
    
    return dict(items)


def flatten_records(
    records: list[dict],
    separator: str = "_",
    max_depth: int = 10,
) -> list[dict]:
    """Flatten a list of nested JSON records.
    
    Args:
        records: List of nested dictionaries
        separator: Separator between nested key levels
        max_depth: Maximum nesting depth to flatten
        
    Returns:
        List of flattened dictionaries
    """
    flattened = []
    
    for i, record in enumerate(records):
        try:
            flattened.append(
                flatten_json(record, separator=separator, max_depth=max_depth)
            )
        except Exception as e:
            logger.error(
                f"Error flattening record at index {i}: {e}",
                extra={"record_index": i, "error": str(e)}
            )
            raise
    
    logger.debug(f"Flattened {len(flattened)} records")
    return flattened

