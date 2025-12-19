"""Data normalization utilities."""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Common timestamp formats to try parsing
TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
]


def normalize_timestamp(
    value: Any,
    output_format: str = "%Y-%m-%dT%H:%M:%S.%fZ",
) -> Optional[str]:
    """Normalize timestamp to ISO 8601 format.
    
    Args:
        value: Timestamp value (string, int, or datetime)
        output_format: Desired output format
        
    Returns:
        Normalized timestamp string or None if parsing fails
    """
    if value is None:
        return None
    
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.strftime(output_format)
    
    if isinstance(value, (int, float)):
        # Assume Unix timestamp
        # Check if milliseconds (> year 3000 in seconds)
        if value > 32503680000:
            value = value / 1000
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
        return dt.strftime(output_format)
    
    if isinstance(value, str):
        # Try each format
        for fmt in TIMESTAMP_FORMATS:
            try:
                dt = datetime.strptime(value, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.strftime(output_format)
            except ValueError:
                continue
        
        logger.warning(f"Could not parse timestamp: {value}")
        return None
    
    return None


def normalize_string(value: Any) -> Optional[str]:
    """Normalize string value."""
    if value is None:
        return None
    
    if isinstance(value, str):
        # Strip whitespace and normalize unicode
        return value.strip()
    
    return str(value)


def normalize_key(key: str) -> str:
    """Normalize a key name for consistency.
    
    Converts to snake_case, removes special characters.
    """
    # Replace hyphens and spaces with underscores
    key = re.sub(r"[-\s]+", "_", key)
    # Remove non-alphanumeric characters (except underscore)
    key = re.sub(r"[^a-zA-Z0-9_]", "", key)
    # Convert camelCase to snake_case
    key = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key)
    # Lowercase and remove duplicate underscores
    key = re.sub(r"_+", "_", key.lower())
    # Remove leading/trailing underscores
    return key.strip("_")


def normalize_record(
    record: dict,
    timestamp_fields: Optional[list[str]] = None,
    normalize_keys: bool = True,
) -> dict:
    """Normalize a data record.
    
    Args:
        record: The record to normalize
        timestamp_fields: List of field names to treat as timestamps
        normalize_keys: Whether to normalize key names to snake_case
        
    Returns:
        Normalized record
    """
    timestamp_fields = timestamp_fields or []
    normalized = {}
    
    for key, value in record.items():
        # Normalize key if requested
        new_key = normalize_key(key) if normalize_keys else key
        
        # Normalize timestamp fields
        if key in timestamp_fields or new_key in timestamp_fields:
            normalized[new_key] = normalize_timestamp(value)
        elif isinstance(value, str):
            normalized[new_key] = normalize_string(value)
        elif isinstance(value, dict):
            # Recursively normalize nested dicts
            normalized[new_key] = normalize_record(
                value,
                timestamp_fields=timestamp_fields,
                normalize_keys=normalize_keys,
            )
        elif isinstance(value, list):
            # Normalize list items if they're dicts
            normalized[new_key] = [
                normalize_record(item, timestamp_fields, normalize_keys)
                if isinstance(item, dict)
                else item
                for item in value
            ]
        else:
            normalized[new_key] = value
    
    return normalized

