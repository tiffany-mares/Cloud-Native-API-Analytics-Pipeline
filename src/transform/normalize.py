"""Data normalization, validation, and deduplication utilities."""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from src.transform.flatten import flatten_json

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


# ============================================
# Validation
# ============================================

@dataclass
class ValidationResult:
    """Result of record validation."""
    
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    record: Optional[dict] = None


class ValidationError(Exception):
    """Raised when validation fails."""
    
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__(f"Validation failed: {errors}")


def validate_required_fields(
    record: dict,
    required_fields: list[str],
) -> ValidationResult:
    """Validate that required fields are present and not null.
    
    Args:
        record: The record to validate
        required_fields: List of required field names
        
    Returns:
        ValidationResult with is_valid flag and any errors
    """
    errors = []
    
    for field_name in required_fields:
        if field_name not in record:
            errors.append(f"Missing required field: {field_name}")
        elif record[field_name] is None:
            errors.append(f"Null value for required field: {field_name}")
        elif isinstance(record[field_name], str) and not record[field_name].strip():
            errors.append(f"Empty value for required field: {field_name}")
    
    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        record=record if len(errors) == 0 else None,
    )


def validate_records(
    records: list[dict],
    required_fields: list[str],
    raise_on_error: bool = False,
) -> tuple[list[dict], list[dict]]:
    """Validate a list of records.
    
    Args:
        records: List of records to validate
        required_fields: List of required field names
        raise_on_error: If True, raise exception on first error
        
    Returns:
        Tuple of (valid_records, invalid_records)
        
    Raises:
        ValidationError: If raise_on_error=True and validation fails
    """
    valid_records = []
    invalid_records = []
    
    for i, record in enumerate(records):
        result = validate_required_fields(record, required_fields)
        
        if result.is_valid:
            valid_records.append(record)
        else:
            if raise_on_error:
                raise ValidationError(result.errors)
            
            # Add error info to invalid record
            invalid_record = {
                "_validation_errors": result.errors,
                "_record_index": i,
                **record,
            }
            invalid_records.append(invalid_record)
            
            logger.warning(
                f"Validation failed for record {i}",
                extra={"errors": result.errors, "record_index": i}
            )
    
    logger.info(
        f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid",
        extra={
            "valid_count": len(valid_records),
            "invalid_count": len(invalid_records),
        }
    )
    
    return valid_records, invalid_records


# ============================================
# Deduplication
# ============================================

def dedupe_records(
    records: list[dict],
    key_fields: list[str],
    sort_field: Optional[str] = None,
    keep: str = "last",
) -> list[dict]:
    """Deduplicate records by composite key.
    
    Args:
        records: List of records to deduplicate
        key_fields: Fields that form the unique key (e.g., ["id", "updated_at"])
        sort_field: Optional field to sort by before deduping
        keep: Which duplicate to keep - "first" or "last"
        
    Returns:
        Deduplicated list of records
        
    Example:
        >>> records = [
        ...     {"id": "1", "updated_at": "2025-01-01", "value": "old"},
        ...     {"id": "1", "updated_at": "2025-01-02", "value": "new"},
        ... ]
        >>> dedupe_records(records, key_fields=["id"], sort_field="updated_at", keep="last")
        [{"id": "1", "updated_at": "2025-01-02", "value": "new"}]
    """
    if not records:
        return []
    
    # Sort if sort_field provided
    if sort_field:
        records = sorted(
            records,
            key=lambda r: r.get(sort_field) or "",
            reverse=(keep == "last"),
        )
    
    seen = {}
    deduped = []
    
    for record in records:
        # Build composite key
        key_values = tuple(record.get(f) for f in key_fields)
        
        # Skip if any key field is None
        if None in key_values:
            logger.warning(
                f"Skipping record with null key field",
                extra={"key_fields": key_fields, "key_values": key_values}
            )
            continue
        
        if key_values not in seen:
            seen[key_values] = True
            deduped.append(record)
    
    # Reverse back if we sorted for "last"
    if sort_field and keep == "last":
        deduped = list(reversed(deduped))
    
    duplicate_count = len(records) - len(deduped)
    if duplicate_count > 0:
        logger.info(
            f"Removed {duplicate_count} duplicate records",
            extra={
                "original_count": len(records),
                "deduped_count": len(deduped),
                "duplicate_count": duplicate_count,
            }
        )
    
    return deduped


def dedupe_by_id_updated(
    records: list[dict],
    id_field: str = "id",
    updated_field: str = "updated_at",
) -> list[dict]:
    """Convenience function to dedupe by id + updated_at, keeping latest.
    
    This is the most common deduplication pattern:
    - Same ID with different updated_at → keep the latest
    - Ensures idempotency for incremental loads
    
    Args:
        records: List of records to deduplicate
        id_field: Name of the ID field
        updated_field: Name of the updated timestamp field
        
    Returns:
        Deduplicated list with latest version of each record
    """
    return dedupe_records(
        records=records,
        key_fields=[id_field],
        sort_field=updated_field,
        keep="last",
    )


# ============================================
# Full Transformation Pipeline
# ============================================

def transform_records(
    records: list[dict],
    required_fields: Optional[list[str]] = None,
    timestamp_fields: Optional[list[str]] = None,
    dedupe_key_fields: Optional[list[str]] = None,
    dedupe_sort_field: Optional[str] = None,
    flatten: bool = False,
    normalize_keys: bool = True,
) -> tuple[list[dict], list[dict]]:
    """Full transformation pipeline: normalize → validate → dedupe.
    
    Args:
        records: Raw records to transform
        required_fields: Fields required for validation
        timestamp_fields: Fields to normalize as timestamps
        dedupe_key_fields: Fields for deduplication key
        dedupe_sort_field: Field to sort by for deduplication
        flatten: Whether to flatten nested JSON
        normalize_keys: Whether to convert keys to snake_case
        
    Returns:
        Tuple of (valid_records, invalid_records)
        
    Example:
        >>> valid, invalid = transform_records(
        ...     records=raw_data,
        ...     required_fields=["id", "name"],
        ...     timestamp_fields=["created_at", "updated_at"],
        ...     dedupe_key_fields=["id"],
        ...     dedupe_sort_field="updated_at",
        ...     flatten=True,
        ... )
    """
    required_fields = required_fields or []
    timestamp_fields = timestamp_fields or []
    
    # Step 1: Flatten if requested
    if flatten:
        records = [flatten_json(r) for r in records]
        logger.debug(f"Flattened {len(records)} records")
    
    # Step 2: Normalize records
    normalized = [
        normalize_record(r, timestamp_fields=timestamp_fields, normalize_keys=normalize_keys)
        for r in records
    ]
    logger.debug(f"Normalized {len(normalized)} records")
    
    # Step 3: Validate required fields
    if required_fields:
        valid_records, invalid_records = validate_records(normalized, required_fields)
    else:
        valid_records = normalized
        invalid_records = []
    
    # Step 4: Deduplicate
    if dedupe_key_fields:
        valid_records = dedupe_records(
            valid_records,
            key_fields=dedupe_key_fields,
            sort_field=dedupe_sort_field,
            keep="last",
        )
    
    logger.info(
        f"Transformation complete",
        extra={
            "input_count": len(records),
            "valid_count": len(valid_records),
            "invalid_count": len(invalid_records),
        }
    )
    
    return valid_records, invalid_records

