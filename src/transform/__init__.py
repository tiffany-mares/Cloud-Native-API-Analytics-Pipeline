"""Data transformation modules.

Handles:
- JSON flattening
- Schema normalization
- Type conversion
- Data validation
- Deduplication
"""

from .flatten import flatten_json, flatten_records
from .normalize import (
    normalize_timestamp,
    normalize_record,
    validate_required_fields,
    validate_records,
    dedupe_records,
    dedupe_by_id_updated,
    transform_records,
    ValidationResult,
    ValidationError,
)

__all__ = [
    # Flattening
    "flatten_json",
    "flatten_records",
    # Normalization
    "normalize_timestamp",
    "normalize_record",
    # Validation
    "validate_required_fields",
    "validate_records",
    "ValidationResult",
    "ValidationError",
    # Deduplication
    "dedupe_records",
    "dedupe_by_id_updated",
    # Full pipeline
    "transform_records",
]

