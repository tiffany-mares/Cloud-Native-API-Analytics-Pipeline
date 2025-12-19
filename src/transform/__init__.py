"""Data transformation modules.

Handles:
- JSON flattening
- Schema normalization
- Type conversion
- Data validation
"""

from .flatten import flatten_json, flatten_records
from .normalize import normalize_timestamp, normalize_record

__all__ = [
    "flatten_json",
    "flatten_records",
    "normalize_timestamp",
    "normalize_record",
]

