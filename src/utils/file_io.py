"""File I/O utilities for staging data."""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

logger = logging.getLogger(__name__)


def get_staging_path(
    base_path: str,
    source: str,
    dt: Optional[datetime] = None,
    hour: Optional[int] = None,
) -> str:
    """Generate staging path following partition convention.
    
    Pattern: base_path/source=<name>/dt=YYYY-MM-DD/hour=HH/
    
    Args:
        base_path: Base staging directory or S3 prefix
        source: Data source name
        dt: Date for partition (defaults to now)
        hour: Hour for partition (defaults to current hour)
        
    Returns:
        Full staging path
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    if hour is None:
        hour = dt.hour
    
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = f"{hour:02d}"
    
    path = f"{base_path.rstrip('/')}/source={source}/dt={date_str}/hour={hour_str}"
    return path


def write_jsonl(
    records: list[dict],
    output_path: Union[str, Path],
    batch_id: Optional[str] = None,
    schema_version: str = "1.0",
) -> dict:
    """Write records to JSONL file with metadata.
    
    Args:
        records: List of records to write
        output_path: Output file path
        batch_id: Optional batch identifier (generated if not provided)
        schema_version: Schema version for metadata
        
    Returns:
        Metadata dict with file info
    """
    if batch_id is None:
        batch_id = str(uuid.uuid4())
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    extracted_at = datetime.now(timezone.utc).isoformat()
    
    # Write records
    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            # Add metadata to each record
            enriched_record = {
                "_batch_id": batch_id,
                "_extracted_at": extracted_at,
                "_schema_version": schema_version,
                **record,
            }
            f.write(json.dumps(enriched_record, default=str) + "\n")
    
    metadata = {
        "file_path": str(output_path),
        "batch_id": batch_id,
        "extracted_at": extracted_at,
        "schema_version": schema_version,
        "record_count": len(records),
        "file_size_bytes": output_path.stat().st_size,
    }
    
    logger.info(
        f"Wrote {len(records)} records to {output_path}",
        extra=metadata
    )
    
    return metadata


def write_parquet(
    records: list[dict],
    output_path: Union[str, Path],
    batch_id: Optional[str] = None,
    schema_version: str = "1.0",
) -> dict:
    """Write records to Parquet file with metadata.
    
    Requires pyarrow to be installed.
    
    Args:
        records: List of records to write
        output_path: Output file path
        batch_id: Optional batch identifier (generated if not provided)
        schema_version: Schema version for metadata
        
    Returns:
        Metadata dict with file info
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("pyarrow is required for Parquet support")
    
    if batch_id is None:
        batch_id = str(uuid.uuid4())
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    extracted_at = datetime.now(timezone.utc).isoformat()
    
    # Add metadata columns
    enriched_records = [
        {
            "_batch_id": batch_id,
            "_extracted_at": extracted_at,
            "_schema_version": schema_version,
            **record,
        }
        for record in records
    ]
    
    # Convert to PyArrow Table and write
    table = pa.Table.from_pylist(enriched_records)
    pq.write_table(table, output_path)
    
    metadata = {
        "file_path": str(output_path),
        "batch_id": batch_id,
        "extracted_at": extracted_at,
        "schema_version": schema_version,
        "record_count": len(records),
        "file_size_bytes": output_path.stat().st_size,
    }
    
    logger.info(
        f"Wrote {len(records)} records to Parquet at {output_path}",
        extra=metadata
    )
    
    return metadata


def read_jsonl(file_path: Union[str, Path]) -> list[dict]:
    """Read records from a JSONL file.
    
    Args:
        file_path: Path to JSONL file
        
    Returns:
        List of parsed records
    """
    records = []
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    
    logger.debug(f"Read {len(records)} records from {file_path}")
    return records

