"""S3 staging path utilities following project conventions."""

import uuid
from datetime import datetime, timezone
from typing import Optional


def get_staging_path(
    source: str,
    dt: Optional[datetime] = None,
    hour: Optional[int] = None,
    batch_id: Optional[str] = None,
) -> str:
    """Generate staging path following partition convention.
    
    Pattern: source=<source>/dt=YYYY-MM-DD/hour=HH/batch_id=<id>/
    
    Args:
        source: Data source name (e.g., 'api_a', 'weather_api')
        dt: Date for partition (defaults to UTC now)
        hour: Hour for partition (defaults to current hour)
        batch_id: Unique batch identifier (auto-generated if not provided)
        
    Returns:
        Staging path prefix (without bucket name)
        
    Example:
        >>> get_staging_path("api_a")
        'source=api_a/dt=2025-12-17/hour=15/batch_id=abc123/'
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    if hour is None:
        hour = dt.hour
    if batch_id is None:
        batch_id = uuid.uuid4().hex[:12]
    
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = f"{hour:02d}"
    
    return f"source={source}/dt={date_str}/hour={hour_str}/batch_id={batch_id}/"


def get_full_s3_path(
    bucket: str,
    source: str,
    dt: Optional[datetime] = None,
    hour: Optional[int] = None,
    batch_id: Optional[str] = None,
) -> str:
    """Generate full S3 path including bucket.
    
    Args:
        bucket: S3 bucket name
        source: Data source name
        dt: Date for partition
        hour: Hour for partition
        batch_id: Unique batch identifier
        
    Returns:
        Full S3 URI (s3://bucket/path/)
    """
    staging_path = get_staging_path(source, dt, hour, batch_id)
    return f"s3://{bucket}/{staging_path}"


def generate_filename(
    part_number: int = 1,
    extension: str = "jsonl",
) -> str:
    """Generate filename for staged data.
    
    Args:
        part_number: Part number for the file
        extension: File extension (jsonl, parquet)
        
    Returns:
        Filename like 'part-0001.jsonl'
    """
    return f"part-{part_number:04d}.{extension}"

