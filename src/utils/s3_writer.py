"""S3 writer for staging data files."""

import json
import logging
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Writer:
    """Write JSONL files to S3 staging bucket.
    
    Follows staging path convention:
    source=<source>/dt=YYYY-MM-DD/hour=HH/batch_id=<id>/part-0001.jsonl
    """
    
    def __init__(
        self,
        bucket: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        """Initialize S3 writer.
        
        Args:
            bucket: S3 bucket name (or from env: S3_BUCKET)
            aws_access_key_id: AWS access key (or from env)
            aws_secret_access_key: AWS secret key (or from env)
            region_name: AWS region (or from env: AWS_REGION)
        """
        self.bucket = bucket or os.getenv("S3_BUCKET")
        if not self.bucket:
            raise ValueError("S3_BUCKET is required")
        
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=region_name or os.getenv("AWS_REGION", "us-east-1"),
        )
    
    def _build_staging_path(
        self,
        source: str,
        batch_id: str,
        dt: Optional[datetime] = None,
    ) -> str:
        """Build staging path following convention.
        
        Pattern: source=<source>/dt=YYYY-MM-DD/hour=HH/batch_id=<id>/part-0001.jsonl
        
        Args:
            source: Data source name (e.g., 'api_a', 'api_b')
            batch_id: Unique batch identifier
            dt: Datetime for partitioning (defaults to UTC now)
            
        Returns:
            Full S3 key path
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = f"{dt.hour:02d}"
        
        path = f"source={source}/dt={date_str}/hour={hour_str}/batch_id={batch_id}/part-0001.jsonl"
        return path
    
    def _write_jsonl_local(
        self,
        records: list[dict],
        file_path: Path,
        batch_id: str,
        source: str,
    ) -> int:
        """Write records to local JSONL file.
        
        Args:
            records: List of records to write
            file_path: Local file path
            batch_id: Batch identifier for metadata
            source: Source identifier for metadata
            
        Returns:
            Number of bytes written
        """
        extracted_at = datetime.now(timezone.utc).isoformat()
        
        with open(file_path, "w", encoding="utf-8") as f:
            for record in records:
                # Add pipeline metadata
                enriched = {
                    "_batch_id": batch_id,
                    "_source": source,
                    "_extracted_at": extracted_at,
                    **record,
                }
                f.write(json.dumps(enriched, default=str) + "\n")
        
        return file_path.stat().st_size
    
    def _upload_to_s3(
        self,
        local_path: Path,
        s3_key: str,
    ) -> str:
        """Upload local file to S3.
        
        Args:
            local_path: Path to local file
            s3_key: S3 object key
            
        Returns:
            S3 URI of uploaded file
        """
        try:
            self.s3_client.upload_file(
                str(local_path),
                self.bucket,
                s3_key,
                ExtraArgs={"ContentType": "application/jsonl"},
            )
            
            s3_uri = f"s3://{self.bucket}/{s3_key}"
            logger.info(f"Uploaded to {s3_uri}")
            return s3_uri
            
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise
    
    def write(
        self,
        records: list[dict],
        source: str,
        batch_id: Optional[str] = None,
        dt: Optional[datetime] = None,
    ) -> dict:
        """Write records to S3 as JSONL.
        
        Args:
            records: List of records to write
            source: Data source name (e.g., 'api_a', 'api_b')
            batch_id: Optional batch ID (auto-generated if not provided)
            dt: Optional datetime for path partitioning
            
        Returns:
            Metadata dict with file info:
            {
                "s3_uri": "s3://bucket/path/file.jsonl",
                "s3_key": "source=api_a/dt=.../part-0001.jsonl",
                "bucket": "bucket-name",
                "batch_id": "abc123",
                "source": "api_a",
                "record_count": 100,
                "file_size_bytes": 12345,
                "extracted_at": "2025-01-15T10:00:00Z"
            }
        """
        if not records:
            logger.warning(f"No records to write for source={source}")
            return {
                "s3_uri": None,
                "record_count": 0,
                "source": source,
                "batch_id": batch_id,
            }
        
        # Generate batch ID if not provided
        if batch_id is None:
            batch_id = uuid.uuid4().hex[:12]
        
        # Build S3 key
        s3_key = self._build_staging_path(source, batch_id, dt)
        
        # Write to temp file then upload
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / "data.jsonl"
            
            # Write JSONL locally
            file_size = self._write_jsonl_local(records, local_path, batch_id, source)
            
            logger.info(
                f"Wrote {len(records)} records ({file_size} bytes) to local file",
                extra={
                    "source": source,
                    "batch_id": batch_id,
                    "record_count": len(records),
                    "file_size_bytes": file_size,
                }
            )
            
            # Upload to S3
            s3_uri = self._upload_to_s3(local_path, s3_key)
        
        metadata = {
            "s3_uri": s3_uri,
            "s3_key": s3_key,
            "bucket": self.bucket,
            "batch_id": batch_id,
            "source": source,
            "record_count": len(records),
            "file_size_bytes": file_size,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }
        
        logger.info(
            f"Successfully staged {len(records)} records to S3",
            extra=metadata
        )
        
        return metadata


def write_to_s3(
    records: list[dict],
    source: str,
    batch_id: Optional[str] = None,
) -> dict:
    """Convenience function to write records to S3.
    
    Uses environment variables for configuration.
    
    Args:
        records: List of records to write
        source: Data source name
        batch_id: Optional batch identifier
        
    Returns:
        Metadata dict with file info
    """
    writer = S3Writer()
    return writer.write(records, source, batch_id)

