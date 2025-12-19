"""Main ingestion entrypoint: API → Transform → S3.

Usage:
    python -m src.ingest_to_s3
    python -m src.ingest_to_s3 --source api_a
    python -m src.ingest_to_s3 --source api_b --since 2025-01-01T00:00:00Z
"""

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv

from src.clients import ApiAClient, ApiBClient
from src.transform import transform_records
from src.utils import setup_logging, S3Writer

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


def ingest_api_a(
    batch_id: str,
    since: Optional[str] = None,
    s3_writer: Optional[S3Writer] = None,
) -> dict:
    """Ingest data from API A.
    
    Args:
        batch_id: Unique batch identifier
        since: Optional ISO timestamp for incremental fetch
        s3_writer: Optional S3Writer instance
        
    Returns:
        Ingestion result metadata
    """
    source = "api_a"
    logger.info(f"Starting ingestion for {source}", extra={"batch_id": batch_id})
    
    try:
        # Fetch from API
        client = ApiAClient()
        raw_records = client.fetch(since=since)
        
        if not raw_records:
            logger.warning(f"No records fetched from {source}")
            return {
                "source": source,
                "batch_id": batch_id,
                "status": "success",
                "records_fetched": 0,
                "records_staged": 0,
            }
        
        # Transform: normalize, validate, dedupe
        valid_records, invalid_records = transform_records(
            raw_records,
            required_fields=["id"],
            timestamp_fields=["created_at", "updated_at"],
            dedupe_key_fields=["id"],
            dedupe_sort_field="updated_at",
            normalize_keys=True,
        )
        
        if invalid_records:
            logger.warning(
                f"Found {len(invalid_records)} invalid records",
                extra={"source": source, "invalid_count": len(invalid_records)}
            )
        
        # Upload to S3
        writer = s3_writer or S3Writer()
        metadata = writer.write(valid_records, source=source, batch_id=batch_id)
        
        result = {
            "source": source,
            "batch_id": batch_id,
            "status": "success",
            "records_fetched": len(raw_records),
            "records_valid": len(valid_records),
            "records_invalid": len(invalid_records),
            "records_staged": metadata.get("record_count", 0),
            "s3_uri": metadata.get("s3_uri"),
            "file_size_bytes": metadata.get("file_size_bytes"),
        }
        
        logger.info(f"Completed ingestion for {source}", extra=result)
        return result
        
    except Exception as e:
        logger.error(f"Failed to ingest {source}: {e}", exc_info=True)
        return {
            "source": source,
            "batch_id": batch_id,
            "status": "error",
            "error": str(e),
        }


def ingest_api_b(
    batch_id: str,
    since: Optional[str] = None,
    s3_writer: Optional[S3Writer] = None,
) -> dict:
    """Ingest data from API B.
    
    Args:
        batch_id: Unique batch identifier
        since: Optional ISO timestamp for incremental fetch
        s3_writer: Optional S3Writer instance
        
    Returns:
        Ingestion result metadata
    """
    source = "api_b"
    logger.info(f"Starting ingestion for {source}", extra={"batch_id": batch_id})
    
    try:
        # Fetch from API
        client = ApiBClient()
        raw_records = client.fetch(since=since)
        
        if not raw_records:
            logger.warning(f"No records fetched from {source}")
            return {
                "source": source,
                "batch_id": batch_id,
                "status": "success",
                "records_fetched": 0,
                "records_staged": 0,
            }
        
        # Transform: normalize, validate, dedupe
        valid_records, invalid_records = transform_records(
            raw_records,
            required_fields=["id"],
            timestamp_fields=["created_at", "modified_at"],
            dedupe_key_fields=["id"],
            dedupe_sort_field="modified_at",
            normalize_keys=True,
        )
        
        if invalid_records:
            logger.warning(
                f"Found {len(invalid_records)} invalid records",
                extra={"source": source, "invalid_count": len(invalid_records)}
            )
        
        # Upload to S3
        writer = s3_writer or S3Writer()
        metadata = writer.write(valid_records, source=source, batch_id=batch_id)
        
        result = {
            "source": source,
            "batch_id": batch_id,
            "status": "success",
            "records_fetched": len(raw_records),
            "records_valid": len(valid_records),
            "records_invalid": len(invalid_records),
            "records_staged": metadata.get("record_count", 0),
            "s3_uri": metadata.get("s3_uri"),
            "file_size_bytes": metadata.get("file_size_bytes"),
        }
        
        logger.info(f"Completed ingestion for {source}", extra=result)
        return result
        
    except Exception as e:
        logger.error(f"Failed to ingest {source}: {e}", exc_info=True)
        return {
            "source": source,
            "batch_id": batch_id,
            "status": "error",
            "error": str(e),
        }


def run_ingestion(
    sources: Optional[list[str]] = None,
    since: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> dict:
    """Run ingestion for specified sources.
    
    Args:
        sources: List of sources to ingest (default: all)
        since: Optional ISO timestamp for incremental fetch
        batch_id: Optional batch ID (auto-generated if not provided)
        
    Returns:
        Combined results for all sources
    """
    # Generate batch ID
    if batch_id is None:
        batch_id = uuid.uuid4().hex[:12]
    
    # Default to all sources
    if sources is None:
        sources = ["api_a", "api_b"]
    
    start_time = datetime.now(timezone.utc)
    
    logger.info(
        "Starting ingestion run",
        extra={
            "batch_id": batch_id,
            "sources": sources,
            "since": since,
        }
    )
    
    # Create shared S3 writer
    s3_writer = S3Writer()
    
    # Run ingestion for each source
    results = {}
    
    if "api_a" in sources:
        results["api_a"] = ingest_api_a(batch_id, since, s3_writer)
    
    if "api_b" in sources:
        results["api_b"] = ingest_api_b(batch_id, since, s3_writer)
    
    end_time = datetime.now(timezone.utc)
    duration_seconds = (end_time - start_time).total_seconds()
    
    # Summarize results
    total_fetched = sum(r.get("records_fetched", 0) for r in results.values())
    total_staged = sum(r.get("records_staged", 0) for r in results.values())
    all_success = all(r.get("status") == "success" for r in results.values())
    
    summary = {
        "batch_id": batch_id,
        "status": "success" if all_success else "partial_failure",
        "sources": sources,
        "total_records_fetched": total_fetched,
        "total_records_staged": total_staged,
        "duration_seconds": duration_seconds,
        "started_at": start_time.isoformat(),
        "completed_at": end_time.isoformat(),
        "results": results,
    }
    
    logger.info(
        f"Ingestion run complete: {total_staged} records staged in {duration_seconds:.2f}s",
        extra=summary
    )
    
    return summary


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Ingest data from APIs to S3 staging"
    )
    parser.add_argument(
        "--source",
        choices=["api_a", "api_b", "all"],
        default="all",
        help="Source to ingest (default: all)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="ISO timestamp for incremental fetch (e.g., 2025-01-01T00:00:00Z)",
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="Batch ID (auto-generated if not provided)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Log level (default: INFO)",
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(level=args.log_level, json_format=True)
    
    # Determine sources
    sources = None if args.source == "all" else [args.source]
    
    # Run ingestion
    result = run_ingestion(
        sources=sources,
        since=args.since,
        batch_id=args.batch_id,
    )
    
    # Exit with error code if any failures
    if result["status"] != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()

