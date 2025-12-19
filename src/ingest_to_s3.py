"""Main ingestion entrypoint: API → Transform → S3.

Usage:
    python -m src.ingest_to_s3
    python -m src.ingest_to_s3 --source api_a
    python -m src.ingest_to_s3 --source api_b --since 2025-01-01T00:00:00Z
"""

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv

from src.clients import ApiAClient, ApiBClient
from src.transform import transform_records
from src.utils import setup_logging, S3Writer
from src.utils.pipeline_logger import PipelineLogger, timed_operation

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
    plog = PipelineLogger(source=source, batch_id=batch_id)
    plog.start("ingestion")
    
    logger.info(
        "Starting ingestion",
        extra={
            "source": source,
            "batch_id": batch_id,
            "since": since,
            "step": "start",
        }
    )
    
    try:
        # Fetch from API with timing
        with timed_operation("api_fetch", logger) as fetch_timer:
            client = ApiAClient()
            raw_records = client.fetch(since=since)
        
        # Log API request metrics
        api_metrics = client.metrics.to_dict()
        logger.info(
            "API fetch completed",
            extra={
                "source": source,
                "batch_id": batch_id,
                "step": "api_fetch",
                "row_count": len(raw_records),
                "duration_ms": round(fetch_timer.duration_ms, 2),
                "retry_count": api_metrics["total_retries"],
                "api_metrics": api_metrics,
            }
        )
        
        if not raw_records:
            plog.success("ingestion", row_count=0)
            return {
                "source": source,
                "batch_id": batch_id,
                "status": "success",
                "records_fetched": 0,
                "records_staged": 0,
                "api_metrics": api_metrics,
            }
        
        # Transform with timing
        with timed_operation("transform", logger) as transform_timer:
            valid_records, invalid_records = transform_records(
                raw_records,
                required_fields=["id"],
                timestamp_fields=["created_at", "updated_at"],
                dedupe_key_fields=["id"],
                dedupe_sort_field="updated_at",
                normalize_keys=True,
            )
        
        plog.log_transform(
            input_count=len(raw_records),
            output_count=len(valid_records),
            invalid_count=len(invalid_records),
            duration_ms=transform_timer.duration_ms,
        )
        
        logger.info(
            "Transform completed",
            extra={
                "source": source,
                "batch_id": batch_id,
                "step": "transform",
                "input_count": len(raw_records),
                "output_count": len(valid_records),
                "invalid_count": len(invalid_records),
                "duration_ms": round(transform_timer.duration_ms, 2),
            }
        )
        
        # Upload to S3 with timing
        with timed_operation("s3_upload", logger) as upload_timer:
            writer = s3_writer or S3Writer()
            metadata = writer.write(valid_records, source=source, batch_id=batch_id)
        
        s3_path = metadata.get("s3_uri")
        plog.log_s3_upload(
            s3_path=s3_path,
            row_count=metadata.get("record_count", 0),
            file_size_bytes=metadata.get("file_size_bytes", 0),
            duration_ms=upload_timer.duration_ms,
        )
        
        logger.info(
            "S3 upload completed",
            extra={
                "source": source,
                "batch_id": batch_id,
                "step": "s3_upload",
                "s3_path": s3_path,
                "row_count": metadata.get("record_count", 0),
                "file_size_bytes": metadata.get("file_size_bytes", 0),
                "duration_ms": round(upload_timer.duration_ms, 2),
            }
        )
        
        result = {
            "source": source,
            "batch_id": batch_id,
            "status": "success",
            "records_fetched": len(raw_records),
            "records_valid": len(valid_records),
            "records_invalid": len(invalid_records),
            "records_staged": metadata.get("record_count", 0),
            "s3_uri": s3_path,
            "s3_path": metadata.get("s3_key"),
            "file_size_bytes": metadata.get("file_size_bytes"),
            "api_metrics": api_metrics,
            "timings": {
                "api_fetch_ms": round(fetch_timer.duration_ms, 2),
                "transform_ms": round(transform_timer.duration_ms, 2),
                "s3_upload_ms": round(upload_timer.duration_ms, 2),
            },
        }
        
        plog.success("ingestion", row_count=len(valid_records), s3_path=s3_path)
        logger.info("Ingestion completed successfully", extra=result)
        return result
        
    except Exception as e:
        plog.error("ingestion", e)
        logger.error(
            "Ingestion failed",
            extra={
                "source": source,
                "batch_id": batch_id,
                "step": "error",
                "error": str(e),
                "retry_count": plog._retry_count,
            },
            exc_info=True
        )
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
    plog = PipelineLogger(source=source, batch_id=batch_id)
    plog.start("ingestion")
    
    logger.info(
        "Starting ingestion",
        extra={
            "source": source,
            "batch_id": batch_id,
            "since": since,
            "step": "start",
        }
    )
    
    try:
        # Fetch from API with timing
        with timed_operation("api_fetch", logger) as fetch_timer:
            client = ApiBClient()
            raw_records = client.fetch(since=since)
        
        # Log API request metrics
        api_metrics = client.metrics.to_dict()
        logger.info(
            "API fetch completed",
            extra={
                "source": source,
                "batch_id": batch_id,
                "step": "api_fetch",
                "row_count": len(raw_records),
                "duration_ms": round(fetch_timer.duration_ms, 2),
                "retry_count": api_metrics["total_retries"],
                "api_metrics": api_metrics,
            }
        )
        
        if not raw_records:
            plog.success("ingestion", row_count=0)
            return {
                "source": source,
                "batch_id": batch_id,
                "status": "success",
                "records_fetched": 0,
                "records_staged": 0,
                "api_metrics": api_metrics,
            }
        
        # Transform with timing
        with timed_operation("transform", logger) as transform_timer:
            valid_records, invalid_records = transform_records(
                raw_records,
                required_fields=["id"],
                timestamp_fields=["created_at", "modified_at"],
                dedupe_key_fields=["id"],
                dedupe_sort_field="modified_at",
                normalize_keys=True,
            )
        
        plog.log_transform(
            input_count=len(raw_records),
            output_count=len(valid_records),
            invalid_count=len(invalid_records),
            duration_ms=transform_timer.duration_ms,
        )
        
        logger.info(
            "Transform completed",
            extra={
                "source": source,
                "batch_id": batch_id,
                "step": "transform",
                "input_count": len(raw_records),
                "output_count": len(valid_records),
                "invalid_count": len(invalid_records),
                "duration_ms": round(transform_timer.duration_ms, 2),
            }
        )
        
        # Upload to S3 with timing
        with timed_operation("s3_upload", logger) as upload_timer:
            writer = s3_writer or S3Writer()
            metadata = writer.write(valid_records, source=source, batch_id=batch_id)
        
        s3_path = metadata.get("s3_uri")
        plog.log_s3_upload(
            s3_path=s3_path,
            row_count=metadata.get("record_count", 0),
            file_size_bytes=metadata.get("file_size_bytes", 0),
            duration_ms=upload_timer.duration_ms,
        )
        
        logger.info(
            "S3 upload completed",
            extra={
                "source": source,
                "batch_id": batch_id,
                "step": "s3_upload",
                "s3_path": s3_path,
                "row_count": metadata.get("record_count", 0),
                "file_size_bytes": metadata.get("file_size_bytes", 0),
                "duration_ms": round(upload_timer.duration_ms, 2),
            }
        )
        
        result = {
            "source": source,
            "batch_id": batch_id,
            "status": "success",
            "records_fetched": len(raw_records),
            "records_valid": len(valid_records),
            "records_invalid": len(invalid_records),
            "records_staged": metadata.get("record_count", 0),
            "s3_uri": s3_path,
            "s3_path": metadata.get("s3_key"),
            "file_size_bytes": metadata.get("file_size_bytes"),
            "api_metrics": api_metrics,
            "timings": {
                "api_fetch_ms": round(fetch_timer.duration_ms, 2),
                "transform_ms": round(transform_timer.duration_ms, 2),
                "s3_upload_ms": round(upload_timer.duration_ms, 2),
            },
        }
        
        plog.success("ingestion", row_count=len(valid_records), s3_path=s3_path)
        logger.info("Ingestion completed successfully", extra=result)
        return result
        
    except Exception as e:
        plog.error("ingestion", e)
        logger.error(
            "Ingestion failed",
            extra={
                "source": source,
                "batch_id": batch_id,
                "step": "error",
                "error": str(e),
                "retry_count": plog._retry_count,
            },
            exc_info=True
        )
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

