"""Structured logging utilities for pipeline observability.

Provides consistent logging format with required fields:
- source
- batch_id
- row_count
- s3_path
- api_request_timing
- retry_count
"""

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class PipelineLogContext:
    """Context for pipeline logging with required fields."""
    
    source: str
    batch_id: str
    step: str = ""
    row_count: int = 0
    s3_path: Optional[str] = None
    request_duration_ms: Optional[float] = None
    retry_count: int = 0
    status: str = "started"
    error: Optional[str] = None
    extra: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for logging."""
        data = asdict(self)
        data["timestamp"] = datetime.now(timezone.utc).isoformat()
        # Remove None values
        return {k: v for k, v in data.items() if v is not None}
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)


class PipelineLogger:
    """Structured logger for pipeline operations."""
    
    def __init__(self, source: str, batch_id: str):
        """Initialize pipeline logger.
        
        Args:
            source: Data source name (e.g., 'api_a', 'api_b')
            batch_id: Unique batch identifier
        """
        self.source = source
        self.batch_id = batch_id
        self.logger = logging.getLogger(f"pipeline.{source}")
        self._start_time: Optional[float] = None
        self._request_times: list[float] = []
        self._retry_count: int = 0
    
    def _log(self, level: int, step: str, **kwargs) -> None:
        """Internal logging method with structured context."""
        ctx = PipelineLogContext(
            source=self.source,
            batch_id=self.batch_id,
            step=step,
            retry_count=self._retry_count,
            **kwargs
        )
        self.logger.log(level, ctx.to_json(), extra=ctx.to_dict())
    
    def start(self, step: str) -> None:
        """Log step start."""
        self._start_time = time.time()
        self._log(logging.INFO, step, status="started")
    
    def success(self, step: str, **kwargs) -> None:
        """Log step success."""
        duration = None
        if self._start_time:
            duration = (time.time() - self._start_time) * 1000
        self._log(
            logging.INFO,
            step,
            status="success",
            request_duration_ms=duration,
            **kwargs
        )
    
    def error(self, step: str, error: Exception, **kwargs) -> None:
        """Log step error."""
        duration = None
        if self._start_time:
            duration = (time.time() - self._start_time) * 1000
        self._log(
            logging.ERROR,
            step,
            status="error",
            error=str(error),
            request_duration_ms=duration,
            **kwargs
        )
    
    def increment_retry(self) -> int:
        """Increment and return retry count."""
        self._retry_count += 1
        return self._retry_count
    
    def log_api_request(
        self,
        endpoint: str,
        duration_ms: float,
        status_code: int,
        row_count: int = 0,
    ) -> None:
        """Log API request with timing."""
        self._request_times.append(duration_ms)
        self._log(
            logging.INFO,
            step="api_request",
            status="success" if status_code < 400 else "error",
            request_duration_ms=duration_ms,
            row_count=row_count,
            extra={
                "endpoint": endpoint,
                "status_code": status_code,
            }
        )
    
    def log_s3_upload(
        self,
        s3_path: str,
        row_count: int,
        file_size_bytes: int,
        duration_ms: float,
    ) -> None:
        """Log S3 upload."""
        self._log(
            logging.INFO,
            step="s3_upload",
            status="success",
            s3_path=s3_path,
            row_count=row_count,
            request_duration_ms=duration_ms,
            extra={
                "file_size_bytes": file_size_bytes,
            }
        )
    
    def log_transform(
        self,
        input_count: int,
        output_count: int,
        invalid_count: int,
        duration_ms: float,
    ) -> None:
        """Log transformation step."""
        self._log(
            logging.INFO,
            step="transform",
            status="success",
            row_count=output_count,
            request_duration_ms=duration_ms,
            extra={
                "input_count": input_count,
                "output_count": output_count,
                "invalid_count": invalid_count,
            }
        )
    
    def get_metrics(self) -> dict:
        """Get aggregated metrics."""
        return {
            "source": self.source,
            "batch_id": self.batch_id,
            "total_requests": len(self._request_times),
            "total_request_time_ms": sum(self._request_times),
            "avg_request_time_ms": (
                sum(self._request_times) / len(self._request_times)
                if self._request_times else 0
            ),
            "retry_count": self._retry_count,
        }


@contextmanager
def timed_operation(name: str, logger: logging.Logger = None):
    """Context manager to time an operation.
    
    Usage:
        with timed_operation("api_fetch") as timer:
            result = fetch_data()
        print(f"Took {timer.duration_ms}ms")
    
    Args:
        name: Operation name for logging
        logger: Optional logger instance
    
    Yields:
        Timer object with duration_ms attribute
    """
    class Timer:
        def __init__(self):
            self.start_time = time.time()
            self.end_time = None
            self.duration_ms = 0
    
    timer = Timer()
    
    try:
        yield timer
    finally:
        timer.end_time = time.time()
        timer.duration_ms = (timer.end_time - timer.start_time) * 1000
        
        if logger:
            logger.debug(
                f"Operation '{name}' completed",
                extra={"operation": name, "duration_ms": timer.duration_ms}
            )


def log_retry(logger: logging.Logger = None):
    """Decorator to log function retries.
    
    Usage:
        @log_retry(logger)
        def fetch_with_retry():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = kwargs.pop("_retry_attempt", 1)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if logger:
                    logger.warning(
                        f"Retry attempt {attempt} for {func.__name__}",
                        extra={
                            "function": func.__name__,
                            "retry_attempt": attempt,
                            "error": str(e),
                        }
                    )
                raise
        
        return wrapper
    return decorator

