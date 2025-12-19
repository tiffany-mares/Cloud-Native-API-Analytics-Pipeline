"""Base API client with common functionality."""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class RequestMetrics:
    """Metrics for API requests."""
    
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_retries: int = 0
    total_duration_ms: float = 0
    request_durations: list[float] = field(default_factory=list)
    
    def record_request(self, duration_ms: float, success: bool) -> None:
        """Record a request."""
        self.total_requests += 1
        self.total_duration_ms += duration_ms
        self.request_durations.append(duration_ms)
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
    
    def record_retry(self) -> None:
        """Record a retry."""
        self.total_retries += 1
    
    @property
    def avg_duration_ms(self) -> float:
        """Average request duration."""
        if not self.request_durations:
            return 0
        return sum(self.request_durations) / len(self.request_durations)
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "total_retries": self.total_retries,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "avg_duration_ms": round(self.avg_duration_ms, 2),
        }


class BaseAPIClient(ABC):
    """Base class for API clients with retry and rate limiting support."""
    
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        rate_limit_requests: int = 100,
        rate_limit_period: int = 60,
    ):
        """Initialize base API client.
        
        Args:
            base_url: Base URL for the API
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts for failed requests
            backoff_factor: Exponential backoff factor
            rate_limit_requests: Max requests per period
            rate_limit_period: Rate limit period in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limit_requests = rate_limit_requests
        self.rate_limit_period = rate_limit_period
        
        # Request tracking for rate limiting
        self._request_timestamps: list[float] = []
        
        # Metrics tracking
        self.metrics = RequestMetrics()
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    @abstractmethod
    def get_auth_headers(self) -> dict:
        """Get authentication headers for requests."""
        pass
    
    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limits."""
        now = time.time()
        
        # Remove timestamps outside the rate limit window
        self._request_timestamps = [
            ts for ts in self._request_timestamps
            if now - ts < self.rate_limit_period
        ]
        
        if len(self._request_timestamps) >= self.rate_limit_requests:
            # Calculate wait time
            oldest = min(self._request_timestamps)
            wait_time = self.rate_limit_period - (now - oldest)
            
            if wait_time > 0:
                logger.warning(
                    f"Rate limit reached, waiting {wait_time:.2f}s",
                    extra={"wait_seconds": wait_time}
                )
                time.sleep(wait_time)
        
        self._request_timestamps.append(time.time())
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        headers: Optional[dict] = None,
        _retry_count: int = 0,
    ) -> requests.Response:
        """Make HTTP request with rate limiting, timing, and logging.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (will be joined with base_url)
            params: Query parameters
            json_data: JSON body data
            headers: Additional headers
            _retry_count: Internal retry counter
            
        Returns:
            Response object
            
        Raises:
            requests.HTTPError: On non-2xx responses after retries
        """
        self._wait_for_rate_limit()
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        request_headers = self.get_auth_headers()
        if headers:
            request_headers.update(headers)
        
        # Start timing
        start_time = time.time()
        
        logger.debug(
            f"Making {method} request",
            extra={
                "url": url,
                "params": params,
                "retry_count": _retry_count,
            }
        )
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                headers=request_headers,
                timeout=self.timeout,
            )
            
            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000
            
            # Handle rate limit response specifically
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                self.metrics.record_retry()
                
                logger.warning(
                    f"Rate limited (429), waiting {retry_after}s",
                    extra={
                        "retry_after": retry_after,
                        "retry_count": _retry_count + 1,
                        "endpoint": endpoint,
                        "duration_ms": round(duration_ms, 2),
                    }
                )
                time.sleep(retry_after)
                return self._make_request(
                    method, endpoint, params, json_data, headers,
                    _retry_count=_retry_count + 1
                )
            
            # Record successful request metrics
            self.metrics.record_request(duration_ms, success=response.ok)
            
            # Log request completion
            logger.info(
                f"API request completed",
                extra={
                    "method": method,
                    "endpoint": endpoint,
                    "status_code": response.status_code,
                    "duration_ms": round(duration_ms, 2),
                    "retry_count": _retry_count,
                    "response_size_bytes": len(response.content),
                }
            )
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            duration_ms = (time.time() - start_time) * 1000
            self.metrics.record_request(duration_ms, success=False)
            
            logger.error(
                f"API request failed",
                extra={
                    "method": method,
                    "endpoint": endpoint,
                    "error": str(e),
                    "duration_ms": round(duration_ms, 2),
                    "retry_count": _retry_count,
                }
            )
            raise
    
    def get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> dict:
        """Make GET request and return JSON response."""
        response = self._make_request("GET", endpoint, params=params, headers=headers)
        return response.json()
    
    def post(
        self,
        endpoint: str,
        json_data: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> dict:
        """Make POST request and return JSON response."""
        response = self._make_request(
            "POST", endpoint, params=params, json_data=json_data, headers=headers
        )
        return response.json()
    
    @abstractmethod
    def paginate(
        self,
        endpoint: str,
        params: Optional[dict] = None,
    ) -> Generator[dict, None, None]:
        """Paginate through API results.
        
        Yields individual records from paginated responses.
        """
        pass
    
    def extract_all(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        since: Optional[str] = None,
    ) -> list[dict]:
        """Extract all records from an endpoint.
        
        Args:
            endpoint: API endpoint
            params: Additional query parameters
            since: Optional timestamp for incremental extraction
            
        Returns:
            List of all extracted records
        """
        params = params or {}
        if since:
            params["since"] = since
        
        records = list(self.paginate(endpoint, params))
        
        logger.info(
            f"Extracted {len(records)} records",
            extra={"endpoint": endpoint, "record_count": len(records)}
        )
        
        return records

