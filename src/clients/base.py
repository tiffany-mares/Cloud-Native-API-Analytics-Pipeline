"""Base API client with common functionality."""

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Generator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


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
        self.rate_limit_requests = rate_limit_requests
        self.rate_limit_period = rate_limit_period
        
        # Request tracking for rate limiting
        self._request_timestamps: list[float] = []
        
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
    ) -> requests.Response:
        """Make HTTP request with rate limiting and logging.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (will be joined with base_url)
            params: Query parameters
            json_data: JSON body data
            headers: Additional headers
            
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
        
        logger.debug(
            f"Making {method} request",
            extra={"url": url, "params": params}
        )
        
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json_data,
            headers=request_headers,
            timeout=self.timeout,
        )
        
        # Handle rate limit response specifically
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning(
                f"Rate limited (429), waiting {retry_after}s",
                extra={"retry_after": retry_after}
            )
            time.sleep(retry_after)
            return self._make_request(method, endpoint, params, json_data, headers)
        
        response.raise_for_status()
        return response
    
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

