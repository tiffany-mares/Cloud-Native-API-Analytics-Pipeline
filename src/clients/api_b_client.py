"""API B Client - API Key authentication with pagination."""

import logging
import os
from typing import Generator, Optional

from src.auth.api_key import APIKeyAuth, APIKeyLocation
from src.clients.base import BaseAPIClient

logger = logging.getLogger(__name__)


class ApiBClient(BaseAPIClient):
    """Client for API B with API Key authentication.
    
    Features:
    - API key authentication (header-based)
    - Offset-based pagination
    - Rate limiting
    - Automatic retries with exponential backoff
    """
    
    DEFAULT_PAGE_SIZE = 100
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        rate_limit_requests: int = 60,
        rate_limit_period: int = 60,
    ):
        """Initialize API B client.
        
        Args:
            base_url: API base URL (or from env: API_B_BASE_URL)
            api_key: API key (or from env: API_B_API_KEY)
            page_size: Number of records per page
            rate_limit_requests: Max requests per period
            rate_limit_period: Rate limit period in seconds
        """
        base_url = base_url or os.getenv("API_B_BASE_URL")
        if not base_url:
            raise ValueError("API_B_BASE_URL is required")
        
        super().__init__(
            base_url=base_url,
            rate_limit_requests=rate_limit_requests,
            rate_limit_period=rate_limit_period,
        )
        
        self.page_size = page_size
        
        # Setup API key authentication
        api_key_value = api_key or os.getenv("API_B_API_KEY")
        if not api_key_value:
            raise ValueError("API_B_API_KEY is required")
        
        self.api_key_auth = APIKeyAuth(
            api_key=api_key_value,
            key_name="X-API-Key",
            location=APIKeyLocation.HEADER,
        )
    
    def get_auth_headers(self) -> dict:
        """Get API key authorization headers."""
        return self.api_key_auth.get_auth_header()
    
    def paginate(
        self,
        endpoint: str,
        params: Optional[dict] = None,
    ) -> Generator[dict, None, None]:
        """Paginate through API results using offset-based pagination.
        
        Assumes API returns:
        {
            "items": [...],
            "total": 1000,
            "offset": 0,
            "limit": 100
        }
        
        Yields:
            Individual records from paginated responses
        """
        params = params or {}
        offset = 0
        total_fetched = 0
        page = 0
        
        while True:
            page += 1
            request_params = {
                **params,
                "offset": offset,
                "limit": self.page_size,
            }
            
            logger.debug(
                f"Fetching page {page}",
                extra={"offset": offset, "limit": self.page_size}
            )
            
            response = self.get(endpoint, params=request_params)
            
            # Extract items array
            items = response.get("items", [])
            if not items:
                logger.debug("No more items, stopping pagination")
                break
            
            for record in items:
                yield record
            
            total_fetched += len(items)
            
            # Check if we've fetched all records
            total = response.get("total", 0)
            if total_fetched >= total:
                logger.debug(f"Fetched all {total} records")
                break
            
            offset += self.page_size
        
        logger.info(f"Pagination complete: {total_fetched} records in {page} pages")
    
    def fetch(self, since: Optional[str] = None) -> list[dict]:
        """Fetch all data from API B.
        
        Args:
            since: Optional ISO timestamp for incremental fetch
                   (e.g., "2025-01-15T00:00:00Z")
        
        Returns:
            List of data records
        """
        endpoint = "data"
        params = {}
        
        if since:
            params["modified_after"] = since
            logger.info(f"Fetching data since {since}")
        else:
            logger.info("Fetching all data (full load)")
        
        records = list(self.paginate(endpoint, params))
        
        logger.info(
            f"Fetched {len(records)} records from API B",
            extra={
                "source": "api_b",
                "record_count": len(records),
                "incremental": since is not None,
            }
        )
        
        return records

