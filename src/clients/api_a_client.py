"""API A Client - OAuth2 authentication with pagination and rate limiting."""

import logging
import os
from typing import Generator, Optional

from src.auth.oauth2 import OAuth2Client
from src.clients.base import BaseAPIClient

logger = logging.getLogger(__name__)


class ApiAClient(BaseAPIClient):
    """Client for API A with OAuth2 authentication.
    
    Features:
    - OAuth2 token refresh flow
    - Cursor-based pagination
    - Rate limiting (100 requests/minute)
    - Automatic retries with exponential backoff
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token_url: Optional[str] = None,
        rate_limit_requests: int = 100,
        rate_limit_period: int = 60,
    ):
        """Initialize API A client.
        
        Args:
            base_url: API base URL (or from env: API_A_BASE_URL)
            client_id: OAuth2 client ID (or from env: API_A_OAUTH_CLIENT_ID)
            client_secret: OAuth2 client secret (or from env: API_A_OAUTH_CLIENT_SECRET)
            token_url: OAuth2 token URL (or from env: API_A_OAUTH_TOKEN_URL)
            rate_limit_requests: Max requests per period
            rate_limit_period: Rate limit period in seconds
        """
        base_url = base_url or os.getenv("API_A_BASE_URL")
        if not base_url:
            raise ValueError("API_A_BASE_URL is required")
        
        super().__init__(
            base_url=base_url,
            rate_limit_requests=rate_limit_requests,
            rate_limit_period=rate_limit_period,
        )
        
        # Setup OAuth2 authentication
        self.oauth_client = OAuth2Client(
            client_id=client_id or os.getenv("API_A_OAUTH_CLIENT_ID"),
            client_secret=client_secret or os.getenv("API_A_OAUTH_CLIENT_SECRET"),
            token_url=token_url or os.getenv("API_A_OAUTH_TOKEN_URL"),
        )
    
    def get_auth_headers(self) -> dict:
        """Get OAuth2 authorization headers."""
        return self.oauth_client.get_auth_header()
    
    def paginate(
        self,
        endpoint: str,
        params: Optional[dict] = None,
    ) -> Generator[dict, None, None]:
        """Paginate through API results using cursor-based pagination.
        
        Assumes API returns:
        {
            "data": [...],
            "meta": {"next_cursor": "..."}
        }
        
        Yields:
            Individual records from paginated responses
        """
        params = params or {}
        cursor = None
        page = 0
        
        while True:
            page += 1
            request_params = {**params}
            if cursor:
                request_params["cursor"] = cursor
            
            logger.debug(f"Fetching page {page}", extra={"cursor": cursor})
            
            response = self.get(endpoint, params=request_params)
            
            # Extract data array
            data = response.get("data", [])
            if not data:
                logger.debug("No more data, stopping pagination")
                break
            
            for record in data:
                yield record
            
            # Check for next cursor
            meta = response.get("meta", {})
            cursor = meta.get("next_cursor")
            
            if not cursor:
                logger.debug("No next cursor, pagination complete")
                break
        
        logger.info(f"Pagination complete after {page} pages")
    
    def fetch(self, since: Optional[str] = None) -> list[dict]:
        """Fetch all events from API A.
        
        Args:
            since: Optional ISO timestamp for incremental fetch
                   (e.g., "2025-01-15T00:00:00Z")
        
        Returns:
            List of event records
        """
        endpoint = "events"
        params = {}
        
        if since:
            params["updated_since"] = since
            logger.info(f"Fetching events since {since}")
        else:
            logger.info("Fetching all events (full load)")
        
        records = list(self.paginate(endpoint, params))
        
        logger.info(
            f"Fetched {len(records)} records from API A",
            extra={
                "source": "api_a",
                "record_count": len(records),
                "incremental": since is not None,
            }
        )
        
        return records

