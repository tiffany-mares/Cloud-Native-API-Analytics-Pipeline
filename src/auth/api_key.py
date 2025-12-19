"""API Key authentication handler."""

import logging
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class APIKeyLocation(Enum):
    """Where to place the API key in requests."""
    
    HEADER = "header"
    QUERY = "query"


class APIKeyAuth:
    """API Key authentication handler.
    
    Supports placing API key in headers or query parameters.
    """
    
    def __init__(
        self,
        api_key: str,
        key_name: str = "X-API-Key",
        location: APIKeyLocation = APIKeyLocation.HEADER,
    ):
        """Initialize API key auth.
        
        Args:
            api_key: The API key value
            key_name: Name of the header or query parameter
            location: Where to place the key (header or query)
        """
        self.api_key = api_key
        self.key_name = key_name
        self.location = location
        
        logger.debug(
            "APIKeyAuth initialized",
            extra={"key_name": key_name, "location": location.value}
        )
    
    def get_auth_header(self) -> dict:
        """Get authorization header dict for requests.
        
        Returns empty dict if location is QUERY.
        """
        if self.location == APIKeyLocation.HEADER:
            return {self.key_name: self.api_key}
        return {}
    
    def get_auth_params(self) -> dict:
        """Get query parameters dict for requests.
        
        Returns empty dict if location is HEADER.
        """
        if self.location == APIKeyLocation.QUERY:
            return {self.key_name: self.api_key}
        return {}
    
    def apply_auth(
        self,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> tuple[dict, dict]:
        """Apply authentication to headers and params.
        
        Args:
            headers: Existing headers dict (or None)
            params: Existing params dict (or None)
            
        Returns:
            Tuple of (headers, params) with auth applied
        """
        headers = headers or {}
        params = params or {}
        
        headers.update(self.get_auth_header())
        params.update(self.get_auth_params())
        
        return headers, params

