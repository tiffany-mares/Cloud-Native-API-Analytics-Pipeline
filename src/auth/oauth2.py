"""OAuth2 authentication with automatic token refresh."""

import time
import logging
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class TokenInfo:
    """OAuth2 token information."""
    
    access_token: str
    token_type: str
    expires_at: float
    refresh_token: Optional[str] = None
    scope: Optional[str] = None


class OAuth2Client:
    """OAuth2 client with automatic token refresh.
    
    Supports client_credentials and refresh_token grant types.
    """
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        refresh_token: Optional[str] = None,
        scope: Optional[str] = None,
        token_expiry_buffer: int = 60,
    ):
        """Initialize OAuth2 client.
        
        Args:
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            token_url: Token endpoint URL
            refresh_token: Optional refresh token for refresh_token grant
            scope: Optional scope(s) to request
            token_expiry_buffer: Seconds before expiry to trigger refresh
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.refresh_token = refresh_token
        self.scope = scope
        self.token_expiry_buffer = token_expiry_buffer
        self._token_info: Optional[TokenInfo] = None
    
    def get_access_token(self) -> str:
        """Get valid access token, refreshing if necessary."""
        if self._token_info is None or self._is_token_expired():
            self._refresh_access_token()
        return self._token_info.access_token
    
    def _is_token_expired(self) -> bool:
        """Check if current token is expired or about to expire."""
        if self._token_info is None:
            return True
        return time.time() >= (self._token_info.expires_at - self.token_expiry_buffer)
    
    def _refresh_access_token(self) -> None:
        """Refresh the access token."""
        if self.refresh_token:
            self._do_refresh_token_grant()
        else:
            self._do_client_credentials_grant()
    
    def _do_client_credentials_grant(self) -> None:
        """Perform client_credentials grant."""
        logger.info("Requesting new access token via client_credentials grant")
        
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scope:
            data["scope"] = self.scope
        
        self._request_token(data)
    
    def _do_refresh_token_grant(self) -> None:
        """Perform refresh_token grant."""
        logger.info("Refreshing access token via refresh_token grant")
        
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }
        
        self._request_token(data)
    
    def _request_token(self, data: dict) -> None:
        """Make token request and update token info."""
        response = requests.post(
            self.token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        response.raise_for_status()
        
        token_data = response.json()
        expires_in = token_data.get("expires_in", 3600)
        
        self._token_info = TokenInfo(
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "Bearer"),
            expires_at=time.time() + expires_in,
            refresh_token=token_data.get("refresh_token", self.refresh_token),
            scope=token_data.get("scope", self.scope),
        )
        
        # Update refresh token if a new one was provided
        if token_data.get("refresh_token"):
            self.refresh_token = token_data["refresh_token"]
        
        logger.info(
            "Token obtained successfully",
            extra={
                "token_type": self._token_info.token_type,
                "expires_in": expires_in,
            }
        )
    
    def get_auth_header(self) -> dict:
        """Get authorization header dict for requests."""
        token = self.get_access_token()
        token_type = self._token_info.token_type if self._token_info else "Bearer"
        return {"Authorization": f"{token_type} {token}"}

