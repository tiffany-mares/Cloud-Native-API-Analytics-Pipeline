"""Authentication modules for API access.

Supports:
- OAuth2 with token refresh flow
- API key authentication
- Key-pair authentication
"""

from .oauth2 import OAuth2Client
from .api_key import APIKeyAuth

__all__ = ["OAuth2Client", "APIKeyAuth"]

