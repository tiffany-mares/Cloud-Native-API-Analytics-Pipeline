"""API client wrappers for external data sources.

Each client handles:
- Authentication
- Pagination
- Rate limiting
- Retries with exponential backoff
"""

from .base import BaseAPIClient
from .api_a_client import ApiAClient
from .api_b_client import ApiBClient

__all__ = ["BaseAPIClient", "ApiAClient", "ApiBClient"]

