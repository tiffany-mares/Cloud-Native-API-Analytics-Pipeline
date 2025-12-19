"""API client wrappers for external data sources.

Each client handles:
- Authentication
- Pagination
- Rate limiting
- Retries with exponential backoff
"""

from .base import BaseAPIClient

__all__ = ["BaseAPIClient"]

