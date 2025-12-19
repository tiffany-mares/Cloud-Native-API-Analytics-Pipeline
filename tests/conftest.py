"""Pytest configuration and fixtures."""

import pytest
from datetime import datetime, timezone


@pytest.fixture
def sample_record():
    """Sample API record for testing."""
    return {
        "id": "12345",
        "name": "Test Record",
        "created_at": "2024-01-15T10:30:00Z",
        "metadata": {
            "source": "api",
            "version": "1.0",
        },
        "tags": ["test", "sample"],
    }


@pytest.fixture
def sample_records():
    """List of sample records for testing."""
    return [
        {
            "id": "1",
            "name": "Record 1",
            "value": 100,
            "timestamp": "2024-01-15T10:00:00Z",
        },
        {
            "id": "2",
            "name": "Record 2",
            "value": 200,
            "timestamp": "2024-01-15T11:00:00Z",
        },
        {
            "id": "3",
            "name": "Record 3",
            "value": 300,
            "timestamp": "2024-01-15T12:00:00Z",
        },
    ]


@pytest.fixture
def nested_record():
    """Deeply nested record for testing flattening."""
    return {
        "id": "nested-1",
        "user": {
            "name": "John Doe",
            "email": "john@example.com",
            "profile": {
                "age": 30,
                "location": {
                    "city": "New York",
                    "country": "USA",
                },
            },
        },
        "items": [
            {"sku": "A1", "qty": 2},
            {"sku": "B2", "qty": 1},
        ],
    }


@pytest.fixture
def mock_datetime():
    """Fixed datetime for testing."""
    return datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

