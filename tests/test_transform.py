"""Tests for transformation modules."""

import pytest
from src.transform.flatten import flatten_json, flatten_records
from src.transform.normalize import (
    normalize_timestamp,
    normalize_key,
    normalize_record,
)


class TestFlattenJson:
    """Tests for JSON flattening."""
    
    def test_flatten_simple_nested(self):
        """Test flattening simple nested dict."""
        nested = {"a": {"b": 1, "c": 2}}
        result = flatten_json(nested)
        
        assert result == {"a_b": 1, "a_c": 2}
    
    def test_flatten_deep_nested(self, nested_record):
        """Test flattening deeply nested dict."""
        result = flatten_json(nested_record)
        
        assert result["id"] == "nested-1"
        assert result["user_name"] == "John Doe"
        assert result["user_profile_age"] == 30
        assert result["user_profile_location_city"] == "New York"
    
    def test_flatten_preserves_lists(self, nested_record):
        """Test that lists are preserved."""
        result = flatten_json(nested_record)
        
        assert result["items"] == [
            {"sku": "A1", "qty": 2},
            {"sku": "B2", "qty": 1},
        ]
    
    def test_flatten_custom_separator(self):
        """Test custom separator."""
        nested = {"a": {"b": 1}}
        result = flatten_json(nested, separator=".")
        
        assert result == {"a.b": 1}
    
    def test_flatten_max_depth(self):
        """Test max depth limit."""
        nested = {"a": {"b": {"c": {"d": 1}}}}
        result = flatten_json(nested, max_depth=2)
        
        assert "a_b_c" in result
        assert isinstance(result["a_b_c"], dict)


class TestFlattenRecords:
    """Tests for batch record flattening."""
    
    def test_flatten_multiple_records(self, sample_records):
        """Test flattening list of records."""
        result = flatten_records(sample_records)
        
        assert len(result) == 3
        assert all("id" in r for r in result)


class TestNormalizeTimestamp:
    """Tests for timestamp normalization."""
    
    def test_normalize_iso_format(self):
        """Test ISO 8601 format."""
        result = normalize_timestamp("2024-01-15T10:30:00Z")
        assert result is not None
        assert "2024-01-15" in result
    
    def test_normalize_unix_timestamp(self):
        """Test Unix timestamp (seconds)."""
        result = normalize_timestamp(1705315800)
        assert result is not None
    
    def test_normalize_unix_milliseconds(self):
        """Test Unix timestamp (milliseconds)."""
        result = normalize_timestamp(1705315800000)
        assert result is not None
    
    def test_normalize_none(self):
        """Test None value."""
        result = normalize_timestamp(None)
        assert result is None
    
    def test_normalize_invalid_string(self):
        """Test invalid string returns None."""
        result = normalize_timestamp("not a timestamp")
        assert result is None


class TestNormalizeKey:
    """Tests for key normalization."""
    
    def test_camel_case_to_snake(self):
        """Test camelCase conversion."""
        assert normalize_key("camelCase") == "camel_case"
        assert normalize_key("someAPIKey") == "some_api_key"
    
    def test_remove_special_chars(self):
        """Test special character removal."""
        assert normalize_key("key@name!") == "keyname"
    
    def test_normalize_spaces_hyphens(self):
        """Test space and hyphen handling."""
        assert normalize_key("key-name") == "key_name"
        assert normalize_key("key name") == "key_name"


class TestNormalizeRecord:
    """Tests for record normalization."""
    
    def test_normalize_basic_record(self, sample_record):
        """Test basic record normalization."""
        result = normalize_record(sample_record)
        
        assert "id" in result
        assert "name" in result
    
    def test_normalize_timestamp_fields(self):
        """Test timestamp field normalization."""
        record = {"created_at": "2024-01-15T10:30:00Z", "name": "Test"}
        result = normalize_record(record, timestamp_fields=["created_at"])
        
        assert result["created_at"] is not None
    
    def test_normalize_keys_disabled(self, sample_record):
        """Test with key normalization disabled."""
        record = {"camelCase": "value"}
        result = normalize_record(record, normalize_keys=False)
        
        assert "camelCase" in result

