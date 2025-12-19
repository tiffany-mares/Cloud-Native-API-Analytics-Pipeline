"""Tests for transformation modules."""

import pytest
from src.transform.flatten import flatten_json, flatten_records
from src.transform.normalize import (
    normalize_timestamp,
    normalize_key,
    normalize_record,
    validate_required_fields,
    validate_records,
    dedupe_records,
    dedupe_by_id_updated,
    transform_records,
    ValidationError,
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


class TestValidation:
    """Tests for record validation."""
    
    def test_validate_required_fields_pass(self):
        """Test validation passes with all required fields."""
        record = {"id": "123", "name": "Test", "value": 100}
        result = validate_required_fields(record, ["id", "name"])
        
        assert result.is_valid is True
        assert result.errors == []
    
    def test_validate_required_fields_missing(self):
        """Test validation fails with missing field."""
        record = {"id": "123"}
        result = validate_required_fields(record, ["id", "name"])
        
        assert result.is_valid is False
        assert "Missing required field: name" in result.errors
    
    def test_validate_required_fields_null(self):
        """Test validation fails with null value."""
        record = {"id": "123", "name": None}
        result = validate_required_fields(record, ["id", "name"])
        
        assert result.is_valid is False
        assert "Null value for required field: name" in result.errors
    
    def test_validate_required_fields_empty_string(self):
        """Test validation fails with empty string."""
        record = {"id": "123", "name": "   "}
        result = validate_required_fields(record, ["id", "name"])
        
        assert result.is_valid is False
        assert "Empty value for required field: name" in result.errors
    
    def test_validate_records_batch(self):
        """Test batch validation."""
        records = [
            {"id": "1", "name": "Valid"},
            {"id": "2", "name": None},
            {"id": "3", "name": "Also Valid"},
        ]
        valid, invalid = validate_records(records, ["id", "name"])
        
        assert len(valid) == 2
        assert len(invalid) == 1
        assert invalid[0]["id"] == "2"
    
    def test_validate_records_raise_on_error(self):
        """Test validation raises exception when requested."""
        records = [{"id": "1"}]
        
        with pytest.raises(ValidationError) as exc_info:
            validate_records(records, ["id", "name"], raise_on_error=True)
        
        assert "name" in str(exc_info.value.errors)


class TestDeduplication:
    """Tests for record deduplication."""
    
    def test_dedupe_by_single_key(self):
        """Test deduplication by single key field."""
        records = [
            {"id": "1", "value": "first"},
            {"id": "2", "value": "unique"},
            {"id": "1", "value": "second"},
        ]
        result = dedupe_records(records, key_fields=["id"])
        
        assert len(result) == 2
    
    def test_dedupe_keep_last(self):
        """Test keeping last duplicate."""
        records = [
            {"id": "1", "updated_at": "2025-01-01", "value": "old"},
            {"id": "1", "updated_at": "2025-01-02", "value": "new"},
        ]
        result = dedupe_records(
            records,
            key_fields=["id"],
            sort_field="updated_at",
            keep="last",
        )
        
        assert len(result) == 1
        assert result[0]["value"] == "new"
    
    def test_dedupe_keep_first(self):
        """Test keeping first duplicate."""
        records = [
            {"id": "1", "updated_at": "2025-01-01", "value": "old"},
            {"id": "1", "updated_at": "2025-01-02", "value": "new"},
        ]
        result = dedupe_records(
            records,
            key_fields=["id"],
            sort_field="updated_at",
            keep="first",
        )
        
        assert len(result) == 1
        assert result[0]["value"] == "old"
    
    def test_dedupe_composite_key(self):
        """Test deduplication with composite key."""
        records = [
            {"id": "1", "date": "2025-01-01", "value": "a"},
            {"id": "1", "date": "2025-01-02", "value": "b"},
            {"id": "1", "date": "2025-01-01", "value": "c"},  # Duplicate
        ]
        result = dedupe_records(records, key_fields=["id", "date"])
        
        assert len(result) == 2
    
    def test_dedupe_by_id_updated_convenience(self):
        """Test convenience function for id+updated_at deduplication."""
        records = [
            {"id": "1", "updated_at": "2025-01-01", "value": "old"},
            {"id": "1", "updated_at": "2025-01-02", "value": "new"},
            {"id": "2", "updated_at": "2025-01-01", "value": "other"},
        ]
        result = dedupe_by_id_updated(records)
        
        assert len(result) == 2
        # Should have the latest version of id=1
        id1_record = next(r for r in result if r["id"] == "1")
        assert id1_record["value"] == "new"
    
    def test_dedupe_empty_list(self):
        """Test deduplication of empty list."""
        result = dedupe_records([], key_fields=["id"])
        assert result == []
    
    def test_dedupe_skips_null_keys(self):
        """Test that records with null key fields are skipped."""
        records = [
            {"id": "1", "value": "valid"},
            {"id": None, "value": "invalid"},
        ]
        result = dedupe_records(records, key_fields=["id"])
        
        assert len(result) == 1
        assert result[0]["id"] == "1"


class TestTransformPipeline:
    """Tests for full transformation pipeline."""
    
    def test_transform_records_full_pipeline(self):
        """Test complete transformation pipeline."""
        records = [
            {"id": "1", "name": "First", "createdAt": "2025-01-01T00:00:00Z"},
            {"id": "2", "name": "Second", "createdAt": "2025-01-02T00:00:00Z"},
            {"id": "1", "name": "Updated", "createdAt": "2025-01-03T00:00:00Z"},
        ]
        
        valid, invalid = transform_records(
            records,
            required_fields=["id", "name"],
            timestamp_fields=["created_at"],
            dedupe_key_fields=["id"],
            dedupe_sort_field="created_at",
            normalize_keys=True,
        )
        
        # Should have 2 records after deduplication
        assert len(valid) == 2
        assert len(invalid) == 0
        
        # Keys should be normalized
        assert all("created_at" in r for r in valid)
    
    def test_transform_records_with_invalid(self):
        """Test pipeline with some invalid records."""
        records = [
            {"id": "1", "name": "Valid"},
            {"id": "2", "name": None},  # Invalid
            {"id": "3", "name": "Also Valid"},
        ]
        
        valid, invalid = transform_records(
            records,
            required_fields=["id", "name"],
        )
        
        assert len(valid) == 2
        assert len(invalid) == 1
