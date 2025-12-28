"""
Tests for File Intent Validator

Run with: python -m pytest test_file_validator.py -v
"""

import pytest
from pathlib import Path
from ai.file_validator import validate_file


TEST_DATA_DIR = Path(__file__).parent / "test_data"


class TestValidFiles:
    """Test files that should be accepted as valid."""
    
    def test_retail_store(self):
        result = validate_file(TEST_DATA_DIR / "retail_store.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_warehouse(self):
        result = validate_file(TEST_DATA_DIR / "warehouse.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_ecommerce_sales(self):
        result = validate_file(TEST_DATA_DIR / "ecommerce_sales.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_distribution(self):
        result = validate_file(TEST_DATA_DIR / "distribution.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_pos_export(self):
        result = validate_file(TEST_DATA_DIR / "pos_export.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_variation_inventory(self):
        result = validate_file(TEST_DATA_DIR / "variation_inventory.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_variation_qty(self):
        result = validate_file(TEST_DATA_DIR / "variation_qty.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_variation_units(self):
        result = validate_file(TEST_DATA_DIR / "variation_units.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_variation_on_hand(self):
        result = validate_file(TEST_DATA_DIR / "variation_on_hand.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"


class TestInvalidFiles:
    """Test files that should be rejected as invalid."""
    
    def test_invalid_hr(self):
        result = validate_file(TEST_DATA_DIR / "invalid_hr.xlsx")
        assert result["status"] == "invalid", f"Expected invalid, got: {result}"
    
    def test_invalid_finance(self):
        result = validate_file(TEST_DATA_DIR / "invalid_finance.xlsx")
        assert result["status"] == "invalid", f"Expected invalid, got: {result}"
    
    def test_invalid_empty(self):
        result = validate_file(TEST_DATA_DIR / "invalid_empty.xlsx")
        assert result["status"] == "invalid", f"Expected invalid, got: {result}"
    
    def test_invalid_customer_list(self):
        result = validate_file(TEST_DATA_DIR / "invalid_customer_list.xlsx")
        assert result["status"] == "invalid", f"Expected invalid, got: {result}"
    
    def test_invalid_misleading(self):
        result = validate_file(TEST_DATA_DIR / "invalid_misleading.xlsx")
        assert result["status"] == "invalid", f"Expected invalid, got: {result}"


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_extra_columns(self):
        result = validate_file(TEST_DATA_DIR / "extra_columns.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_reordered_columns(self):
        result = validate_file(TEST_DATA_DIR / "reordered_columns.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_mixed_types(self):
        result = validate_file(TEST_DATA_DIR / "mixed_types.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_tiny_dataset(self):
        result = validate_file(TEST_DATA_DIR / "tiny_dataset.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_large_quantities(self):
        result = validate_file(TEST_DATA_DIR / "large_quantities.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_zero_quantities(self):
        result = validate_file(TEST_DATA_DIR / "zero_quantities.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"
    
    def test_partial_missing(self):
        result = validate_file(TEST_DATA_DIR / "partial_missing.xlsx")
        assert result["status"] == "valid", f"Expected valid, got: {result}"


class TestFileTypeValidation:
    """Test file type detection."""
    
    def test_nonexistent_file(self):
        result = validate_file(TEST_DATA_DIR / "nonexistent.xlsx")
        assert result["status"] == "invalid"
        assert "not found" in result["reason"].lower()
    
    def test_unsupported_extension(self):
        from ai.file_validator import validate_file_type
        result = validate_file_type(Path("test.txt"))
        assert result["valid"] == False
        assert "Unsupported" in result["error"]


class TestConfidenceLevels:
    """Test confidence level assignment."""
    
    def test_high_confidence_file(self):
        result = validate_file(TEST_DATA_DIR / "retail_store.xlsx")
        if result["status"] == "valid":
            assert result["confidence"] in ["high", "medium", "low"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
