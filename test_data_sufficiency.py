"""
Tests for Data Sufficiency Check

Run with: python3 -m pytest test_data_sufficiency.py -v
"""

import pytest
from pathlib import Path
from ai.data_sufficiency import check_sufficiency_from_file, check_sufficiency
import pandas as pd


TEST_DATA_DIR = Path(__file__).parent / "test_data"


class TestSufficientFiles:
    """Test files that should pass sufficiency check."""
    
    def test_retail_store(self):
        result = check_sufficiency_from_file(TEST_DATA_DIR / "retail_store.xlsx")
        assert result["status"] == "sufficient", f"Expected sufficient, got: {result}"
    
    def test_ecommerce_sales(self):
        result = check_sufficiency_from_file(TEST_DATA_DIR / "ecommerce_sales.xlsx")
        assert result["status"] == "sufficient", f"Expected sufficient, got: {result}"
    
    def test_warehouse(self):
        result = check_sufficiency_from_file(TEST_DATA_DIR / "warehouse.xlsx")
        assert result["status"] == "sufficient", f"Expected sufficient, got: {result}"
    


class TestInsufficientFiles:
    """Test files that should fail sufficiency check."""
    
    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = check_sufficiency(df)
        assert result["status"] == "insufficient", f"Expected insufficient, got: {result}"
    
    def test_single_row(self):
        df = pd.DataFrame({
            "product_id": ["P001"],
            "quantity": [10],
            "date": ["2024-01-01"]
        })
        result = check_sufficiency(df)
        assert result["status"] == "insufficient", f"Expected insufficient, got: {result}"
        assert "single record" in result["reason"].lower()
    
    def test_no_product_identifier(self):
        df = pd.DataFrame({
            "quantity": [10, 20, 30],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"]
        })
        result = check_sufficiency(df)
        assert result["status"] == "insufficient", f"Expected insufficient, got: {result}"
        assert "product" in result["reason"].lower()
    
    def test_no_quantity(self):
        df = pd.DataFrame({
            "product_id": ["P001", "P002", "P003"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "name": ["Product A", "Product B", "Product C"]
        })
        result = check_sufficiency(df)
        assert result["status"] == "insufficient", f"Expected insufficient, got: {result}"
        assert "quantity" in result["reason"].lower()
    
    def test_no_time_context(self):
        df = pd.DataFrame({
            "product_id": ["P001", "P002", "P003"],
            "quantity": [10, 20, 30],
            "name": ["Product A", "Product B", "Product C"]
        })
        result = check_sufficiency(df)
        assert result["status"] == "insufficient", f"Expected insufficient, got: {result}"
        assert "time" in result["reason"].lower() or "date" in result["reason"].lower()
    
    def test_single_date_value(self):
        df = pd.DataFrame({
            "product_id": ["P001", "P002", "P003"],
            "quantity": [10, 20, 30],
            "date": ["2024-01-01", "2024-01-01", "2024-01-01"]
        })
        result = check_sufficiency(df)
        assert result["status"] == "insufficient", f"Expected insufficient, got: {result}"
        assert "single" in result["reason"].lower() or "two" in result["reason"].lower()


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_minimal_sufficient_data(self):
        df = pd.DataFrame({
            "product_id": ["P001", "P002"],
            "quantity": [10, 20],
            "date": ["2024-01-01", "2024-01-02"]
        })
        result = check_sufficiency(df)
        assert result["status"] == "sufficient", f"Expected sufficient, got: {result}"
    
    def test_all_zero_quantities(self):
        df = pd.DataFrame({
            "product_id": ["P001", "P002", "P003"],
            "quantity": [0, 0, 0],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"]
        })
        result = check_sufficiency(df)
        assert result["status"] == "insufficient", f"Expected insufficient, got: {result}"
    
    def test_mixed_zero_and_nonzero(self):
        df = pd.DataFrame({
            "product_id": ["P001", "P002", "P003"],
            "quantity": [0, 10, 0],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"]
        })
        result = check_sufficiency(df)
        assert result["status"] == "sufficient", f"Expected sufficient, got: {result}"


class TestOutputFormat:
    """Test that output format is correct."""
    
    def test_sufficient_output_format(self):
        df = pd.DataFrame({
            "product_id": ["P001", "P002"],
            "quantity": [10, 20],
            "date": ["2024-01-01", "2024-01-02"]
        })
        result = check_sufficiency(df)
        assert "status" in result
        assert result["status"] == "sufficient"
        assert "explanation" in result
        assert "reason" not in result
    
    def test_insufficient_output_format(self):
        df = pd.DataFrame()
        result = check_sufficiency(df)
        assert "status" in result
        assert result["status"] == "insufficient"
        assert "reason" in result
        assert "explanation" not in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
