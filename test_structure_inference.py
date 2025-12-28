"""
Tests for Structure Inference and Analytics

Run with: python3 -m pytest test_structure_inference.py -v
"""

import pytest
from pathlib import Path
from ai.structure_inference import (
    infer_column_roles,
    validate_required_roles,
    run_stage3_analysis
)
import pandas as pd


TEST_DATA_DIR = Path(__file__).parent / "test_data"


class TestColumnRoleInference:
    """Test column role inference logic."""
    
    def test_standard_column_names(self):
        df = pd.DataFrame({
            "product_id": ["P001"],
            "date": ["2024-01-01"],
            "quantity": [10]
        })
        roles = infer_column_roles(df)
        assert "product_id" in roles
        assert "date" in roles
        assert "quantity" in roles or "quantity_sold" in roles
    
    def test_alternate_column_names(self):
        df = pd.DataFrame({
            "sku": ["SKU001"],
            "transaction_date": ["2024-01-01"],
            "units_sold": [10]
        })
        roles = infer_column_roles(df)
        assert "product_id" in roles
        assert "date" in roles
    
    def test_mixed_column_names(self):
        df = pd.DataFrame({
            "item_code": ["ITEM001"],
            "sale_date": ["2024-01-01"],
            "qty": [10],
            "extra_col": ["x"]
        })
        roles = infer_column_roles(df)
        assert "product_id" in roles
        assert "date" in roles


class TestRequiredRoleValidation:
    """Test validation of required roles."""
    
    def test_all_roles_present(self):
        roles = {"product_id": "pid", "date": "dt", "quantity": "qty"}
        valid, msg = validate_required_roles(roles)
        assert valid == True
    
    def test_missing_product_id(self):
        roles = {"date": "dt", "quantity": "qty"}
        valid, msg = validate_required_roles(roles)
        assert valid == False
        assert "product" in msg.lower()
    
    def test_missing_quantity(self):
        roles = {"product_id": "pid", "date": "dt"}
        valid, msg = validate_required_roles(roles)
        assert valid == False
        assert "quantity" in msg.lower()
    
    def test_missing_date(self):
        roles = {"product_id": "pid", "quantity": "qty"}
        valid, msg = validate_required_roles(roles)
        assert valid == False
        assert "date" in msg.lower()


class TestStage3Analysis:
    """Test end-to-end Stage 3 analysis."""
    
    def test_ecommerce_sales_analysis(self):
        """Ecommerce file has order_date so should succeed."""
        result = run_stage3_analysis(pd.read_excel(TEST_DATA_DIR / "ecommerce_sales.xlsx"))
        assert result["status"] == "ready", f"Expected ready, got: {result}"
        assert "analytics" in result
        assert "products" in result["analytics"]
    
    def test_inventory_snapshot_no_dates(self):
        """Inventory snapshots without transaction dates should fail Stage 3."""
        result = run_stage3_analysis(pd.read_excel(TEST_DATA_DIR / "retail_store.xlsx"))
        assert result["status"] == "error"
        assert "date" in result["reason"].lower()
    
    def test_empty_dataframe(self):
        result = run_stage3_analysis(pd.DataFrame())
        assert result["status"] == "error"
    
    def test_missing_product_id_column(self):
        df = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "quantity": [10, 20]
        })
        result = run_stage3_analysis(df)
        assert result["status"] == "error"
        assert "product" in result["reason"].lower()


class TestOutputFormat:
    """Test output format compliance."""
    
    def test_success_output_format(self):
        df = pd.read_excel(TEST_DATA_DIR / "ecommerce_sales.xlsx")
        result = run_stage3_analysis(df)
        assert result["status"] == "ready"
        assert "message" in result
        assert "analytics" in result
    
    def test_error_output_format(self):
        result = run_stage3_analysis(pd.DataFrame())
        assert result["status"] == "error"
        assert "reason" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
