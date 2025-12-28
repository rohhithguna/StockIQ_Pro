"""
Structure Inference Module

Stage 3 for StockIQ.
Infers column roles from validated data, prepares standardized datasets,
and triggers analytics pipeline.
"""

import re
import os
import tempfile
from typing import Dict, List, Optional, Tuple
import pandas as pd
from pathlib import Path

from .demand_forecast import forecast_demand, load_sales_data
from .expiry_risk import analyze_expiry_risk
from .decision_engine import run_analysis, get_supplier_info


ROLE_PATTERNS = {
    "product_id": [
        r"\bproduct[_\s]?id\b", r"\bitem[_\s]?id\b", r"\bsku\b", r"\bbarcode\b",
        r"\bupc\b", r"\bean\b", r"\barticle\b", r"\bmaterial\b", r"\bitem[_\s]?code\b",
        r"\bproduct\b", r"\bitem\b", r"\bcode\b"
    ],
    "product_name": [
        r"\bproduct[_\s]?name\b", r"\bitem[_\s]?name\b", r"\bdescription\b",
        r"\bname\b", r"\btitle\b"
    ],
    "date": [
        r"\bdate\b", r"\bday\b", r"\btimestamp\b", r"\btime\b",
        r"\btransaction[_\s]?date\b", r"\bsale[_\s]?date\b", r"\border[_\s]?date\b",
        r"\bperiod\b", r"\bmonth\b"
    ],
    "quantity_sold": [
        r"\bsold\b", r"\bsales[_\s]?qty\b", r"\bquantity[_\s]?sold\b",
        r"\bunits[_\s]?sold\b", r"\bqty[_\s]?sold\b"
    ],
    "quantity": [
        r"\bquantity\b", r"\bqty\b", r"\bunits\b", r"\bcount\b"
    ],
    "current_stock": [
        r"\bcurrent[_\s]?stock\b", r"\bstock\b", r"\binventory\b",
        r"\bavailable\b", r"\bon[_\s]?hand\b", r"\bbalance\b"
    ],
    "expiry": [
        r"\bexpir", r"\bbest[_\s]?before\b", r"\bshelf[_\s]?life\b",
        r"\bdays[_\s]?to[_\s]?expir"
    ],
    "price": [
        r"\bprice\b", r"\bcost\b", r"\bvalue\b", r"\bamount\b",
        r"\bmsrp\b", r"\bretail\b"
    ]
}


def match_column_to_role(column_name: str, role: str) -> bool:
    """Check if a column name matches any pattern for a role."""
    col_lower = str(column_name).lower()
    patterns = ROLE_PATTERNS.get(role, [])
    for pattern in patterns:
        if re.search(pattern, col_lower, re.IGNORECASE):
            return True
    return False


def infer_column_roles(df: pd.DataFrame) -> Dict[str, str]:
    """
    Step 1: Infer column roles from DataFrame.
    
    Returns:
        Dict mapping role -> column name
    """
    role_mapping = {}
    used_columns = set()
    
    priority_order = [
        "product_id", "date", "quantity_sold", "quantity",
        "current_stock", "expiry", "product_name", "price"
    ]
    
    for role in priority_order:
        for col in df.columns:
            if col in used_columns:
                continue
            if match_column_to_role(col, role):
                if role == "quantity" and "quantity_sold" in role_mapping:
                    continue
                role_mapping[role] = col
                used_columns.add(col)
                break
    
    if "quantity_sold" not in role_mapping and "quantity" in role_mapping:
        role_mapping["quantity_sold"] = role_mapping["quantity"]
    
    return role_mapping


def validate_required_roles(role_mapping: Dict[str, str]) -> Tuple[bool, str]:
    """
    Validate that all required roles are present.
    
    Returns:
        Tuple of (valid, error_message)
    """
    if "product_id" not in role_mapping:
        return False, "Unable to identify a product identifier column. Please ensure your file has a column for product IDs, SKUs, or item codes."
    
    if "quantity" not in role_mapping and "quantity_sold" not in role_mapping and "current_stock" not in role_mapping:
        return False, "Unable to identify a quantity column. Please ensure your file has a column for units, stock levels, or quantities sold."
    
    if "date" not in role_mapping:
        return False, "Unable to identify a date column. Please ensure your file has a column for dates or time periods."
    
    return True, ""


def standardize_dataframe(df: pd.DataFrame, role_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Step 2: Rename columns to internal standard names.
    """
    rename_map = {}
    for role, col in role_mapping.items():
        rename_map[col] = role
    
    df_std = df.rename(columns=rename_map)
    return df_std


def prepare_sales_csv(df: pd.DataFrame, role_mapping: Dict[str, str], temp_dir: str) -> str:
    """
    Prepare a temporary sales CSV file.
    
    Expected format: product_id, date, quantity_sold
    """
    sales_path = os.path.join(temp_dir, "sales.csv")
    
    product_col = role_mapping.get("product_id")
    date_col = role_mapping.get("date")
    qty_col = role_mapping.get("quantity_sold") or role_mapping.get("quantity") or role_mapping.get("current_stock")
    
    if not all([product_col, date_col, qty_col]):
        return None
    
    sales_df = df[[product_col, date_col, qty_col]].copy()
    sales_df.columns = ["product_id", "date", "quantity_sold"]
    
    sales_df["date"] = pd.to_datetime(sales_df["date"], errors="coerce")
    sales_df["quantity_sold"] = pd.to_numeric(sales_df["quantity_sold"], errors="coerce")
    
    sales_df = sales_df.dropna(subset=["product_id", "date", "quantity_sold"])
    
    sales_df.to_csv(sales_path, index=False)
    return sales_path


def prepare_products_csv(df: pd.DataFrame, role_mapping: Dict[str, str], temp_dir: str) -> str:
    """
    Prepare a temporary products CSV file.
    
    Expected format: product_id, current_stock, days_to_expiry
    """
    products_path = os.path.join(temp_dir, "products.csv")
    
    product_col = role_mapping.get("product_id")
    stock_col = role_mapping.get("current_stock") or role_mapping.get("quantity")
    expiry_col = role_mapping.get("expiry")
    
    if not product_col:
        return None
    
    product_ids = df[product_col].unique()
    
    products_data = []
    for pid in product_ids:
        product_rows = df[df[product_col] == pid]
        
        if stock_col and stock_col in df.columns:
            stock_vals = pd.to_numeric(product_rows[stock_col], errors="coerce").dropna()
            current_stock = int(stock_vals.iloc[-1]) if len(stock_vals) > 0 else 100
        else:
            current_stock = 100
        
        if expiry_col and expiry_col in df.columns:
            expiry_vals = pd.to_numeric(product_rows[expiry_col], errors="coerce").dropna()
            days_to_expiry = int(expiry_vals.iloc[0]) if len(expiry_vals) > 0 else 30
        else:
            days_to_expiry = 30
        
        products_data.append({
            "product_id": pid,
            "current_stock": current_stock,
            "days_to_expiry": days_to_expiry
        })
    
    products_df = pd.DataFrame(products_data)
    products_df.to_csv(products_path, index=False)
    return products_path


def prepare_suppliers_csv(temp_dir: str, product_ids: List) -> str:
    """
    Prepare a default suppliers CSV file.
    """
    suppliers_path = os.path.join(temp_dir, "suppliers.csv")
    
    suppliers_data = []
    for pid in product_ids:
        suppliers_data.append({
            "supplier_id": f"SUP-{pid}",
            "supplier_name": f"Supplier for {pid}",
            "product_id": pid,
            "lead_time_days": 3,
            "min_order_qty": 20
        })
    
    suppliers_df = pd.DataFrame(suppliers_data)
    suppliers_df.to_csv(suppliers_path, index=False)
    return suppliers_path


def validate_analytics_readiness(sales_path: str, products_path: str) -> Tuple[bool, str]:
    """
    Step 4: Confirm datasets meet minimum requirements for analytics.
    """
    try:
        sales_df = pd.read_csv(sales_path, parse_dates=["date"])
        products_df = pd.read_csv(products_path)
    except Exception as e:
        return False, "Unable to prepare data for analysis."
    
    if len(sales_df) < 2:
        return False, "Insufficient sales data to generate meaningful analytics. At least 2 records are required."
    
    if sales_df["date"].nunique() < 2:
        return False, "Sales data must span at least 2 different dates for trend analysis."
    
    if len(products_df) == 0:
        return False, "No products identified in the data."
    
    return True, ""


def run_analytics_for_product(
    product_id: str,
    products_path: str,
    sales_path: str,
    suppliers_path: str
) -> Dict:
    """
    Run full analytics pipeline for a single product.
    """
    try:
        result = run_analysis(
            product_id=str(product_id),
            products_path=products_path,
            sales_path=sales_path,
            suppliers_path=suppliers_path
        )
        return {"product_id": product_id, "success": True, **result}
    except Exception as e:
        return {
            "product_id": product_id,
            "success": False,
            "error": str(e)
        }


def run_stage3_analysis(df: pd.DataFrame) -> Dict:
    """
    Main entry point for Stage 3.
    
    Infers structure, prepares datasets, and triggers analytics.
    
    Args:
        df: Validated DataFrame from Stage 2.
    
    Returns:
        Dict with analysis results or error.
    """
    if df is None or df.empty:
        return {
            "status": "error",
            "reason": "No data available for analysis."
        }
    
    role_mapping = infer_column_roles(df)
    
    valid, error_msg = validate_required_roles(role_mapping)
    if not valid:
        return {"status": "error", "reason": error_msg}
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        sales_path = prepare_sales_csv(df, role_mapping, temp_dir)
        if not sales_path:
            return {
                "status": "error",
                "reason": "Unable to prepare sales data for analysis. Required columns may be missing."
            }
        
        products_path = prepare_products_csv(df, role_mapping, temp_dir)
        if not products_path:
            return {
                "status": "error",
                "reason": "Unable to prepare product data for analysis."
            }
        
        products_df = pd.read_csv(products_path)
        product_ids = products_df["product_id"].tolist()
        
        suppliers_path = prepare_suppliers_csv(temp_dir, product_ids)
        
        valid, error_msg = validate_analytics_readiness(sales_path, products_path)
        if not valid:
            return {"status": "error", "reason": error_msg}
        
        analytics_results = []
        for pid in product_ids[:10]:
            result = run_analytics_for_product(
                str(pid), products_path, sales_path, suppliers_path
            )
            if result["success"]:
                analytics_results.append(result)
        
        if not analytics_results:
            return {
                "status": "error",
                "reason": "Unable to generate analytics for any products. The data may contain inconsistencies."
            }
        
        return {
            "status": "ready",
            "message": "Data successfully analyzed. Dashboard generated.",
            "analytics": {
                "products": analytics_results,
                "total_products": len(product_ids),
                "analyzed_products": len(analytics_results)
            },
            "role_mapping": role_mapping
        }
    
    except Exception as e:
        return {
            "status": "error",
            "reason": f"An error occurred during analysis. Please ensure your data is properly formatted."
        }
    
    finally:
        try:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass
