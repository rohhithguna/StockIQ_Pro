"""
Data Ingestion Module

Enterprise-grade Excel data ingestion with dynamic schema interpretation.
Handles various retail, warehouse, and e-commerce data formats.
"""

import pandas as pd
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import re
from datetime import datetime


# ═══════════════════════════════════════════════════════════════
# COLUMN MATCHING RULES
# ═══════════════════════════════════════════════════════════════

# Patterns for identifying column types (case-insensitive)
# ORDER MATTERS: More specific patterns should come first in the dict
COLUMN_PATTERNS = {
    "product_id": [
        r"product[\s_-]?id", r"sku", r"item[\s_-]?(?:id|code|number)",
        r"upc", r"ean", r"barcode", r"article", r"prod[\s_-]?(?:id|code)",
        r"material[\s_-]?(?:id|number)", r"part[\s_-]?(?:id|number)",
        r"^item$"
    ],
    "product_name": [
        r"product[\s_-]?name", r"item[\s_-]?name", r"description",
        r"product[\s_-]?desc", r"item[\s_-]?desc", r"title",
        r"material[\s_-]?desc", r"article[\s_-]?name"
    ],
    "category": [
        r"category", r"department", r"dept", r"class",
        r"segment", r"family", r"division"
    ],
    "expiry": [
        r"expir", r"exp[\s_-]?date", r"best[\s_-]?before", r"bb[\s_-]?date",
        r"shelf[\s_-]?life", r"use[\s_-]?by", r"days[\s_-]?(?:to[\s_-]?)?expir",
        r"days[\s_-]?until[\s_-]?expir"
    ],
    "sales_quantity": [
        r"sold", r"sales[\s_-]?qty", r"quantity[\s_-]?sold",
        r"units[\s_-]?sold", r"orders", r"shipped"
    ],
    "quantity": [
        r"qty[\s_-]?(?:on[\s_-]?hand)?", r"quantity", r"units", r"count", r"stock",
        r"on[\s_-]?hand", r"available", r"inventory", r"balance",
        r"current[\s_-]?stock", r"in[\s_-]?stock"
    ],
    "date": [
        r"^date$", r"order[\s_-]?date", r"sale[\s_-]?date", r"trans[\s_-]?date",
        r"timestamp", r"datetime", r"period"
    ],
    "price": [
        r"price", r"cost", r"unit[\s_-]?price", r"msrp", r"retail",
        r"selling[\s_-]?price"
    ],
    "supplier": [
        r"supplier", r"vendor", r"manufacturer", r"source",
        r"lead[\s_-]?time", r"delivery[\s_-]?time"
    ]
}


# ═══════════════════════════════════════════════════════════════
# VALIDATION RESULT CLASS
# ═══════════════════════════════════════════════════════════════

class ValidationResult:
    """Encapsulates the result of data validation."""
    
    def __init__(self, valid: bool, message: str, data: Optional[pd.DataFrame] = None,
                 mapping: Optional[Dict] = None, data_type: Optional[str] = None):
        self.valid = valid
        self.message = message
        self.data = data
        self.mapping = mapping or {}
        self.data_type = data_type  # "inventory", "sales", "combined"
    
    def __bool__(self):
        return self.valid


# ═══════════════════════════════════════════════════════════════
# COLUMN MATCHING FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def match_column(column_name: str, patterns: List[str]) -> bool:
    """Check if a column name matches any of the given patterns."""
    column_lower = column_name.lower().strip()
    for pattern in patterns:
        if re.search(pattern, column_lower):
            return True
    return False


def identify_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Dynamically identify column types from a DataFrame.
    Returns a mapping of semantic type -> actual column name.
    """
    mapping = {}
    used_columns = set()
    
    for semantic_type, patterns in COLUMN_PATTERNS.items():
        for col in df.columns:
            if col in used_columns:
                continue
            if match_column(str(col), patterns):
                mapping[semantic_type] = col
                used_columns.add(col)
                break
    
    return mapping


def infer_data_type(mapping: Dict[str, str]) -> str:
    """
    Infer what type of data this file contains.
    Returns: "inventory", "sales", or "combined"
    """
    has_stock = "quantity" in mapping
    has_sales = "sales_quantity" in mapping
    has_date = "date" in mapping
    
    if has_sales and has_date:
        return "sales"
    elif has_stock and not has_date:
        return "inventory"
    elif has_stock and has_date:
        return "combined"
    elif has_sales:
        return "sales"
    else:
        return "unknown"


# ═══════════════════════════════════════════════════════════════
# DATA VALIDATION FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def validate_product_identifiers(df: pd.DataFrame, mapping: Dict) -> Tuple[bool, str]:
    """Check if the data has usable product identifiers."""
    if "product_id" in mapping:
        col = mapping["product_id"]
        if df[col].isna().all():
            return False, "Product identifiers are empty."
        return True, ""
    
    if "product_name" in mapping:
        col = mapping["product_name"]
        if df[col].isna().all():
            return False, "Product names are empty."
        return True, ""
    
    return False, "No product identifier found. Please include a column for product ID, SKU, or product name."


def validate_quantities(df: pd.DataFrame, mapping: Dict) -> Tuple[bool, str]:
    """Check if the data has usable quantity information."""
    qty_cols = ["quantity", "sales_quantity"]
    
    for col_type in qty_cols:
        if col_type in mapping:
            col = mapping[col_type]
            # Try to convert to numeric
            try:
                numeric_values = pd.to_numeric(df[col], errors='coerce')
                valid_count = numeric_values.notna().sum()
                if valid_count > 0:
                    return True, ""
            except:
                continue
    
    return False, "No quantity or sales data found. Please include a column with stock levels or units sold."


def validate_for_demand_analysis(df: pd.DataFrame, mapping: Dict) -> Tuple[bool, str]:
    """Check if the data can be used for demand trend analysis."""
    if "date" not in mapping and "sales_quantity" not in mapping:
        # Can still work with inventory-only data (snapshot analysis)
        if "quantity" in mapping:
            return True, ""
        return False, "Unable to analyze demand. Please include either sales history with dates, or current inventory levels."
    return True, ""


def validate_minimum_rows(df: pd.DataFrame) -> Tuple[bool, str]:
    """Check if there's enough data to analyze."""
    if len(df) < 1:
        return False, "The file appears to be empty. Please upload a file with data."
    return True, ""


# ═══════════════════════════════════════════════════════════════
# MAIN INGESTION FUNCTION
# ═══════════════════════════════════════════════════════════════

def ingest_excel(file_path: str) -> ValidationResult:
    """
    Main entry point for Excel data ingestion.
    
    Args:
        file_path: Path to the Excel file
    
    Returns:
        ValidationResult with status, message, and processed data
    """
    # Step 1: File type validation
    path = Path(file_path)
    if not path.exists():
        return ValidationResult(False, "File not found. Please select a valid file.")
    
    if path.suffix.lower() not in ['.xlsx', '.xls']:
        return ValidationResult(
            False, 
            "Unsupported file format. Please upload an Excel file (.xlsx)."
        )
    
    # Step 2: Read the file
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
    except Exception as e:
        return ValidationResult(
            False,
            "Unable to read the Excel file. Please ensure it is not corrupted or password-protected."
        )
    
    # Step 3: Basic structure validation
    valid, msg = validate_minimum_rows(df)
    if not valid:
        return ValidationResult(False, msg)
    
    # Step 4: Dynamic column identification
    mapping = identify_columns(df)
    
    if not mapping:
        return ValidationResult(
            False,
            "Unable to recognize the data structure. Please ensure your file contains columns for products, quantities, or sales."
        )
    
    # Step 5: Validate product identifiers
    valid, msg = validate_product_identifiers(df, mapping)
    if not valid:
        return ValidationResult(False, msg)
    
    # Step 6: Validate quantities
    valid, msg = validate_quantities(df, mapping)
    if not valid:
        return ValidationResult(False, msg)
    
    # Step 7: Validate for analysis capability
    valid, msg = validate_for_demand_analysis(df, mapping)
    if not valid:
        return ValidationResult(False, msg)
    
    # Step 8: Determine data type
    data_type = infer_data_type(mapping)
    
    # Step 9: Success
    return ValidationResult(
        valid=True,
        message=f"Successfully loaded {len(df)} records. Data type: {data_type}.",
        data=df,
        mapping=mapping,
        data_type=data_type
    )


def ingest_uploaded_file(uploaded_file) -> ValidationResult:
    """
    Ingest data from a Streamlit uploaded file object.
    
    Args:
        uploaded_file: Streamlit UploadedFile object
    
    Returns:
        ValidationResult with status, message, and processed data
    """
    if uploaded_file is None:
        return ValidationResult(False, "No file uploaded.")
    
    # Check file extension
    file_name = uploaded_file.name
    if not file_name.lower().endswith(('.xlsx', '.xls')):
        return ValidationResult(
            False,
            "Unsupported file format. Please upload an Excel file (.xlsx)."
        )
    
    # Read the file
    try:
        df = pd.read_excel(uploaded_file, engine='openpyxl')
    except Exception as e:
        return ValidationResult(
            False,
            "Unable to read the Excel file. Please ensure it is not corrupted or password-protected."
        )
    
    # Run validation pipeline
    valid, msg = validate_minimum_rows(df)
    if not valid:
        return ValidationResult(False, msg)
    
    mapping = identify_columns(df)
    
    if not mapping:
        return ValidationResult(
            False,
            "Unable to recognize the data structure. Please ensure your file contains columns for products, quantities, or sales."
        )
    
    valid, msg = validate_product_identifiers(df, mapping)
    if not valid:
        return ValidationResult(False, msg)
    
    valid, msg = validate_quantities(df, mapping)
    if not valid:
        return ValidationResult(False, msg)
    
    valid, msg = validate_for_demand_analysis(df, mapping)
    if not valid:
        return ValidationResult(False, msg)
    
    data_type = infer_data_type(mapping)
    
    return ValidationResult(
        valid=True,
        message=f"Successfully loaded {len(df)} records.",
        data=df,
        mapping=mapping,
        data_type=data_type
    )


# ═══════════════════════════════════════════════════════════════
# DATA TRANSFORMATION FOR DECISION ENGINE
# ═══════════════════════════════════════════════════════════════

def transform_to_products(df: pd.DataFrame, mapping: Dict) -> pd.DataFrame:
    """
    Transform ingested data to the format expected by the decision engine.
    Creates a standardized products dataframe.
    """
    result = pd.DataFrame()
    
    # Product ID
    if "product_id" in mapping:
        result["product_id"] = df[mapping["product_id"]].astype(str)
    elif "product_name" in mapping:
        # Generate IDs from names
        result["product_id"] = ["P" + str(i+1).zfill(3) for i in range(len(df))]
    
    # Product Name
    if "product_name" in mapping:
        result["name"] = df[mapping["product_name"]]
    else:
        result["name"] = result["product_id"]
    
    # Category
    if "category" in mapping:
        result["category"] = df[mapping["category"]]
    else:
        result["category"] = "General"
    
    # Current Stock
    if "quantity" in mapping:
        result["current_stock"] = pd.to_numeric(df[mapping["quantity"]], errors='coerce').fillna(0).astype(int)
    else:
        result["current_stock"] = 50  # Default
    
    # Days to Expiry
    if "expiry" in mapping:
        expiry_col = df[mapping["expiry"]]
        if pd.api.types.is_numeric_dtype(expiry_col):
            result["days_to_expiry"] = expiry_col.fillna(30).astype(int)
        else:
            # Try to parse as date and calculate days
            try:
                expiry_dates = pd.to_datetime(expiry_col, errors='coerce')
                today = datetime.now()
                result["days_to_expiry"] = (expiry_dates - today).dt.days.fillna(30).astype(int)
            except:
                result["days_to_expiry"] = 30
    else:
        result["days_to_expiry"] = 30  # Default
    
    # Price
    if "price" in mapping:
        result["unit_price"] = pd.to_numeric(df[mapping["price"]], errors='coerce').fillna(0)
    else:
        result["unit_price"] = 0
    
    return result


def transform_to_sales(df: pd.DataFrame, mapping: Dict) -> pd.DataFrame:
    """
    Transform ingested data to sales format for demand forecasting.
    """
    result = pd.DataFrame()
    
    # Date
    if "date" in mapping:
        result["date"] = pd.to_datetime(df[mapping["date"]], errors='coerce')
    else:
        # Generate dates based on row index (assume daily data)
        result["date"] = pd.date_range(end=datetime.now(), periods=len(df), freq='D')
    
    # Product ID
    if "product_id" in mapping:
        result["product_id"] = df[mapping["product_id"]].astype(str)
    elif "product_name" in mapping:
        result["product_id"] = ["P" + str(i+1).zfill(3) for i in range(len(df))]
    
    # Quantity Sold
    if "sales_quantity" in mapping:
        result["quantity_sold"] = pd.to_numeric(df[mapping["sales_quantity"]], errors='coerce').fillna(0).astype(int)
    elif "quantity" in mapping:
        result["quantity_sold"] = pd.to_numeric(df[mapping["quantity"]], errors='coerce').fillna(0).astype(int)
    
    return result


def get_ingestion_summary(result: ValidationResult) -> Dict:
    """
    Generate a human-readable summary of what was ingested.
    """
    if not result.valid:
        return {
            "success": False,
            "message": result.message,
            "details": None
        }
    
    mapping = result.mapping
    identified = []
    
    if "product_id" in mapping or "product_name" in mapping:
        identified.append("Product identifiers")
    if "quantity" in mapping:
        identified.append("Stock levels")
    if "sales_quantity" in mapping:
        identified.append("Sales data")
    if "date" in mapping:
        identified.append("Date/time information")
    if "expiry" in mapping:
        identified.append("Expiry dates")
    if "category" in mapping:
        identified.append("Product categories")
    if "price" in mapping:
        identified.append("Pricing")
    
    return {
        "success": True,
        "message": result.message,
        "records": len(result.data),
        "data_type": result.data_type,
        "identified_fields": identified,
        "column_mapping": {k: v for k, v in mapping.items()}
    }
