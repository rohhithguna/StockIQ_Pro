"""
Data Sufficiency Check Module

Stage 2 validation gate for StockIQ.
Determines whether validated inventory-intent files contain enough usable data for analysis.
"""

import re
from typing import Dict, List, Optional, Tuple
import pandas as pd


PRODUCT_PATTERNS = [
    r"\bproduct\b", r"\bitem\b", r"\bsku\b", r"\bcode\b",
    r"\bitem[_\s]?id\b", r"\bproduct[_\s]?id\b", r"\bdescription\b",
    r"\barticle\b", r"\bbarcode\b", r"\bupc\b", r"\bean\b",
    r"\bproduct[_\s]?name\b", r"\bitem[_\s]?name\b",
    r"\bmaterial\b", r"\bgoods\b", r"\bcommodity\b"
]

QUANTITY_PATTERNS = [
    r"\bquantity\b", r"\bqty\b", r"\bunits\b", r"\bstock\b",
    r"\binventory\b", r"\bsold\b", r"\bsales\b", r"\bavailable\b",
    r"\bon[_\s]?hand\b", r"\bcount\b", r"\bbalance\b", r"\bcurrent[_\s]?stock\b"
]

PRICE_PATTERNS = [
    r"\bprice\b", r"\bcost\b", r"\brevenue\b", r"\btotal\b",
    r"\bamount\b", r"\bvalue\b", r"\bmsrp\b", r"\bretail\b"
]

TIME_PATTERNS = [
    r"\bdate\b", r"\bday\b", r"\bmonth\b", r"\byear\b", r"\bperiod\b",
    r"\btimestamp\b", r"\btime\b", r"\border[_\s]?date\b", r"\bsale[_\s]?date\b",
    r"\btransaction[_\s]?date\b", r"\binvoice[_\s]?date\b",
    r"\bbest[_\s]?before\b", r"\bexpir"
]


def find_matching_columns(df: pd.DataFrame, patterns: List[str]) -> List[str]:
    """Find columns that match any of the given patterns."""
    matching = []
    for col in df.columns:
        col_lower = str(col).lower()
        for pattern in patterns:
            if re.search(pattern, col_lower, re.IGNORECASE):
                matching.append(col)
                break
    return matching


def check_product_identifiers(df: pd.DataFrame) -> Tuple[bool, str, List[str]]:
    """
    Step 1: Check if at least one product identifier column exists.
    
    Returns:
        Tuple of (passed, message, matching_columns)
    """
    matching_cols = find_matching_columns(df, PRODUCT_PATTERNS)
    
    if not matching_cols:
        return False, "No product identifier column found. The file must contain a way to distinguish products (e.g., product ID, SKU, item name).", []
    
    for col in matching_cols:
        non_empty = df[col].dropna()
        if len(non_empty) > 0:
            return True, "", matching_cols
    
    return False, "Product identifier column exists but contains no usable values.", matching_cols


def check_quantity_meaning(df: pd.DataFrame) -> Tuple[bool, str, List[str]]:
    """
    Step 2: Check if meaningful quantity information exists.
    
    Must contain numeric values that represent counts, not just prices.
    
    Returns:
        Tuple of (passed, message, quantity_columns)
    """
    quantity_cols = find_matching_columns(df, QUANTITY_PATTERNS)
    price_cols = find_matching_columns(df, PRICE_PATTERNS)
    
    usable_quantity_cols = []
    
    for col in quantity_cols:
        numeric_values = pd.to_numeric(df[col], errors='coerce')
        valid_values = numeric_values.dropna()
        
        if len(valid_values) == 0:
            continue
        
        if (valid_values == 0).all():
            continue
        
        usable_quantity_cols.append(col)
    
    if usable_quantity_cols:
        return True, "", usable_quantity_cols
    
    if quantity_cols:
        return False, "Quantity columns exist but contain no meaningful numeric data. All values are either missing or zero.", []
    
    has_any_numeric = False
    for col in df.columns:
        if col in price_cols:
            continue
        numeric_values = pd.to_numeric(df[col], errors='coerce')
        valid_count = numeric_values.dropna()
        if len(valid_count) > len(df) * 0.3:
            non_zero = (valid_count != 0).sum()
            if non_zero > 0:
                has_any_numeric = True
                usable_quantity_cols.append(col)
    
    if has_any_numeric:
        return True, "", usable_quantity_cols
    
    if price_cols and not quantity_cols:
        return False, "This file contains price or revenue data but no inventory quantity information.", []
    
    return False, "No quantity or inventory count information found. Please upload a file with stock levels or units sold.", []


def check_time_context(df: pd.DataFrame) -> Tuple[bool, str, List[str]]:
    """
    Step 3: Check if time context exists and varies.
    
    Requires at least 2 distinct time points.
    
    Returns:
        Tuple of (passed, message, time_columns)
    """
    time_cols = find_matching_columns(df, TIME_PATTERNS)
    
    if not time_cols:
        return False, "No date or time information found. The file must contain time-based data to analyze trends.", []
    
    for col in time_cols:
        try:
            if pd.api.types.is_numeric_dtype(df[col]):
                unique_values = df[col].dropna().nunique()
            else:
                parsed_dates = pd.to_datetime(df[col], errors='coerce')
                unique_values = parsed_dates.dropna().nunique()
            
            if unique_values >= 2:
                return True, "", time_cols
        except:
            continue
    
    if time_cols:
        return False, "Time column exists but contains only a single date or period. At least two distinct time points are needed for trend analysis.", time_cols
    
    return False, "No usable time context found in the data.", []


def check_data_volume(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Step 4: Check if data volume is minimally usable.
    
    Requires more than one record.
    
    Returns:
        Tuple of (passed, message)
    """
    if len(df) == 0:
        return False, "The file contains no data rows."
    
    if len(df) == 1:
        return False, "The file contains only a single record. Multiple records are needed for meaningful analysis."
    
    return True, ""


def check_logical_consistency(df: pd.DataFrame, quantity_cols: List[str]) -> Tuple[bool, str]:
    """
    Step 5: Perform lightweight sanity checks.
    
    Returns:
        Tuple of (passed, message)
    """
    for col in quantity_cols:
        numeric_values = pd.to_numeric(df[col], errors='coerce')
        negative_count = (numeric_values < 0).sum()
        if negative_count > len(df) * 0.5:
            return False, f"Quantity data contains too many negative values, which may indicate incorrect data formatting."
    
    time_cols = find_matching_columns(df, TIME_PATTERNS)
    for col in time_cols:
        try:
            parsed_dates = pd.to_datetime(df[col], errors='coerce')
            valid_dates = parsed_dates.dropna()
            if len(valid_dates) > 0:
                min_date = valid_dates.min()
                max_date = valid_dates.max()
                if min_date.year < 1900 or max_date.year > 2100:
                    return False, "Date values appear to be outside a reasonable range (1900-2100)."
        except:
            continue
    
    return True, ""


def check_sufficiency(df: pd.DataFrame) -> Dict:
    """
    Main entry point for data sufficiency validation.
    
    Runs all sufficiency checks and returns structured result.
    
    Args:
        df: DataFrame to validate.
    
    Returns:
        Dict with validation result:
        - If sufficient: {"status": "sufficient", "explanation": "..."}
        - If insufficient: {"status": "insufficient", "reason": "..."}
    """
    if df is None or df.empty:
        return {
            "status": "insufficient",
            "reason": "The file contains no data to analyze."
        }
    
    passed, message = check_data_volume(df)
    if not passed:
        return {"status": "insufficient", "reason": message}
    
    passed, message, product_cols = check_product_identifiers(df)
    if not passed:
        return {"status": "insufficient", "reason": message}
    
    passed, message, quantity_cols = check_quantity_meaning(df)
    if not passed:
        return {"status": "insufficient", "reason": message}
    
    passed, message, time_cols = check_time_context(df)
    if not passed:
        return {"status": "insufficient", "reason": message}
    
    passed, message = check_logical_consistency(df, quantity_cols)
    if not passed:
        return {"status": "insufficient", "reason": message}
    
    return {
        "status": "sufficient",
        "explanation": "Data contains product identifiers, quantity information, and time context suitable for inventory analysis."
    }


def check_sufficiency_from_file(file_path: str) -> Dict:
    """
    Check sufficiency from a file path.
    
    Args:
        file_path: Path to Excel file.
    
    Returns:
        Same structure as check_sufficiency().
    """
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        return check_sufficiency(df)
    except Exception as e:
        return {
            "status": "insufficient",
            "reason": "Unable to read the file for sufficiency analysis."
        }
