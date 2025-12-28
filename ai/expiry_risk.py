"""
Expiry Risk Analysis Module

Assesses the risk of product waste due to expiration.
Compares stock levels against shelf life and sales velocity.
"""

import pandas as pd
from typing import Dict
from pathlib import Path


def load_products_data(data_path: str = "data/products.csv") -> pd.DataFrame:
    """Load product inventory data from CSV."""
    return pd.read_csv(data_path)


def load_sales_data(data_path: str = "data/sales.csv") -> pd.DataFrame:
    """Load historical sales data from CSV."""
    return pd.read_csv(data_path, parse_dates=["date"])


def calculate_sales_velocity(sales_df: pd.DataFrame, product_id: str) -> float:
    """
    Calculate average daily sales rate for a product.
    This is the 'velocity' at which stock moves.
    """
    product_sales = sales_df[sales_df["product_id"] == product_id]
    if product_sales.empty:
        return 0.0
    return product_sales["quantity_sold"].mean()


def estimate_days_to_stockout(
    current_stock: int,
    daily_velocity: float
) -> float:
    """
    Estimate how many days until stock runs out.
    """
    if daily_velocity <= 0:
        return float('inf')  # Stock won't deplete if no sales
    return current_stock / daily_velocity


def calculate_risk_level(
    days_to_expiry: int,
    days_to_stockout: float
) -> str:
    """
    Determine expiry risk level based on simple business logic:
    
    - HIGH: Stock will expire before it sells out
    - MEDIUM: Stock will sell out close to expiry date (within 2 days)
    - LOW: Stock will sell out well before expiry
    """
    if days_to_stockout == float('inf'):
        # No sales happening - high risk of expiry
        return "HIGH"
    
    # Days of buffer between stockout and expiry
    buffer_days = days_to_expiry - days_to_stockout
    
    if buffer_days < 0:
        # Stock expires before it sells out
        return "HIGH"
    elif buffer_days < 2:
        # Cutting it close
        return "MEDIUM"
    else:
        # Comfortable buffer
        return "LOW"


def analyze_expiry_risk(
    product_id: str,
    products_path: str = "data/products.csv",
    sales_path: str = "data/sales.csv"
) -> Dict:
    """
    Analyze expiry risk for a specific product.
    
    Args:
        product_id: Product identifier
        products_path: Path to products CSV
        sales_path: Path to sales CSV
    
    Returns:
        Dictionary containing:
        - risk_level: HIGH, MEDIUM, or LOW
        - days_to_expiry: Days until product expires
        - days_to_stockout: Estimated days until stock depletes
        - current_stock: Current inventory level
        - daily_velocity: Average units sold per day
        - excess_units: Units at risk of expiring (if any)
    """
    products_df = load_products_data(products_path)
    sales_df = load_sales_data(sales_path)
    
    # Get product info
    product = products_df[products_df["product_id"] == product_id]
    if product.empty:
        raise ValueError(f"Product {product_id} not found")
    
    product = product.iloc[0]
    current_stock = int(product["current_stock"])
    days_to_expiry = int(product["days_to_expiry"])
    
    # Calculate velocity and stockout estimate
    daily_velocity = calculate_sales_velocity(sales_df, product_id)
    days_to_stockout = estimate_days_to_stockout(current_stock, daily_velocity)
    
    # Determine risk level
    risk_level = calculate_risk_level(days_to_expiry, days_to_stockout)
    
    # Calculate excess units at risk
    if days_to_stockout > days_to_expiry:
        units_sellable = round(daily_velocity * days_to_expiry)
        excess_units = max(0, current_stock - units_sellable)
    else:
        excess_units = 0
    
    return {
        "risk_level": risk_level,
        "days_to_expiry": days_to_expiry,
        "days_to_stockout": round(days_to_stockout, 1) if days_to_stockout != float('inf') else None,
        "current_stock": current_stock,
        "daily_velocity": round(daily_velocity, 1),
        "excess_units": excess_units
    }
