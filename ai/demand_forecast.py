"""
Demand Forecasting Module

Predicts short-term product demand using historical sales data.
Uses a simple rolling average approach for clarity and explainability.
"""

import pandas as pd
from typing import Dict, List, Tuple
from pathlib import Path


def load_sales_data(data_path: str = "data/sales.csv") -> pd.DataFrame:
    """Load historical sales data from CSV."""
    return pd.read_csv(data_path, parse_dates=["date"])


def calculate_daily_average(sales_df: pd.DataFrame, product_id: str) -> float:
    """Calculate average daily sales for a product."""
    product_sales = sales_df[sales_df["product_id"] == product_id]
    if product_sales.empty:
        return 0.0
    return product_sales["quantity_sold"].mean()


def calculate_trend(sales_df: pd.DataFrame, product_id: str) -> float:
    """
    Calculate sales trend (positive = increasing, negative = decreasing).
    Returns percentage change between first half and second half of period.
    """
    product_sales = sales_df[sales_df["product_id"] == product_id].sort_values("date")
    if len(product_sales) < 4:
        return 0.0
    
    mid_point = len(product_sales) // 2
    first_half_avg = product_sales.iloc[:mid_point]["quantity_sold"].mean()
    second_half_avg = product_sales.iloc[mid_point:]["quantity_sold"].mean()
    
    if first_half_avg == 0:
        return 0.0
    
    return (second_half_avg - first_half_avg) / first_half_avg


def calculate_confidence(sales_df: pd.DataFrame, product_id: str) -> float:
    """
    Calculate confidence score based on data consistency.
    Lower variance = higher confidence.
    """
    product_sales = sales_df[sales_df["product_id"] == product_id]
    if len(product_sales) < 3:
        return 0.3  # Low confidence with insufficient data
    
    mean_sales = product_sales["quantity_sold"].mean()
    std_sales = product_sales["quantity_sold"].std()
    
    if mean_sales == 0:
        return 0.3
    
    # Coefficient of variation (lower = more consistent)
    cv = std_sales / mean_sales
    
    # Convert to confidence score (0 to 1)
    # CV of 0 = confidence 1.0, CV of 1 = confidence 0.5
    confidence = max(0.3, min(1.0, 1.0 - (cv * 0.5)))
    
    return round(confidence, 2)


def forecast_demand(
    product_id: str,
    forecast_days: int = 7,
    data_path: str = "data/sales.csv"
) -> Dict:
    """
    Forecast demand for a product over the next N days.
    
    Args:
        product_id: Product identifier
        forecast_days: Number of days to forecast (default 7)
        data_path: Path to sales CSV file
    
    Returns:
        Dictionary containing:
        - daily_forecast: List of predicted units per day
        - total_demand: Total predicted demand
        - average_daily: Average daily demand
        - trend: Demand trend (positive/negative percentage)
        - confidence: Confidence score (0-1)
    """
    sales_df = load_sales_data(data_path)
    
    # Calculate base metrics
    daily_avg = calculate_daily_average(sales_df, product_id)
    trend = calculate_trend(sales_df, product_id)
    confidence = calculate_confidence(sales_df, product_id)
    
    # Generate daily forecast with trend adjustment
    daily_forecast = []
    for day in range(1, forecast_days + 1):
        # Apply gradual trend adjustment
        trend_factor = 1 + (trend * day / forecast_days)
        predicted = round(daily_avg * trend_factor)
        daily_forecast.append(max(0, predicted))
    
    total_demand = sum(daily_forecast)
    
    return {
        "days": [f"Day {i}" for i in range(1, forecast_days + 1)],
        "daily_forecast": daily_forecast,
        "total_demand": total_demand,
        "average_daily": round(daily_avg, 1),
        "trend": round(trend * 100, 1),  # As percentage
        "confidence": confidence
    }
