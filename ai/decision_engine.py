"""
Decision Engine Module

Combines demand forecast, expiry risk, and supplier data to make inventory decisions.
Outputs a single actionable recommendation with business justification.
"""

import pandas as pd
from typing import Dict
from pathlib import Path

from .demand_forecast import forecast_demand
from .expiry_risk import analyze_expiry_risk


def load_supplier_data(data_path: str = "data/suppliers.csv") -> pd.DataFrame:
    """Load supplier information from CSV."""
    return pd.read_csv(data_path)


def load_products_data(data_path: str = "data/products.csv") -> pd.DataFrame:
    """Load product data from CSV."""
    return pd.read_csv(data_path)


def get_supplier_info(product_id: str, suppliers_path: str = "data/suppliers.csv") -> Dict:
    """Get supplier lead time and minimum order for a product."""
    suppliers_df = load_supplier_data(suppliers_path)
    supplier = suppliers_df[suppliers_df["product_id"] == product_id]
    
    if supplier.empty:
        return {"lead_time": 3, "min_order": 20}  # Defaults
    
    supplier = supplier.iloc[0]
    return {
        "lead_time": int(supplier["lead_time_days"]),
        "min_order": int(supplier["min_order_qty"])
    }


def calculate_reorder_quantity(
    forecast_demand: int,
    current_stock: int,
    min_order: int,
    safety_buffer: float = 1.2
) -> int:
    """
    Calculate optimal reorder quantity.
    
    Ensures we have enough to cover demand plus a safety buffer.
    Respects minimum order quantities.
    """
    target_stock = round(forecast_demand * safety_buffer)
    needed = max(0, target_stock - current_stock)
    
    if needed == 0:
        return 0
    
    # Round up to minimum order quantity
    return max(min_order, needed)


def generate_explanation(
    decision: str,
    days_to_stockout: float,
    days_to_expiry: int,
    lead_time: int,
    risk_level: str,
    quantity: int = 0
) -> str:
    """
    Generate a concise business explanation for the decision.
    Uses short, confident sentences.
    """
    if decision == "REORDER":
        if days_to_stockout and days_to_stockout < lead_time + 1:
            return f"Stock depletes in {round(days_to_stockout)} days. Supplier delivers in {lead_time} days. Order now to avoid lost sales."
        else:
            return f"Stock running low. Demand is steady. Reorder {quantity} units to maintain availability."
    
    elif decision == "DISCOUNT":
        return f"Expiry in {days_to_expiry} days. Stock exceeds demand. Discount to reduce waste."
    
    else:  # WAIT
        return f"Stock levels healthy. No action needed. Next review in 3 days."


def make_decision(
    demand_data: Dict,
    risk_data: Dict,
    supplier_info: Dict
) -> Dict:
    """
    Apply business rules to determine the best action.
    
    Decision Logic:
    1. If HIGH expiry risk and excess stock → DISCOUNT
    2. If stock will run out before supplier can deliver → REORDER
    3. If stock is low relative to demand → REORDER
    4. Otherwise → WAIT
    """
    risk_level = risk_data["risk_level"]
    days_to_stockout = risk_data["days_to_stockout"]
    days_to_expiry = risk_data["days_to_expiry"]
    current_stock = risk_data["current_stock"]
    excess_units = risk_data["excess_units"]
    
    total_demand = demand_data["total_demand"]
    lead_time = supplier_info["lead_time"]
    min_order = supplier_info["min_order"]
    
    # Rule 1: HIGH expiry risk with excess stock → DISCOUNT
    if risk_level == "HIGH" and excess_units > 0:
        return {
            "decision": "DISCOUNT",
            "quantity": excess_units,
            "explanation": generate_explanation(
                "DISCOUNT", days_to_stockout, days_to_expiry, lead_time, risk_level
            )
        }
    
    # Rule 2: Stock will run out soon → REORDER
    if days_to_stockout is not None and days_to_stockout <= lead_time + 2:
        reorder_qty = calculate_reorder_quantity(
            total_demand, current_stock, min_order
        )
        return {
            "decision": "REORDER",
            "quantity": reorder_qty,
            "explanation": generate_explanation(
                "REORDER", days_to_stockout, days_to_expiry, lead_time, risk_level, reorder_qty
            )
        }
    
    # Rule 3: Stock won't cover forecast demand → REORDER
    if current_stock < total_demand * 0.8:
        reorder_qty = calculate_reorder_quantity(
            total_demand, current_stock, min_order
        )
        return {
            "decision": "REORDER",
            "quantity": reorder_qty,
            "explanation": generate_explanation(
                "REORDER", days_to_stockout, days_to_expiry, lead_time, risk_level, reorder_qty
            )
        }
    
    # Rule 4: Everything looks fine → WAIT
    return {
        "decision": "WAIT",
        "quantity": 0,
        "explanation": generate_explanation(
            "WAIT", days_to_stockout, days_to_expiry, lead_time, risk_level
        )
    }


def run_analysis(
    product_id: str,
    products_path: str = "data/products.csv",
    sales_path: str = "data/sales.csv",
    suppliers_path: str = "data/suppliers.csv"
) -> Dict:
    """
    Main entry point for the StockIQ decision engine.
    
    This is the ONLY function the frontend should call.
    
    Args:
        product_id: Product identifier (e.g., "P001")
        products_path: Path to products CSV
        sales_path: Path to sales CSV
        suppliers_path: Path to suppliers CSV
    
    Returns:
        Dictionary containing:
        - forecast: Demand forecast data
        - expiry_risk: Risk assessment
        - decision: Final action (REORDER/DISCOUNT/WAIT)
        - quantity: Quantity for action (if applicable)
        - explanation: Business justification
    """
    # Step 1: Forecast demand
    demand_data = forecast_demand(
        product_id,
        forecast_days=7,
        data_path=sales_path
    )
    
    # Step 2: Analyze expiry risk
    risk_data = analyze_expiry_risk(
        product_id,
        products_path=products_path,
        sales_path=sales_path
    )
    
    # Step 3: Get supplier info
    supplier_info = get_supplier_info(product_id, suppliers_path)
    
    # Step 4: Make decision
    decision_data = make_decision(demand_data, risk_data, supplier_info)
    
    # Compile final result
    return {
        "forecast": {
            "days": demand_data["days"],
            "predicted_units": demand_data["daily_forecast"],
            "total_demand": demand_data["total_demand"],
            "confidence": demand_data["confidence"]
        },
        "expiry_risk": risk_data["risk_level"],
        "risk_details": {
            "days_to_expiry": risk_data["days_to_expiry"],
            "days_to_stockout": risk_data["days_to_stockout"],
            "current_stock": risk_data["current_stock"],
            "daily_velocity": risk_data["daily_velocity"]
        },
        "decision": decision_data["decision"],
        "quantity": decision_data["quantity"],
        "explanation": decision_data["explanation"]
    }
