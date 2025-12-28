"""
StockIQ AI Module

Backend decision engine for inventory management.
"""

from .decision_engine import run_analysis
from .data_ingestion import (
    ingest_excel,
    ingest_uploaded_file,
    transform_to_products,
    transform_to_sales,
    get_ingestion_summary,
    ValidationResult
)
from .file_validator import (
    validate_file,
    validate_uploaded_file as validate_uploaded
)
from .data_sufficiency import (
    check_sufficiency,
    check_sufficiency_from_file
)
from .structure_inference import (
    run_stage3_analysis,
    infer_column_roles
)

__all__ = [
    "run_analysis",
    "ingest_excel",
    "ingest_uploaded_file",
    "transform_to_products",
    "transform_to_sales",
    "get_ingestion_summary",
    "ValidationResult",
    "validate_file",
    "validate_uploaded",
    "check_sufficiency",
    "check_sufficiency_from_file",
    "run_stage3_analysis",
    "infer_column_roles"
]
