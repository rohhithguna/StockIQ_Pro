"""
File Intent Validator Module

Stage 1 validation gate for StockIQ.
Accepts or rejects uploaded files based on domain signal detection.
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Union
import pandas as pd

try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False


SIGNAL_PATTERNS = {
    "quantity": [
        r"\bquantity\b", r"\bqty\b", r"\bunits\b", r"\bstock\b",
        r"\binventory\b", r"\bsold\b", r"\bsales\b", r"\bavailable\b",
        r"\bon[_\s]?hand\b", r"\bcount\b", r"\bbalance\b", r"\bcurrent[_\s]?stock\b"
    ],
    "time": [
        r"\bdate\b", r"\bday\b", r"\bmonth\b", r"\byear\b", r"\bperiod\b",
        r"\binvoice[_\s]?date\b", r"\btransaction[_\s]?date\b", r"\btime\b",
        r"\btimestamp\b", r"\border[_\s]?date\b", r"\bsale[_\s]?date\b",
        r"\bbest[_\s]?before\b", r"\bexpir"
    ],
    "product": [
        r"\bproduct\b", r"\bitem\b", r"\bsku\b",
        r"\bitem[_\s]?id\b", r"\bproduct[_\s]?id\b", r"\bitem[_\s]?code\b",
        r"\barticle\b", r"\bbarcode\b", r"\bupc\b", r"\bean\b",
        r"\bproduct[_\s]?name\b", r"\bitem[_\s]?name\b",
        r"\bmaterial\b", r"\bgoods\b", r"\bcommodity\b"
    ]
}

REJECTION_PATTERNS = [
    r"\bemployee\b", r"\bsalary\b", r"\bhr\b", r"\bhuman[_\s]?resource",
    r"\bpayroll\b", r"\bhire[_\s]?date\b", r"\bjob[_\s]?title\b",
    r"\bresume\b", r"\bcandidate\b", r"\bapplicant\b"
]

SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".pdf"}


def validate_file_type(file_path: Union[str, Path]) -> Dict:
    """
    Step 1: Check if file type is supported.
    
    Returns:
        Dict with 'valid' and optionally 'file_type' or 'error'.
    """
    path = Path(file_path) if isinstance(file_path, str) else file_path
    ext = path.suffix.lower()
    
    if ext not in SUPPORTED_EXTENSIONS:
        return {
            "valid": False,
            "error": "Unsupported file format. Please upload an Excel or PDF file containing inventory or sales data."
        }
    
    file_type = "pdf" if ext == ".pdf" else "excel"
    return {"valid": True, "file_type": file_type, "extension": ext}


def extract_excel_content(file_path: Union[str, Path]) -> Dict:
    """
    Step 2a: Extract content from Excel file.
    
    Returns:
        Dict with 'columns', 'sample_values', and 'dataframes'.
    """
    try:
        xlsx = pd.ExcelFile(file_path, engine='openpyxl')
        all_columns = []
        all_values = []
        dataframes = []
        
        for sheet_name in xlsx.sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet_name)
            if df.empty:
                continue
            
            dataframes.append(df)
            all_columns.extend([str(col).lower() for col in df.columns])
            
            for col in df.columns:
                sample = df[col].dropna().head(5).tolist()
                all_values.extend([str(v).lower() for v in sample])
        
        if not dataframes:
            return {"valid": False, "error": "The file appears to be empty. Please upload a file with data."}
        
        combined_df = pd.concat(dataframes, ignore_index=True) if len(dataframes) > 1 else dataframes[0]
        
        return {
            "valid": True,
            "columns": all_columns,
            "sample_values": all_values,
            "dataframe": combined_df,
            "text_content": " ".join(all_columns + all_values)
        }
    except Exception as e:
        return {
            "valid": False,
            "error": "Unable to read the Excel file. Please ensure it is not corrupted or password-protected."
        }


def extract_pdf_content(file_path: Union[str, Path]) -> Dict:
    """
    Step 2b: Extract raw text from PDF file.
    
    Returns:
        Dict with 'text_content'.
    """
    if not PDF_SUPPORT:
        return {
            "valid": False,
            "error": "PDF support is not available. Please install pdfplumber: pip install pdfplumber"
        }
    
    try:
        text_content = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_content.append(page_text.lower())
        
        if not text_content:
            return {"valid": False, "error": "The PDF file appears to be empty or contains only images."}
        
        return {
            "valid": True,
            "text_content": " ".join(text_content),
            "columns": [],
            "dataframe": None
        }
    except Exception as e:
        return {
            "valid": False,
            "error": "Unable to read the PDF file. Please ensure it is not corrupted or password-protected."
        }


def detect_rejection_signals(text_content: str, columns: List[str]) -> bool:
    """
    Detect if file contains HR/finance/non-inventory content.
    
    Returns:
        True if rejection signals are found.
    """
    search_text = text_content + " " + " ".join(columns)
    
    for pattern in REJECTION_PATTERNS:
        if re.search(pattern, search_text, re.IGNORECASE):
            return True
    return False


def detect_signals(text_content: str, columns: List[str]) -> Dict[str, bool]:
    """
    Step 3: Detect domain signals in content.
    
    Checks for at least 2 of 3 signal groups:
    - Quantity indicators
    - Time indicators  
    - Product identifiers
    
    Returns:
        Dict mapping signal group to detection status.
    """
    search_text = text_content + " " + " ".join(columns)
    
    detected = {}
    for signal_group, patterns in SIGNAL_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, search_text, re.IGNORECASE):
                detected[signal_group] = True
                break
        if signal_group not in detected:
            detected[signal_group] = False
    
    return detected


def verify_numeric_data(df: Optional[pd.DataFrame], text_content: str, columns: List[str]) -> Dict:
    """
    Step 4: Verify that numeric data exists.
    
    Returns:
        Dict with 'has_numeric' and 'numeric_columns'.
    """
    if df is not None:
        numeric_columns = []
        quantity_patterns = SIGNAL_PATTERNS["quantity"]
        
        for col in df.columns:
            col_lower = str(col).lower()
            is_quantity_col = any(re.search(p, col_lower, re.IGNORECASE) for p in quantity_patterns)
            
            if is_quantity_col:
                numeric_values = pd.to_numeric(df[col], errors='coerce')
                valid_count = numeric_values.notna().sum()
                if valid_count > 0:
                    numeric_columns.append(col)
        
        if numeric_columns:
            return {"has_numeric": True, "numeric_columns": numeric_columns}
        
        for col in df.columns:
            numeric_values = pd.to_numeric(df[col], errors='coerce')
            valid_count = numeric_values.notna().sum()
            if valid_count > len(df) * 0.3:
                numeric_columns.append(col)
        
        return {"has_numeric": len(numeric_columns) > 0, "numeric_columns": numeric_columns}
    
    numbers = re.findall(r'\b\d+(?:\.\d+)?\b', text_content)
    return {"has_numeric": len(numbers) >= 3, "numeric_columns": []}


def calculate_confidence(signals: Dict[str, bool], numeric_result: Dict) -> str:
    """
    Calculate confidence level based on detected signals and numeric data.
    """
    signal_count = sum(signals.values())
    has_numeric = numeric_result["has_numeric"]
    numeric_col_count = len(numeric_result.get("numeric_columns", []))
    
    if signal_count == 3 and has_numeric and numeric_col_count >= 1:
        return "high"
    elif signal_count >= 2 and has_numeric:
        return "medium"
    else:
        return "low"


def generate_explanation(signals: Dict[str, bool], file_type: str) -> str:
    """
    Generate a human-readable explanation for valid files.
    """
    detected_groups = [k for k, v in signals.items() if v]
    group_names = {
        "quantity": "quantity/stock information",
        "time": "time/date references",
        "product": "product identifiers"
    }
    
    detected_names = [group_names[g] for g in detected_groups]
    
    if len(detected_names) == 3:
        return f"File contains {detected_names[0]}, {detected_names[1]}, and {detected_names[2]} suitable for inventory analysis."
    elif len(detected_names) == 2:
        return f"File contains {detected_names[0]} and {detected_names[1]} suitable for inventory analysis."
    else:
        return "File appears to contain inventory or sales related data."


def generate_rejection_reason(signals: Dict[str, bool], has_numeric: bool) -> str:
    """
    Generate a clear rejection reason for invalid files.
    """
    if not has_numeric:
        return "No numeric quantity or sales data found. Please upload a file containing inventory or sales figures."
    
    missing = [k for k, v in signals.items() if not v]
    
    if "quantity" in missing and "time" in missing:
        return "This document does not appear to be related to inventory or sales data. No quantity or time information found."
    elif "quantity" in missing:
        return "This file does not contain quantity or sales information. Please upload an inventory or sales file."
    elif "time" in missing:
        return "No date or time reference found to analyze inventory trends."
    elif "product" in missing:
        return "No product identifiers found. Please ensure the file contains product or item information."
    
    return "This document does not appear to be related to inventory or sales data."


def validate_file(file_path: Union[str, Path]) -> Dict:
    """
    Main entry point for file validation.
    
    Orchestrates all validation steps and returns a structured result.
    
    Args:
        file_path: Path to the uploaded file.
    
    Returns:
        Dict with validation result:
        - If valid: {"status": "valid", "file_type": "excel"|"pdf", "confidence": "low"|"medium"|"high", "explanation": "..."}
        - If invalid: {"status": "invalid", "reason": "..."}
    """
    path = Path(file_path) if isinstance(file_path, str) else file_path
    
    if not path.exists():
        return {"status": "invalid", "reason": "File not found. Please select a valid file."}
    
    type_result = validate_file_type(path)
    if not type_result["valid"]:
        return {"status": "invalid", "reason": type_result["error"]}
    
    file_type = type_result["file_type"]
    
    if file_type == "excel":
        content_result = extract_excel_content(path)
    else:
        content_result = extract_pdf_content(path)
    
    if not content_result["valid"]:
        return {"status": "invalid", "reason": content_result["error"]}
    
    text_content = content_result.get("text_content", "")
    columns = content_result.get("columns", [])
    
    if detect_rejection_signals(text_content, columns):
        return {
            "status": "invalid",
            "reason": "This document does not appear to be related to inventory or sales data."
        }
    
    signals = detect_signals(text_content, columns)
    
    has_product_and_quantity = signals.get("product", False) and signals.get("quantity", False)
    signal_count = sum(signals.values())
    
    if signal_count < 2 and not has_product_and_quantity:
        return {
            "status": "invalid",
            "reason": generate_rejection_reason(signals, True)
        }
    
    numeric_result = verify_numeric_data(
        content_result.get("dataframe"),
        content_result.get("text_content", ""),
        content_result.get("columns", [])
    )
    
    if not numeric_result["has_numeric"]:
        return {
            "status": "invalid",
            "reason": generate_rejection_reason(signals, False)
        }
    
    confidence = calculate_confidence(signals, numeric_result)
    explanation = generate_explanation(signals, file_type)
    
    return {
        "status": "valid",
        "file_type": file_type,
        "confidence": confidence,
        "explanation": explanation
    }


def validate_uploaded_file(uploaded_file) -> Dict:
    """
    Validate a Streamlit UploadedFile object.
    
    Args:
        uploaded_file: Streamlit UploadedFile object.
    
    Returns:
        Same structure as validate_file().
    """
    if uploaded_file is None:
        return {"status": "invalid", "reason": "No file uploaded."}
    
    file_name = uploaded_file.name
    ext = Path(file_name).suffix.lower()
    
    if ext not in SUPPORTED_EXTENSIONS:
        return {
            "status": "invalid",
            "reason": "Unsupported file format. Please upload an Excel or PDF file containing inventory or sales data."
        }
    
    file_type = "pdf" if ext == ".pdf" else "excel"
    
    try:
        if file_type == "excel":
            df = pd.read_excel(uploaded_file, engine='openpyxl')
            if df.empty:
                return {"status": "invalid", "reason": "The file appears to be empty. Please upload a file with data."}
            
            columns = [str(col).lower() for col in df.columns]
            sample_values = []
            for col in df.columns:
                sample = df[col].dropna().head(5).tolist()
                sample_values.extend([str(v).lower() for v in sample])
            
            text_content = " ".join(columns + sample_values)
            content_result = {
                "valid": True,
                "columns": columns,
                "text_content": text_content,
                "dataframe": df
            }
        else:
            if not PDF_SUPPORT:
                return {
                    "status": "invalid",
                    "reason": "PDF support is not available. Please install pdfplumber: pip install pdfplumber"
                }
            
            import io
            text_content = []
            with pdfplumber.open(io.BytesIO(uploaded_file.read())) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_content.append(page_text.lower())
            
            uploaded_file.seek(0)
            
            if not text_content:
                return {"status": "invalid", "reason": "The PDF file appears to be empty or contains only images."}
            
            content_result = {
                "valid": True,
                "text_content": " ".join(text_content),
                "columns": [],
                "dataframe": None
            }
    except Exception as e:
        return {
            "status": "invalid",
            "reason": f"Unable to read the file. Please ensure it is not corrupted or password-protected."
        }
    
    text_content = content_result.get("text_content", "")
    columns = content_result.get("columns", [])
    
    if detect_rejection_signals(text_content, columns):
        return {
            "status": "invalid",
            "reason": "This document does not appear to be related to inventory or sales data."
        }
    
    signals = detect_signals(text_content, columns)
    
    has_product_and_quantity = signals.get("product", False) and signals.get("quantity", False)
    signal_count = sum(signals.values())
    
    if signal_count < 2 and not has_product_and_quantity:
        return {
            "status": "invalid",
            "reason": generate_rejection_reason(signals, True)
        }
    
    numeric_result = verify_numeric_data(
        content_result.get("dataframe"),
        content_result.get("text_content", ""),
        content_result.get("columns", [])
    )
    
    if not numeric_result["has_numeric"]:
        return {
            "status": "invalid",
            "reason": generate_rejection_reason(signals, False)
        }
    
    confidence = calculate_confidence(signals, numeric_result)
    explanation = generate_explanation(signals, file_type)
    
    return {
        "status": "valid",
        "file_type": file_type,
        "confidence": confidence,
        "explanation": explanation
    }
