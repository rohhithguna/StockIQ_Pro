import streamlit as st
import pandas as pd
import time
from ai.file_validator import validate_uploaded_file
from ai.data_sufficiency import check_sufficiency
from ai.structure_inference import run_stage3_analysis

st.set_page_config(
    page_title="StockIQ",
    page_icon="◼",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .block-container {
        padding: 1.5rem 3rem 2rem 3rem;
        max-width: 900px;
    }
    
    #MainMenu, footer, header {visibility: hidden;}
    
    .header-bar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding-bottom: 1.5rem;
        border-bottom: 1px solid #f0f0f0;
        margin-bottom: 2rem;
    }
    
    .logo {
        font-size: 1.1rem;
        font-weight: 600;
        color: #111;
        letter-spacing: -0.02em;
    }
    
    .status-indicator {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        font-size: 0.7rem;
        color: #666;
    }
    
    .status-dot {
        width: 6px;
        height: 6px;
        background: #22c55e;
        border-radius: 50%;
    }
    
    /* Stage indicators - minimal */
    .stages-row {
        display: flex;
        gap: 0.5rem;
        margin-bottom: 2rem;
    }
    
    .stage-pill {
        font-size: 0.65rem;
        font-weight: 500;
        padding: 0.25rem 0.6rem;
        border-radius: 4px;
        background: #f5f5f5;
        color: #888;
    }
    
    .stage-pill.passed {
        background: #dcfce7;
        color: #166534;
    }
    
    .stage-pill.failed {
        background: #fee2e2;
        color: #991b1b;
    }
    
    /* Decision Summary - Primary Section */
    .decision-hero {
        text-align: center;
        padding: 3rem 2rem;
        margin-bottom: 2rem;
    }
    
    .decision-badge {
        display: inline-block;
        font-size: 0.6rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.15em;
        color: #888;
        margin-bottom: 1rem;
    }
    
    .decision-action {
        font-size: 4rem;
        font-weight: 800;
        letter-spacing: -0.03em;
        margin-bottom: 0.75rem;
        display: inline-block;
        padding: 0.25rem 1.5rem;
        border-radius: 12px;
    }
    
    .decision-action.reorder { color: #1d4ed8; background: #dbeafe; }
    .decision-action.discount { color: #b45309; background: #fef3c7; }
    .decision-action.wait { color: #047857; background: #d1fae5; }
    
    .confidence-indicator {
        font-size: 0.7rem;
        font-weight: 500;
        color: #888;
        margin-bottom: 1.5rem;
    }
    
    .confidence-indicator .dot {
        display: inline-block;
        width: 6px;
        height: 6px;
        border-radius: 50%;
        margin-right: 0.4rem;
    }
    
    .confidence-indicator .dot.high { background: #22c55e; }
    .confidence-indicator .dot.medium { background: #f59e0b; }
    .confidence-indicator .dot.low { background: #ef4444; }
    
    .decision-explanation {
        font-size: 1rem;
        color: #555;
        line-height: 1.6;
        max-width: 480px;
        margin: 0 auto 1.5rem auto;
    }
    
    .affected-product {
        font-size: 0.75rem;
        color: #999;
    }
    
    .affected-product strong {
        color: #555;
        font-weight: 500;
    }
    
    /* Error states */
    .error-box {
        background: #fef2f2;
        border: 1px solid #fecaca;
        border-radius: 8px;
        padding: 1.5rem;
        text-align: center;
    }
    
    .error-box .title {
        font-size: 0.9rem;
        font-weight: 600;
        color: #991b1b;
        margin-bottom: 0.5rem;
    }
    
    .error-box .message {
        font-size: 0.85rem;
        color: #7f1d1d;
        line-height: 1.5;
    }
    
    /* Upload prompt */
    .upload-prompt {
        text-align: center;
        padding: 4rem 2rem;
        color: #888;
    }
    
    .upload-prompt .icon {
        font-size: 2.5rem;
        margin-bottom: 1rem;
        opacity: 0.5;
    }
    
    .upload-prompt .title {
        font-size: 1rem;
        font-weight: 500;
        color: #555;
        margin-bottom: 0.5rem;
    }
    
    .upload-prompt .subtitle {
        font-size: 0.8rem;
        color: #999;
    }
    
    .section-label {
        font-size: 0.6rem;
        color: #999;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-bottom: 0.75rem;
    }
    
    .footer-note {
        text-align: center;
        font-size: 0.65rem;
        color: #ccc;
        margin-top: 3rem;
        padding-top: 1.5rem;
        border-top: 1px solid #f5f5f5;
    }
</style>
""", unsafe_allow_html=True)


st.markdown("""
    <div class="header-bar">
        <div class="logo">StockIQ</div>
        <div class="status-indicator">
            <span class="status-dot"></span>
            Ready
        </div>
    </div>
""", unsafe_allow_html=True)

st.markdown('<div class="section-label">Upload File</div>', unsafe_allow_html=True)

uploaded_file = st.file_uploader(
    "Choose an Excel or PDF file",
    type=["xlsx", "xls", "pdf"],
    label_visibility="collapsed"
)

if uploaded_file is not None:
    with st.spinner("Analyzing..."):
        time.sleep(0.3)
        stage1_result = validate_uploaded_file(uploaded_file)
    
    stage1_pass = stage1_result["status"] == "valid"
    stage2_pass = False
    stage3_pass = False
    stage2_result = None
    stage3_result = None
    df = None
    
    if stage1_pass:
        uploaded_file.seek(0)
        try:
            if stage1_result["file_type"] == "excel":
                df = pd.read_excel(uploaded_file, engine='openpyxl')
        except:
            df = None
        
        if df is not None:
            stage2_result = check_sufficiency(df)
            stage2_pass = stage2_result["status"] == "sufficient"
            
            if stage2_pass:
                stage3_result = run_stage3_analysis(df)
                stage3_pass = stage3_result["status"] == "ready"
    
    stages_html = '<div class="stages-row">'
    stages_html += f'<span class="stage-pill {"passed" if stage1_pass else "failed"}">Intent {"✓" if stage1_pass else "✗"}</span>'
    stages_html += f'<span class="stage-pill {"passed" if stage2_pass else ("failed" if stage1_pass else "")}">Sufficiency {"✓" if stage2_pass else ("✗" if stage1_pass and not stage2_pass else "")}</span>'
    stages_html += f'<span class="stage-pill {"passed" if stage3_pass else ("failed" if stage2_pass else "")}">Analysis {"✓" if stage3_pass else ("✗" if stage2_pass and not stage3_pass else "")}</span>'
    stages_html += '</div>'
    st.markdown(stages_html, unsafe_allow_html=True)
    
    if stage3_pass and stage3_result:
        analytics = stage3_result["analytics"]
        products = analytics["products"]
        
        if products:
            product = products[0]
            decision = product.get("decision", "WAIT")
            explanation = product.get("explanation", "")
            product_id = product.get("product_id", "Product")
            forecast = product.get("forecast", {})
            confidence_score = forecast.get("confidence", 0.5)
            
            if confidence_score >= 0.7:
                confidence_level = "High"
                confidence_class = "high"
            elif confidence_score >= 0.5:
                confidence_level = "Medium"
                confidence_class = "medium"
            else:
                confidence_level = "Low"
                confidence_class = "low"
            
            st.markdown(f"""
                <div class="decision-hero">
                    <div class="decision-badge">Recommended Action</div>
                    <div class="decision-action {decision.lower()}">{decision}</div>
                    <div class="confidence-indicator">
                        <span class="dot {confidence_class}"></span>
                        {confidence_level} confidence
                    </div>
                    <div class="decision-explanation">{explanation}</div>
                    <div class="affected-product">
                        Affecting <strong>{product_id}</strong>
                    </div>
                </div>
            """, unsafe_allow_html=True)
            
            # Section 2: Decision Justification (3 signals)
            risk_details = product.get("risk_details", {})
            
            total_demand = forecast.get("total_demand", 0)
            current_stock = risk_details.get("current_stock", 0)
            days_to_stockout = risk_details.get("days_to_stockout")
            days_to_expiry = risk_details.get("days_to_expiry", 30)
            
            stockout_text = f"{days_to_stockout:.0f} days" if days_to_stockout and days_to_stockout != float('inf') else "Not projected"
            
            # Risk classification based on inventory timing vs supplier lead time
            supplier_lead_time = 3  # Default supplier lead time in days
            
            if days_to_stockout is None or days_to_stockout == float('inf'):
                inventory_risk = "LOW"
                risk_explanation = "Stock levels are healthy"
            elif days_to_stockout < supplier_lead_time:
                inventory_risk = "HIGH"
                risk_explanation = "Stock may run out before resupply"
            elif days_to_stockout <= supplier_lead_time + 2:
                inventory_risk = "MEDIUM"
                risk_explanation = "Inventory timing is tight"
            else:
                inventory_risk = "LOW"
                risk_explanation = "Stock levels are healthy"
            
            risk_color = "#991b1b" if inventory_risk == "HIGH" else "#854d0e" if inventory_risk == "MEDIUM" else "#166534"
            
            st.markdown(f"""
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin-bottom: 2rem;">
                    <div style="background: #fafafa; border-radius: 8px; padding: 1.25rem;">
                        <div style="font-size: 0.6rem; color: #999; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.5rem;">Demand</div>
                        <div style="font-size: 1.25rem; font-weight: 600; color: #111; margin-bottom: 0.25rem;">{total_demand} units</div>
                        <div style="font-size: 0.75rem; color: #666;">7-day forecast</div>
                    </div>
                    <div style="background: #fafafa; border-radius: 8px; padding: 1.25rem;">
                        <div style="font-size: 0.6rem; color: #999; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.5rem;">Inventory</div>
                        <div style="font-size: 1.25rem; font-weight: 600; color: #111; margin-bottom: 0.25rem;">{stockout_text}</div>
                        <div style="font-size: 0.75rem; color: #666;">until stockout</div>
                    </div>
                    <div style="background: #fafafa; border-radius: 8px; padding: 1.25rem;">
                        <div style="font-size: 0.6rem; color: #999; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.5rem;">Risk Level</div>
                        <div style="font-size: 1.25rem; font-weight: 600; color: {risk_color}; margin-bottom: 0.25rem;">{inventory_risk}</div>
                        <div style="font-size: 0.75rem; color: #666;">{risk_explanation}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
            
            # Section 3: Data Trust & Validation
            from datetime import datetime
            file_name = uploaded_file.name
            upload_time = datetime.now().strftime("%I:%M %p")
            
            st.markdown(f"""
                <div style="background: #f8fdf8; border: 1px solid #d1fae5; border-radius: 8px; padding: 1rem 1.25rem; margin-bottom: 2rem;">
                    <div style="font-size: 0.6rem; color: #166534; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 0.75rem;">Data Validation</div>
                    <div style="display: flex; flex-direction: column; gap: 0.4rem;">
                        <div style="font-size: 0.8rem; color: #166534; display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 0.9rem;">✓</span> File validated successfully
                        </div>
                        <div style="font-size: 0.8rem; color: #166534; display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 0.9rem;">✓</span> Inventory data detected
                        </div>
                        <div style="font-size: 0.8rem; color: #166534; display: flex; align-items: center; gap: 0.5rem;">
                            <span style="font-size: 0.9rem;">✓</span> Analysis completed
                        </div>
                    </div>
                    <div style="font-size: 0.7rem; color: #888; margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid #d1fae5;">
                        {file_name} · Uploaded at {upload_time}
                    </div>
                </div>
            """, unsafe_allow_html=True)
            
            # Section 4: Minimal Controls
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                if st.button("Re-run Analysis", use_container_width=True):
                    st.rerun()
    
    elif not stage1_pass:
        st.markdown(f"""
            <div class="error-box">
                <div class="title">Unable to Process File</div>
                <div class="message">{stage1_result.get("reason", "This file does not appear to contain inventory or sales data.")}</div>
            </div>
        """, unsafe_allow_html=True)
    
    elif stage1_pass and not stage2_pass and stage2_result:
        st.markdown(f"""
            <div class="error-box">
                <div class="title">Insufficient Data</div>
                <div class="message">{stage2_result.get("reason", "The file does not contain enough information for analysis.")}</div>
            </div>
        """, unsafe_allow_html=True)
    
    elif stage2_pass and not stage3_pass and stage3_result:
        st.markdown(f"""
            <div class="error-box">
                <div class="title">Analysis Not Possible</div>
                <div class="message">{stage3_result.get("reason", "Unable to analyze this data structure.")}</div>
            </div>
        """, unsafe_allow_html=True)
    
    elif df is None and stage1_pass:
        st.markdown("""
            <div class="error-box">
                <div class="title">PDF Not Supported</div>
                <div class="message">PDF analysis requires tabular data extraction. Please upload an Excel file.</div>
            </div>
        """, unsafe_allow_html=True)

else:
    st.markdown("""
        <div class="upload-prompt">
            <div class="icon">📊</div>
            <div class="title">Upload inventory or sales data</div>
            <div class="subtitle">Excel files (.xlsx) supported</div>
        </div>
    """, unsafe_allow_html=True)

st.markdown('<div class="footer-note">StockIQ Decision Engine</div>', unsafe_allow_html=True)
