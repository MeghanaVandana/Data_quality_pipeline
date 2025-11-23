# src/app/dashboard.py
import os
import sys
from dotenv import load_dotenv
load_dotenv()

# Ensure project root is importable when Streamlit runs from elsewhere
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import json
from glob import glob
from datetime import datetime

# import the orchestrator runner
try:
    from src.pipeline.orchestrator import run_pipeline
except Exception as e:
    run_pipeline = None
    ORCHESTRATOR_IMPORT_ERROR = e
else:
    ORCHESTRATOR_IMPORT_ERROR = None

st.set_page_config(layout="wide", page_title="Universal Data Quality Pipeline")

# ----- Configuration you can change -----
COMPANY_COLOR = "#0b5cff"   # change this hex to your preferred company color
# ----------------------------------------

st.title("🧹 Universal Data Quality Pipeline — Live Demo")
st.markdown(
    "Upload any CSV and run the pipeline. The app validates, runs ML checks, "
    "produces cleaned & quarantined outputs, loads cleaned data to MySQL (if configured), "
    "and shows a quality report with visuals and run history."
)

# Make sure directories exist
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/clean", exist_ok=True)
os.makedirs("data/quarantine", exist_ok=True)
os.makedirs("data/samples", exist_ok=True)
os.makedirs("data/reports", exist_ok=True)

# Sidebar controls (unique keys added)
with st.sidebar:
    st.header("Controls")
    st.write("Upload a CSV and press **Run Cleaning Pipeline**")
    show_pie = st.checkbox("Show pie chart", value=True, key="show_pie_checkbox")
    show_failure_bar = st.checkbox("Show failure reasons bar", value=False, key="failure_bar_checkbox")
    preview_rows = st.slider("Preview rows", min_value=5, max_value=200, value=50, key="preview_rows_slider")
    st.markdown("---")
    if ORCHESTRATOR_IMPORT_ERROR:
        st.error("Orchestrator import failed. Pipeline will not run.")
        st.exception(ORCHESTRATOR_IMPORT_ERROR)

uploaded = st.file_uploader("Upload a CSV file", type=["csv"], key="file_uploader")

def safe_read_csv(path, nrows=None):
    try:
        return pd.read_csv(path, nrows=nrows)
    except Exception:
        return pd.DataFrame()

def compute_report_fallback(raw_path, clean_path, quarantine_path):
    raw_df = safe_read_csv(raw_path)
    clean_df = safe_read_csv(clean_path)
    q_df = safe_read_csv(quarantine_path)
    total = len(raw_df)
    cleaned = len(clean_df)
    quarantined = len(q_df)
    pass_rate = round((cleaned / total) * 100, 2) if total > 0 else 0.0
    return {
        "total_records": total,
        "cleaned_records": cleaned,
        "quarantined_records": quarantined,
        "pass_rate_pct": pass_rate,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def load_run_history(reports_dir="data/reports"):
    files = sorted(glob(os.path.join(reports_dir, "report_*.json")), reverse=True)
    rows = []
    for p in files:
        try:
            with open(p, "r") as f:
                js = json.load(f)
            js["_path"] = p
            rows.append(js)
        except Exception:
            continue
    if rows:
        df = pd.DataFrame(rows)
        # normalize common fields
        df = df.rename(columns={
            "total_records":"total",
            "cleaned_records":"cleaned",
            "quarantined_records":"quarantined",
            "pass_rate_pct":"pass_rate_pct"
        })
        # show newest first
        df = df.sort_values(by="timestamp", ascending=False).reset_index(drop=True)
        return df
    return pd.DataFrame(columns=["timestamp","total","cleaned","quarantined","pass_rate_pct","_path"])

def render_gauge(value_pct):
    """
    Try rendering a Plotly gauge. If plotly not available, return False to indicate fallback.
    """
    try:
        import plotly.graph_objects as go
    except Exception:
        return False

    # sanitize
    val = float(value_pct)
    if val < 0: val = 0
    if val > 100: val = 100

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=val,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Overall Success Rate", 'font': {'size': 18}},
        delta={'reference': 80, 'increasing': {'color': "green"}, 'decreasing': {'color': "red"}},
        gauge={
            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "#666"},
            'bar': {'color': COMPANY_COLOR},
            'bgcolor': "white",
            'steps': [
                {'range': [0, 50], 'color': "#f4cccc"},
                {'range': [50, 80], 'color': "#ffe7b3"},
                {'range': [80, 100], 'color': "#d3f9d8"}
            ],
            'threshold': {
                'line': {'color': COMPANY_COLOR, 'width': 4},
                'thickness': 0.75,
                'value': val
            }
        }
    ))
    fig.update_layout(font={'color': "#333", 'family': "Arial"}, margin=dict(l=20,r=20,t=40,b=20), paper_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)
    return True

# -------------------------
# Upload / Run pipeline UI
# -------------------------
if uploaded:
    raw_path = os.path.join("data", "raw", uploaded.name)
    with open(raw_path, "wb") as f:
        f.write(uploaded.getvalue())
    st.success(f"Saved {uploaded.name} -> {raw_path}")

    # preview uploaded file
    try:
        preview_df = pd.read_csv(raw_path)
        st.subheader("Uploaded file preview")
        st.dataframe(preview_df.head(preview_rows))
    except Exception as e:
        st.error(f"Could not preview uploaded file: {e}")

    if st.button("Run Cleaning Pipeline", key="run_pipeline_button"):
        if ORCHESTRATOR_IMPORT_ERROR:
            st.error("Pipeline cannot run because orchestrator import failed. See sidebar for details.")
            st.stop()

        with st.spinner("Running pipeline (rule engine + ML checks + DB load)..."):
            try:
                result = run_pipeline(raw_path)
            except Exception as exc:
                st.error("Pipeline failed:")
                st.exception(exc)
                st.stop()

        st.success("Pipeline finished successfully.")

        # read report, fallback if not present
        report = result.get("report") or {}
        if not report:
            report = compute_report_fallback(raw_path, result.get("clean_path",""), result.get("quarantine_path",""))
            # also save this fallback report to the reports folder for run history
            try:
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                report_path = f"data/reports/report_{timestamp}.json"
                with open(report_path, "w") as f:
                    json.dump(report, f, indent=2)
                result["report_path"] = report_path
            except Exception:
                pass

        total = report.get("total_records", report.get("total", 0))
        cleaned = report.get("cleaned_records", report.get("cleaned", 0))
        quarantined = report.get("quarantined_records", report.get("quarantined", 0))
        pass_rate = report.get("pass_rate_pct", report.get("pass_rate_pct", 0.0))

        # Top metrics row
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        col1.metric("Total rows", f"{int(total):,}")
        col2.metric("Cleaned rows", f"{int(cleaned):,}", delta=f"{int(cleaned) - int(quarantined):,}")
        col3.metric("Quarantined rows", f"{int(quarantined):,}")
        col4.metric("Pass rate", f"{pass_rate} %")

        # Success card (styled with company color)
        st.markdown("---")
        st.markdown("## ⭐ Overall Data Quality Score")

        st.markdown(f"""
        <div style="
            padding: 18px;
            border-radius: 10px;
            background-color: #ffffff;
            border-left: 8px solid {COMPANY_COLOR};
            box-shadow: 0 2px 6px rgba(0,0,0,0.06);
            margin-bottom: 10px;
        ">
            <h2 style="color:{COMPANY_COLOR}; margin:0; font-size:28px;">{pass_rate}% Success Rate</h2>
            <p style="margin:0; color:#666;">Percentage of rows that passed validation and anomaly checks.</p>
        </div>
        """, unsafe_allow_html=True)

        # Render Plotly gauge if available, else fallback to progress bar
        gauge_ok = render_gauge(pass_rate)
        if not gauge_ok:
            st.info("Plotly not available — showing a progress bar instead. (Install `plotly` to enable the gauge.)")
            try:
                st.progress(min(max(pass_rate / 100.0, 0.0), 1.0))
            except Exception:
                pass

        # Status badge
        if pass_rate >= 90:
            st.success(f"Excellent — {pass_rate}% of your data is clean and ready for use.")
        elif pass_rate >= 70:
            st.warning(f"Moderate Quality — {pass_rate}% passed. Some records require review.")
        else:
            st.error(f"Low Quality — Only {pass_rate}% passed. Review the raw data and rules.")

        # Pie chart
        st.markdown("---")
        if show_pie:
            labels = ["Cleaned", "Quarantined"]
            sizes = [cleaned, quarantined]
            if sum(sizes) == 0:
                sizes = [1, 0]
            fig, ax = plt.subplots()
            ax.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=90)
            ax.axis("equal")
            ax.set_title("Clean vs Quarantined")
            st.pyplot(fig)

        # Failure reasons bar
        if show_failure_bar:
            qpath = result.get("quarantine_path")
            qdf = safe_read_csv(qpath)
            if not qdf.empty and "failure_reason" in qdf.columns:
                st.markdown("### Top failure reasons")
                top = qdf["failure_reason"].value_counts().nlargest(10)
                fig2, ax2 = plt.subplots()
                top.plot.barh(ax=ax2)
                ax2.invert_yaxis()
                st.pyplot(fig2)
            else:
                st.info("No failure_reason column available in quarantine file to build failure reasons chart.")

        # Cleaned & quarantine previews
        st.markdown("---")
        st.subheader("Cleaned Data (preview)")
        clean_df = safe_read_csv(result.get("clean_path",""))
        if clean_df.empty:
            st.info("Cleaned data is empty or could not be read.")
        else:
            st.dataframe(clean_df.head(preview_rows))

        st.subheader("Quarantined Records (preview)")
        q_df = safe_read_csv(result.get("quarantine_path",""))
        if q_df.empty:
            st.info("No quarantined records.")
        else:
            st.dataframe(q_df.head(preview_rows))

        # Downloads
        st.markdown("---")
        c1, c2 = st.columns(2)
        try:
            with open(result.get("clean_path",""), "rb") as f:
                c_bytes = f.read()
            c1.download_button("Download cleaned CSV", data=c_bytes, file_name=f"cleaned_{uploaded.name}", key="download_clean")
        except Exception:
            c1.info("Cleaned file not available for download.")

        try:
            with open(result.get("quarantine_path",""), "rb") as f:
                q_bytes = f.read()
            c2.download_button("Download quarantined CSV", data=q_bytes, file_name=f"quarantine_{uploaded.name}", key="download_quarantine")
        except Exception:
            c2.info("Quarantine file not available for download.")

        # Files & report paths
        st.markdown("---")
        st.write("Files written:")
        st.write(f"- Raw: `{raw_path}`")
        st.write(f"- Clean: `{result.get('clean_path')}`")
        st.write(f"- Quarantine: `{result.get('quarantine_path')}`")
        st.write(f"- Report: `{result.get('report_path')}`")

# -------------------------
# Run history (bottom panel)
# -------------------------
st.markdown("---")
st.markdown("## 🕘 Recent pipeline runs (history)")

history_df = load_run_history("data/reports")
if history_df.empty:
    st.info("No historical reports found in data/reports. Runs will be saved there automatically when pipeline runs.")
else:
    # show a concise table
    display_df = history_df[["timestamp","total","cleaned","quarantined","pass_rate_pct"]].copy()
    display_df["timestamp"] = pd.to_datetime(display_df["timestamp"], errors="coerce")
    display_df = display_df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    st.dataframe(display_df.head(20), use_container_width=True, key="history_table")

    # allow user to select a report and download
    sel = st.selectbox("Select a report to download or view", history_df["_path"].tolist(), key="select_report")
    if sel:
        try:
            with open(sel, "r") as f:
                rawj = json.load(f)
            st.json(rawj)
            with open(sel, "rb") as f:
                sel_bytes = f.read()
            st.download_button("Download selected report (JSON)", data=sel_bytes, file_name=os.path.basename(sel), key="download_report")
        except Exception as e:
            st.error(f"Could not open selected report: {e}")

st.markdown("End of dashboard.")
