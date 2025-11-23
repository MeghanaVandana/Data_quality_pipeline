# src/pipeline/orchestrator.py
import os
import yaml
import pandas as pd
from datetime import datetime

from .rule_engine import apply_rules
from .schema_detector import detect_schema
from .ml_anomaly import detect_anomalies
from .quarantine import quarantine_rows

# -------------------------
# Paths (resolve from file)
# -------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "default_rules.yml")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
QUARANTINE_DIR = os.path.join(BASE_DIR, "data", "quarantine")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)

# -------------------------
# Robust CSV loader
# -------------------------
def load_csv_safely(path):
    """
    Try multiple encodings and return a pandas.DataFrame.
    Raises ValueError if none succeed.
    """
    encodings = ["utf-8", "utf-8-sig", "latin1", "cp1252", "ISO-8859-1", "macroman"]
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc, engine="python")
        except Exception:
            continue
    # As a last resort, try reading as binary and decoding manually (very permissive)
    try:
        with open(path, "rb") as f:
            raw = f.read()
        text = raw.decode("utf-8", errors="replace")
        from io import StringIO
        return pd.read_csv(StringIO(text))
    except Exception as e:
        raise ValueError(f"Could not decode CSV file {path}: {e}")

# -------------------------
# Load YAML rules
# -------------------------
def load_default_rules():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Rules file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

# -------------------------
# Main pipeline
# -------------------------
def run_pipeline(path):
    """
    path -> CSV file path (absolute or relative)
    Returns dict with summary and output paths.
    """
    path = os.path.abspath(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")

    # 0. Load CSV robustly
    df = load_csv_safely(path)

    # 1. Detect schema (optional, useful for UI)
    try:
        schema = detect_schema(path)
    except Exception:
        schema = None

    # 2. Load validation rules
    rules = load_default_rules()

    # 3. Apply rule-based validation
    df_clean, df_bad = apply_rules(df, rules)

    # 4. ML anomaly detection (on numeric columns by default)
    df_anomalies = detect_anomalies(df_clean)

    # 5. Quarantine combined bad + anomalies
    quarantine_path = quarantine_rows(df_bad, df_anomalies, QUARANTINE_DIR)

    # 6. Save clean data with timestamp
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    clean_out = os.path.join(CLEAN_DIR, f"clean_output_{ts}.csv")
    df_clean.to_csv(clean_out, index=False)

    return {
        "input_path": path,
        "clean_path": clean_out,
        "quarantine_path": quarantine_path,
        "clean_rows": len(df_clean),
        "bad_rows": len(df_bad),
        "anomaly_rows": len(df_anomalies),
        "schema_sample": schema
    }

# CLI helper
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run data quality pipeline on a CSV")
    parser.add_argument("csv", help="path to CSV file")
    args = parser.parse_args()
    result = run_pipeline(args.csv)
    print(result)
