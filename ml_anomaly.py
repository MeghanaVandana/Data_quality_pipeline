from sklearn.ensemble import IsolationForest
import pandas as pd

def ml_anomaly_detection(df, contamination=0.05):
    if df is None or df.empty:
        return df, pd.DataFrame()

    numeric = df.select_dtypes(include="number")
    if numeric.empty:
        return df, pd.DataFrame()

    model = IsolationForest(contamination=contamination, random_state=42)
    model.fit(numeric.fillna(numeric.mean()))

    preds = model.predict(numeric.fillna(numeric.mean()))

    mask_bad = preds == -1
    mask_good = preds == 1

    df_bad = df[mask_bad].copy()
    df_good = df[mask_good].copy()

    if not df_bad.empty:
        df_bad["failure_reason"] = "ML anomaly detected"

    return df_good.reset_index(drop=True), df_bad.reset_index(drop=True)

# backwards compatibility
def detect_anomalies(df, contamination=0.05):
    return ml_anomaly_detection(df, contamination)
