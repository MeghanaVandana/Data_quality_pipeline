# src/pipeline/quarantine.py
import os
import pandas as pd
from datetime import datetime

def _to_dataframe(maybe_df):
    """
    Ensure the input is a pandas.DataFrame.
    If it's a tuple/list, try to unwrap the first DataFrame-like element.
    If it's a dict/list of dicts, convert to DataFrame.
    If it's already a DataFrame, return as-is.
    Otherwise return None.
    """
    if maybe_df is None:
        return None

    # If it's already a DataFrame
    if isinstance(maybe_df, pd.DataFrame):
        return maybe_df

    # If it's a tuple/list, try to find a DataFrame inside or take first element
    if isinstance(maybe_df, (tuple, list)):
        for elem in maybe_df:
            if isinstance(elem, pd.DataFrame):
                return elem
        # fallback: try to convert the first element
        try:
            return pd.DataFrame(maybe_df[0])
        except Exception:
            return None

    # If it's a dict or list-of-dicts, convert to DataFrame
    if isinstance(maybe_df, dict):
        try:
            return pd.DataFrame([maybe_df])
        except Exception:
            return None

    # Last resort: try to coerce via DataFrame constructor
    try:
        return pd.DataFrame(maybe_df)
    except Exception:
        return None


def quarantine_rows(df_bad, df_anomalies, quarantine_dir):
    """
    Combine df_bad and df_anomalies (deduplicated), write to a timestamped CSV
    in `quarantine_dir` and return the path to the file.

    This function is defensive: it will unwrap tuples/lists and coerce dicts
    into DataFrames so pd.concat only receives DataFrame objects.
    """
    os.makedirs(quarantine_dir, exist_ok=True)

    parts = []
    for candidate, name in [(df_bad, "df_bad"), (df_anomalies, "df_anomalies")]:
        df_candidate = _to_dataframe(candidate)
        if df_candidate is not None and len(df_candidate) > 0:
            parts.append(df_candidate)
        else:
            # optional: you can log or print here for debugging
            # print(f"[quarantine] skipping {name} (not a DataFrame or empty)")
            pass

    if not parts:
        # nothing to quarantine - return None
        return None

    # Now it's safe to concatenate
    combined = pd.concat(parts, ignore_index=True).drop_duplicates().reset_index(drop=True)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(quarantine_dir, f"quarantine_{ts}.csv")
    combined.to_csv(out_path, index=False)
    return out_path
