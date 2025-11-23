import pandas as pd
import yaml
import os

from .schema_detector import detect_schema

def load_default_rules(path="config/default_rules.yml"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Rules file not found: {path}")

    with open(path, "r") as f:
        return yaml.safe_load(f)

def apply_rules(df, rules):
    if isinstance(df, str):
        df = pd.read_csv(df)

    if df.empty:
        return df, pd.DataFrame()

    schema = detect_schema(df)
    quarantined = []
    valid_mask = pd.Series(True, index=df.index)

    for col, col_type in schema.items():
        # simple baseline rules: drop nulls for string
        if df[col].isnull().any():
            mask = df[col].isnull()
            for idx in df[mask].index:
                r = df.loc[idx].to_dict()
                r["failure_reason"] = f"{col}: NULL not allowed"
                quarantined.append(r)
            valid_mask &= ~mask

    df_good = df[valid_mask].reset_index(drop=True)
    df_bad = pd.DataFrame(quarantined)

    return df_good, df_bad
