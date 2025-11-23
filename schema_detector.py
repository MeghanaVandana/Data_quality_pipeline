import pandas as pd
import os

def detect_schema(obj):
    if isinstance(obj, str):
        obj = pd.read_csv(obj)

    schema = {}
    for col in obj.columns:
        series = obj[col]

        if pd.api.types.is_numeric_dtype(series):
            schema[col] = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(series):
            schema[col] = "datetime"
        elif series.nunique() < 20:
            schema[col] = "categorical"
        else:
            schema[col] = "string"

    return schema
