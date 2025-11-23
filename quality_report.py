import pandas as pd
from datetime import datetime
import os
from ..utils.logger import get_logger

logger = get_logger(__name__)

def generate_report(raw_df, clean_df, quarantine_df, output_dir="data/reports"):
    os.makedirs(output_dir, exist_ok=True)
    total = len(raw_df)
    cleaned = len(clean_df)
    quarantined = len(quarantine_df)
    pass_rate = (cleaned / total * 100) if total > 0 else 0.0

    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_records": int(total),
        "cleaned_records": int(cleaned),
        "quarantined_records": int(quarantined),
        "pass_rate_pct": round(pass_rate, 2)
    }

    path = os.path.join(output_dir, f"report_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json")
    import json
    with open(path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Quality report saved: {path}")
    return report, path
