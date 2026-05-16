from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATALAKE_DIR = BASE_DIR / "datalake"

LANDING_DIR = DATALAKE_DIR / "landing"
BRONZE_BATCH_DIR = DATALAKE_DIR / "bronze" / "batch"
BRONZE_STREAM_DIR = DATALAKE_DIR / "bronze" / "stream"
SILVER_DIR = DATALAKE_DIR / "silver"
GOLD_DIR = DATALAKE_DIR / "gold"
QUARANTINE_DIR = DATALAKE_DIR / "quarantine"
CHECKPOINT_DIR = DATALAKE_DIR / "checkpoints"

BATCH_SOURCES = {
    "customers_orgs": {
        "path": LANDING_DIR / "customers_orgs.csv",
        "key_cols": ["org_id"],
    },
    "users": {
        "path": LANDING_DIR / "users.csv",
        "key_cols": ["user_id"],
    },
    "billing_monthly": {
        "path": LANDING_DIR / "billing_monthly.csv",
        "key_cols": ["org_id", "month"],
    },
}

USAGE_STREAM_DIR = LANDING_DIR / "usage_events_stream"

APP_NAME = "cloud-provider-analytics-mvp"
