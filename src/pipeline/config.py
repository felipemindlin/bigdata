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
    "resources": {
        "path": LANDING_DIR / "resources.csv",
        "key_cols": ["resource_id"],
    },
    "support_tickets": {
        "path": LANDING_DIR / "support_tickets.csv",
        "key_cols": ["ticket_id"],
    },
    "marketing_touches": {
        "path": LANDING_DIR / "marketing_touches.csv",
        "key_cols": ["touch_id"],
    },
    "nps_surveys": {
        "path": LANDING_DIR / "nps_surveys.csv",
        "key_cols": ["org_id", "survey_date"],
    },
    "billing_monthly": {
        "path": LANDING_DIR / "billing_monthly.csv",
        "key_cols": ["invoice_id"],
    },
}

USAGE_STREAM_DIR = LANDING_DIR / "usage_events_stream"

GENAI_COST_PER_1K_TOKENS_USD = 0.002

APP_NAME = "cloud-provider-analytics-final"
