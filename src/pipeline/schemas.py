from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("org_id", StringType(), False),
        StructField("org_name", StringType(), True),
        StructField("industry", StringType(), True),
        StructField("hq_region", StringType(), True),
        StructField("plan_tier", StringType(), True),
        StructField("is_enterprise", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("sales_rep", StringType(), True),
        StructField("lifecycle_stage", StringType(), True),
        StructField("marketing_source", StringType(), True),
        StructField("nps_score", DoubleType(), True),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("org_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("role", StringType(), True),
        StructField("active", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("last_login", StringType(), True),
    ]
)

RESOURCES_SCHEMA = StructType(
    [
        StructField("resource_id", StringType(), False),
        StructField("org_id", StringType(), True),
        StructField("service", StringType(), True),
        StructField("region", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("state", StringType(), True),
        StructField("tags_json", StringType(), True),
    ]
)

SUPPORT_TICKETS_SCHEMA = StructType(
    [
        StructField("ticket_id", StringType(), False),
        StructField("org_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("resolved_at", StringType(), True),
        StructField("csat", DoubleType(), True),
        StructField("sla_breached", BooleanType(), True),
    ]
)

MARKETING_SCHEMA = StructType(
    [
        StructField("touch_id", StringType(), False),
        StructField("org_id", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("clicked", StringType(), True),
        StructField("converted", StringType(), True),
    ]
)

NPS_SCHEMA = StructType(
    [
        StructField("org_id", StringType(), False),
        StructField("survey_date", StringType(), False),
        StructField("nps_score", DoubleType(), True),
        StructField("comment", StringType(), True),
    ]
)

BILLING_SCHEMA = StructType(
    [
        StructField("invoice_id", StringType(), False),
        StructField("org_id", StringType(), False),
        StructField("month", StringType(), False),
        StructField("subtotal", DoubleType(), True),
        StructField("credits", DoubleType(), True),
        StructField("taxes", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("exchange_rate_to_usd", DoubleType(), True),
    ]
)

USAGE_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("org_id", StringType(), True),
        StructField("resource_id", StringType(), True),
        StructField("service", StringType(), True),
        StructField("region", StringType(), True),
        StructField("metric", StringType(), True),
        StructField("value", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("cost_usd_increment", DoubleType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("carbon_kg", DoubleType(), True),
        StructField("genai_tokens", DoubleType(), True),
    ]
)

BATCH_SCHEMAS = {
    "customers_orgs": CUSTOMERS_SCHEMA,
    "users": USERS_SCHEMA,
    "resources": RESOURCES_SCHEMA,
    "support_tickets": SUPPORT_TICKETS_SCHEMA,
    "marketing_touches": MARKETING_SCHEMA,
    "nps_surveys": NPS_SCHEMA,
    "billing_monthly": BILLING_SCHEMA,
}
