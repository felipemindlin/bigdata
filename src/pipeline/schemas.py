from pyspark.sql.types import (
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
        StructField("region", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("nps_score", DoubleType(), True),
    ]
)

USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("org_id", StringType(), False),
        StructField("role", StringType(), True),
        StructField("is_active", StringType(), True),
    ]
)

BILLING_SCHEMA = StructType(
    [
        StructField("org_id", StringType(), False),
        StructField("month", StringType(), False),
        StructField("revenue_usd", DoubleType(), True),
        StructField("credits_usd", DoubleType(), True),
        StructField("taxes_usd", DoubleType(), True),
        StructField("currency", StringType(), True),
    ]
)

USAGE_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("org_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("resource_id", StringType(), True),
        StructField("service", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("value", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("cost_usd_increment", DoubleType(), True),
        StructField("requests", DoubleType(), True),
        StructField("cpu_hours", DoubleType(), True),
        StructField("storage_gb_hours", DoubleType(), True),
        StructField("genai_tokens", DoubleType(), True),
        StructField("carbon_kg", DoubleType(), True),
    ]
)

BATCH_SCHEMAS = {
    "customers_orgs": CUSTOMERS_SCHEMA,
    "users": USERS_SCHEMA,
    "billing_monthly": BILLING_SCHEMA,
}
