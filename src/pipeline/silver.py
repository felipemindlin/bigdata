from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import BRONZE_BATCH_DIR, BRONZE_STREAM_DIR, QUARANTINE_DIR, SILVER_DIR

VALID_SEVERITIES = ["critical", "high", "medium", "low"]


def _normalize_usage(df: DataFrame) -> DataFrame:
    value_num = F.regexp_replace(F.col("value").cast("string"), ",", ".").cast("double")
    return (
        df.withColumn("value_num", value_num)
        .withColumn("usage_date", F.to_date("event_ts"))
        .withColumn("cost_usd_increment", F.coalesce(F.col("cost_usd_increment"), F.lit(0.0)))
        .withColumn("genai_tokens", F.coalesce(F.col("genai_tokens"), F.lit(0.0)))
        .withColumn("carbon_kg", F.coalesce(F.col("carbon_kg"), F.lit(0.0)))
    )


def run_silver(spark) -> None:
    """Silver de usage: calidad, pivot de metricas EAV, enriquecimiento y features diarias."""
    usage = spark.read.parquet(str(BRONZE_STREAM_DIR / "usage_events"))
    customers = spark.read.parquet(str(BRONZE_BATCH_DIR / "customers_orgs"))
    resources = spark.read.parquet(str(BRONZE_BATCH_DIR / "resources"))

    usage = usage.dropDuplicates(["event_id"])
    usage = _normalize_usage(usage)

    rule_event_id_ok = F.col("event_id").isNotNull()
    rule_cost_ok = F.col("cost_usd_increment") >= F.lit(-0.01)
    rule_unit_ok = (~F.col("value_num").isNotNull()) | F.col("unit").isNotNull()

    quality_df = (
        usage.withColumn("rule_event_id_ok", rule_event_id_ok)
        .withColumn("rule_cost_ok", rule_cost_ok)
        .withColumn("rule_unit_ok", rule_unit_ok)
    )

    quarantine = quality_df.filter(~(rule_event_id_ok & rule_cost_ok & rule_unit_ok)).withColumn(
        "quality_issue",
        F.concat_ws(
            ";",
            F.when(~rule_event_id_ok, F.lit("event_id_null")).otherwise(F.lit(None)),
            F.when(~rule_cost_ok, F.lit("cost_lt_-0.01")).otherwise(F.lit(None)),
            F.when(~rule_unit_ok, F.lit("unit_null_with_value")).otherwise(F.lit(None)),
        ),
    )

    valid = quality_df.filter(rule_event_id_ok & rule_cost_ok & rule_unit_ok)

    p99 = valid.approxQuantile("cost_usd_increment", [0.99], 0.05)[0] if valid.count() else 0.0
    valid = valid.withColumn("cost_anomaly_flag", F.col("cost_usd_increment") > F.lit(p99 * 2.0))

    silver_usage = valid.join(
        customers.select("org_id", "industry", "plan_tier", "hq_region"),
        on="org_id",
        how="left",
    ).join(
        resources.select(
            "resource_id",
            F.col("state").alias("resource_state"),
            F.col("region").alias("resource_region"),
        ),
        on="resource_id",
        how="left",
    )

    metric_sum = lambda name: F.sum(
        F.when(F.col("metric") == name, F.col("value_num")).otherwise(F.lit(0.0))
    )

    silver_daily_features = silver_usage.groupBy("org_id", "service", "usage_date").agg(
        F.sum("cost_usd_increment").alias("daily_cost_usd"),
        metric_sum("requests").alias("requests"),
        metric_sum("cpu_hours").alias("cpu_hours"),
        metric_sum("storage_gb_hours").alias("storage_gb_hours"),
        F.sum("genai_tokens").alias("genai_tokens"),
        F.sum("carbon_kg").alias("carbon_kg"),
        F.max("cost_anomaly_flag").alias("has_cost_anomaly"),
    )

    silver_usage.write.mode("overwrite").partitionBy("usage_date").parquet(str(SILVER_DIR / "usage_enriched"))
    silver_daily_features.write.mode("overwrite").partitionBy("usage_date").parquet(
        str(SILVER_DIR / "daily_features")
    )
    quarantine.write.mode("overwrite").partitionBy("ingest_date").parquet(
        str(QUARANTINE_DIR / "silver_quality")
    )

    print(
        f"[silver] usage_enriched={silver_usage.count()} "
        f"daily_features={silver_daily_features.count()} quarantine={quarantine.count()}"
    )


def run_silver_tickets(spark) -> None:
    """Silver de soporte: normaliza tickets, valida severidad/CSAT y aisla invalidos."""
    tickets = spark.read.parquet(str(BRONZE_BATCH_DIR / "support_tickets"))

    norm = (
        tickets.withColumn("ticket_date", F.to_date("created_at"))
        .withColumn("severity", F.lower(F.trim(F.col("severity"))))
        .withColumn("sla_breached", F.coalesce(F.col("sla_breached"), F.lit(False)))
    )

    rule_org_ok = F.col("org_id").isNotNull()
    rule_date_ok = F.col("ticket_date").isNotNull()
    rule_sev_ok = F.col("severity").isin(VALID_SEVERITIES)
    rule_csat_ok = (~F.col("csat").isNotNull()) | (F.col("csat").between(1.0, 5.0))

    flagged = (
        norm.withColumn("rule_org_ok", rule_org_ok)
        .withColumn("rule_date_ok", rule_date_ok)
        .withColumn("rule_sev_ok", rule_sev_ok)
        .withColumn("rule_csat_ok", rule_csat_ok)
    )

    all_ok = rule_org_ok & rule_date_ok & rule_sev_ok & rule_csat_ok
    quarantine = flagged.filter(~all_ok).withColumn(
        "quality_issue",
        F.concat_ws(
            ";",
            F.when(~rule_org_ok, F.lit("org_id_null")).otherwise(F.lit(None)),
            F.when(~rule_date_ok, F.lit("ticket_date_null")).otherwise(F.lit(None)),
            F.when(~rule_sev_ok, F.lit("severity_invalid")).otherwise(F.lit(None)),
            F.when(~rule_csat_ok, F.lit("csat_out_of_range")).otherwise(F.lit(None)),
        ),
    )
    valid = flagged.filter(all_ok)

    valid.write.mode("overwrite").partitionBy("ticket_date").parquet(str(SILVER_DIR / "tickets"))
    quarantine.write.mode("overwrite").parquet(str(QUARANTINE_DIR / "tickets_quality"))

    print(f"[silver] tickets={valid.count()} quarantine={quarantine.count()}")


def run_silver_billing(spark) -> None:
    """Silver de facturacion: normaliza a USD usando exchange_rate_to_usd por factura."""
    billing = spark.read.parquet(str(BRONZE_BATCH_DIR / "billing_monthly"))

    rate = F.coalesce(F.col("exchange_rate_to_usd"), F.lit(1.0))
    subtotal = F.coalesce(F.col("subtotal"), F.lit(0.0))
    credits = F.coalesce(F.col("credits"), F.lit(0.0))
    taxes = F.coalesce(F.col("taxes"), F.lit(0.0))

    norm = (
        billing.withColumn("month_date", F.to_date("month"))
        .withColumn("subtotal_usd", subtotal * rate)
        .withColumn("credits_usd", credits * rate)
        .withColumn("taxes_usd", taxes * rate)
        .withColumn("revenue_usd", (subtotal - credits + taxes) * rate)
    )

    rule_org_ok = F.col("org_id").isNotNull()
    rule_month_ok = F.col("month_date").isNotNull()
    rule_subtotal_ok = subtotal >= F.lit(0.0)

    all_ok = rule_org_ok & rule_month_ok & rule_subtotal_ok
    quarantine = norm.filter(~all_ok).withColumn(
        "quality_issue",
        F.concat_ws(
            ";",
            F.when(~rule_org_ok, F.lit("org_id_null")).otherwise(F.lit(None)),
            F.when(~rule_month_ok, F.lit("month_null")).otherwise(F.lit(None)),
            F.when(~rule_subtotal_ok, F.lit("subtotal_negative")).otherwise(F.lit(None)),
        ),
    )
    valid = norm.filter(all_ok)

    valid.write.mode("overwrite").parquet(str(SILVER_DIR / "billing_norm"))
    quarantine.write.mode("overwrite").parquet(str(QUARANTINE_DIR / "billing_quality"))

    print(f"[silver] billing_norm={valid.count()} quarantine={quarantine.count()}")
