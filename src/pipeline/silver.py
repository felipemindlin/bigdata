from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import BRONZE_BATCH_DIR, BRONZE_STREAM_DIR, QUARANTINE_DIR, SILVER_DIR


def _normalize_usage(df: DataFrame) -> DataFrame:
    value_num = F.regexp_replace(F.col("value"), ",", ".").cast("double")
    return (
        df.withColumn("value_num", value_num)
        .withColumn("usage_date", F.to_date("event_ts"))
        .withColumn("cost_usd_increment", F.coalesce(F.col("cost_usd_increment"), F.lit(0.0)))
        .withColumn("requests", F.coalesce(F.col("requests"), F.lit(0.0)))
        .withColumn("genai_tokens", F.coalesce(F.col("genai_tokens"), F.lit(0.0)))
        .withColumn("carbon_kg", F.coalesce(F.col("carbon_kg"), F.lit(0.0)))
        .withColumn("cpu_hours", F.coalesce(F.col("cpu_hours"), F.lit(0.0)))
        .withColumn("storage_gb_hours", F.coalesce(F.col("storage_gb_hours"), F.lit(0.0)))
    )


def run_silver(spark) -> None:
    usage = spark.read.parquet(str(BRONZE_STREAM_DIR / "usage_events"))
    customers = spark.read.parquet(str(BRONZE_BATCH_DIR / "customers_orgs"))

    usage = _normalize_usage(usage)

    # Reglas de calidad minimas del parcial 2
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

    # Anomalia simple: costo incremental por encima de p99*2
    p99 = valid.approxQuantile("cost_usd_increment", [0.99], 0.05)[0] if valid.count() else 0.0
    valid = valid.withColumn("cost_anomaly_flag", F.col("cost_usd_increment") > F.lit(p99 * 2.0))

    silver_usage = valid.join(
        customers.select("org_id", "region", "plan"),
        on="org_id",
        how="left",
    )

    silver_daily_features = (
        silver_usage.groupBy("org_id", "service", "usage_date")
        .agg(
            F.sum("cost_usd_increment").alias("daily_cost_usd"),
            F.sum("requests").alias("requests"),
            F.sum("genai_tokens").alias("genai_tokens"),
            F.sum("carbon_kg").alias("carbon_kg"),
            F.sum("cpu_hours").alias("cpu_hours"),
            F.sum("storage_gb_hours").alias("storage_gb_hours"),
            F.max("cost_anomaly_flag").alias("has_cost_anomaly"),
        )
    )

    silver_usage.write.mode("overwrite").partitionBy("usage_date").parquet(str(SILVER_DIR / "usage_enriched"))
    silver_daily_features.write.mode("overwrite").partitionBy("usage_date").parquet(
        str(SILVER_DIR / "daily_features")
    )
    quarantine.write.mode("overwrite").partitionBy("ingest_date").parquet(
        str(QUARANTINE_DIR / "silver_quality")
    )

    print(f"[silver] usage_enriched={silver_usage.count()} daily_features={silver_daily_features.count()} quarantine={quarantine.count()}")
