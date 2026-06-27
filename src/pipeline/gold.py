from pyspark.sql import functions as F

from .config import GOLD_DIR, SILVER_DIR


def _gold_org_daily_usage(spark) -> int:
    daily_features = spark.read.parquet(str(SILVER_DIR / "daily_features"))
    mart = daily_features.select(
        "org_id",
        "service",
        "usage_date",
        F.col("daily_cost_usd").cast("double"),
        F.col("requests").cast("double"),
        F.col("cpu_hours").cast("double"),
        F.col("storage_gb_hours").cast("double"),
        F.col("genai_tokens").cast("double"),
        F.col("carbon_kg").cast("double"),
        F.col("has_cost_anomaly").cast("boolean"),
    ).dropDuplicates(["org_id", "service", "usage_date"])
    mart.write.mode("overwrite").partitionBy("usage_date").parquet(
        str(GOLD_DIR / "org_daily_usage_by_service")
    )
    return mart.count()


def _gold_revenue_by_org_month(spark) -> int:
    billing = spark.read.parquet(str(SILVER_DIR / "billing_norm"))
    agg = billing.groupBy("org_id", "month_date").agg(
        F.sum("revenue_usd").alias("revenue_usd"),
        F.sum("subtotal_usd").alias("subtotal_usd"),
        F.sum("credits_usd").alias("credits_usd"),
        F.sum("taxes_usd").alias("taxes_usd"),
    )
    mart = agg.withColumn(
        "revenue_breakdown",
        F.create_map(
            F.lit("subtotal"), F.col("subtotal_usd"),
            F.lit("credits"), F.col("credits_usd"),
            F.lit("taxes"), F.col("taxes_usd"),
            F.lit("net"), F.col("revenue_usd"),
        ),
    ).withColumnRenamed("month_date", "month")
    mart.coalesce(1).write.mode("overwrite").parquet(str(GOLD_DIR / "revenue_by_org_month"))
    return mart.count()


def _gold_tickets_by_org_date(spark) -> int:
    tickets = spark.read.parquet(str(SILVER_DIR / "tickets"))
    sev_counts = (
        tickets.groupBy("org_id", "ticket_date", "severity")
        .agg(F.count(F.lit(1)).cast("int").alias("cnt"))
        .groupBy("org_id", "ticket_date")
        .agg(
            F.map_from_entries(
                F.collect_list(F.struct("severity", "cnt"))
            ).alias("counts_by_severity")
        )
    )
    base = tickets.groupBy("org_id", "ticket_date").agg(
        F.count(F.lit(1)).cast("int").alias("total_tickets"),
        F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).cast("int").alias("critical_count"),
        F.avg(F.col("sla_breached").cast("double")).alias("sla_breach_rate"),
        F.avg("csat").alias("csat_avg"),
    )
    mart = base.join(sev_counts, on=["org_id", "ticket_date"], how="left")
    mart.coalesce(1).write.mode("overwrite").parquet(str(GOLD_DIR / "tickets_by_org_date"))
    return mart.count()


def _gold_genai_tokens_by_org_date(spark) -> int:
    daily_features = spark.read.parquet(str(SILVER_DIR / "daily_features"))
    mart = (
        daily_features.filter(F.col("service") == "genai")
        .groupBy("org_id", "usage_date")
        .agg(
            F.sum("genai_tokens").alias("total_tokens"),
            F.sum("daily_cost_usd").alias("est_cost_usd"),
        )
        .filter(F.col("total_tokens") > 0)
    )
    mart.coalesce(1).write.mode("overwrite").parquet(str(GOLD_DIR / "genai_tokens_by_org_date"))
    return mart.count()


def run_gold(spark) -> None:
    """Construye los marts de negocio (FinOps, Soporte, Producto/GenAI)."""
    n_usage = _gold_org_daily_usage(spark)
    n_revenue = _gold_revenue_by_org_month(spark)
    n_tickets = _gold_tickets_by_org_date(spark)
    n_genai = _gold_genai_tokens_by_org_date(spark)
    print(
        f"[gold] org_daily_usage_by_service={n_usage} revenue_by_org_month={n_revenue} "
        f"tickets_by_org_date={n_tickets} genai_tokens_by_org_date={n_genai}"
    )
