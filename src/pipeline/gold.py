from pyspark.sql import functions as F

from .config import GOLD_DIR, SILVER_DIR


def run_gold(spark) -> None:
    """Gold minimo del segundo parcial: org_daily_usage_by_service."""

    daily_features = spark.read.parquet(str(SILVER_DIR / "daily_features"))

    mart = (
        daily_features.select(
            "org_id",
            "service",
            "usage_date",
            F.col("daily_cost_usd").cast("double"),
            F.col("requests").cast("double"),
            F.col("genai_tokens").cast("double"),
            F.col("carbon_kg").cast("double"),
            F.col("has_cost_anomaly").cast("boolean"),
        )
        .dropDuplicates(["org_id", "service", "usage_date"])
    )

    mart.write.mode("overwrite").partitionBy("usage_date").parquet(
        str(GOLD_DIR / "org_daily_usage_by_service")
    )

    print(f"[gold] org_daily_usage_by_service={mart.count()}")
