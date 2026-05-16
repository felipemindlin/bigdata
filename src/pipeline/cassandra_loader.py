from pyspark.sql import functions as F

from .config import GOLD_DIR


def run_cassandra_load(spark, cassandra_host: str, cassandra_port: str, keyspace: str) -> None:
    """Carga el mart minimo de Gold a Cassandra (query-first)."""

    spark.conf.set("spark.cassandra.connection.host", cassandra_host)
    spark.conf.set("spark.cassandra.connection.port", cassandra_port)

    mart = spark.read.parquet(str(GOLD_DIR / "org_daily_usage_by_service"))

    cass_df = mart.select(
        "org_id",
        "service",
        "usage_date",
        F.col("daily_cost_usd").alias("cost_usd"),
        "requests",
        "genai_tokens",
        "carbon_kg",
        "has_cost_anomaly",
    )

    (
        cass_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="org_daily_usage_by_service", keyspace=keyspace)
        .save()
    )

    # Tabla auxiliar para consulta Top-N por costo en ultimos 14 dias
    topn_df = (
        cass_df.groupBy("org_id", "service")
        .agg(F.sum("cost_usd").alias("cost_14d_usd"))
        .withColumn("as_of_date", F.current_date())
    )

    (
        topn_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="org_top_services_14d", keyspace=keyspace)
        .save()
    )

    print(f"[cassandra] loaded rows={cass_df.count()} keyspace={keyspace}")
