from cassandra.cluster import Cluster
from pyspark.sql import functions as F

from .config import GOLD_DIR


def _ensure_schema(host: str, port: str, keyspace: str) -> None:
    cluster = Cluster([host], port=int(port))
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace)
    session.execute(
        f"""CREATE TABLE IF NOT EXISTS {keyspace}.org_daily_usage_by_service (
          org_id text, service text, usage_date date,
          cost_usd double, requests double, genai_tokens double,
          carbon_kg double, has_cost_anomaly boolean,
          PRIMARY KEY ((org_id, service), usage_date)
        ) WITH CLUSTERING ORDER BY (usage_date DESC)"""
    )
    session.execute(
        f"""CREATE TABLE IF NOT EXISTS {keyspace}.org_top_services_14d (
          org_id text, as_of_date date, service text, cost_14d_usd double,
          PRIMARY KEY ((org_id, as_of_date), cost_14d_usd, service)
        ) WITH CLUSTERING ORDER BY (cost_14d_usd DESC, service ASC)"""
    )
    session.shutdown()
    cluster.shutdown()


def run_cassandra_load(spark, cassandra_host: str, cassandra_port: str, keyspace: str) -> None:
    """Carga el mart minimo de Gold a Cassandra (query-first)."""

    _ensure_schema(cassandra_host, cassandra_port, keyspace)

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

    # Tabla auxiliar para consulta Top-N por costo en ultimos 14 dias.
    # as_of_date se fija a la maxima usage_date observada para que el demo sea idempotente.
    as_of_row = cass_df.agg(F.max("usage_date").alias("as_of_date")).collect()[0]
    as_of_date = as_of_row["as_of_date"]
    topn_df = (
        cass_df.filter(F.col("usage_date") > F.date_sub(F.lit(as_of_date), 14))
        .groupBy("org_id", "service")
        .agg(F.sum("cost_usd").alias("cost_14d_usd"))
        .withColumn("as_of_date", F.lit(as_of_date))
    )

    (
        topn_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="org_top_services_14d", keyspace=keyspace)
        .save()
    )

    print(f"[cassandra] loaded rows={cass_df.count()} keyspace={keyspace}")
