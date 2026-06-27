from cassandra.cluster import Cluster
from pyspark.sql import functions as F

from .config import GOLD_DIR

CREATE_TABLES = [
    """CREATE TABLE IF NOT EXISTS {ks}.org_daily_usage_by_service (
      org_id text, service text, usage_date date,
      cost_usd double, requests double, cpu_hours double, storage_gb_hours double,
      genai_tokens double, carbon_kg double, has_cost_anomaly boolean,
      PRIMARY KEY ((org_id, service), usage_date)
    ) WITH CLUSTERING ORDER BY (usage_date DESC)""",
    """CREATE TABLE IF NOT EXISTS {ks}.org_top_services_14d (
      org_id text, as_of_date date, service text, cost_14d_usd double,
      PRIMARY KEY ((org_id, as_of_date), cost_14d_usd, service)
    ) WITH CLUSTERING ORDER BY (cost_14d_usd DESC, service ASC)""",
    """CREATE TABLE IF NOT EXISTS {ks}.revenue_by_org_month (
      org_id text, month date,
      revenue_usd double, subtotal_usd double, credits_usd double, taxes_usd double,
      revenue_breakdown map<text, double>,
      PRIMARY KEY ((org_id), month)
    ) WITH CLUSTERING ORDER BY (month DESC)""",
    """CREATE TABLE IF NOT EXISTS {ks}.tickets_by_org_date (
      org_id text, ticket_date date,
      total_tickets int, critical_count int, sla_breach_rate double, csat_avg double,
      counts_by_severity map<text, int>,
      PRIMARY KEY ((org_id), ticket_date)
    ) WITH CLUSTERING ORDER BY (ticket_date DESC)""",
    """CREATE TABLE IF NOT EXISTS {ks}.genai_tokens_by_org_date (
      org_id text, usage_date date,
      total_tokens double, est_cost_usd double,
      PRIMARY KEY ((org_id), usage_date)
    ) WITH CLUSTERING ORDER BY (usage_date DESC)""",
]


def _ensure_schema(host: str, port: str, keyspace: str) -> None:
    cluster = Cluster([host], port=int(port))
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(keyspace)
    for ddl in CREATE_TABLES:
        session.execute(ddl.format(ks=keyspace))
    session.shutdown()
    cluster.shutdown()


def _write(df, table: str, keyspace: str) -> None:
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=table, keyspace=keyspace)
        .save()
    )


def run_cassandra_load(spark, cassandra_host: str, cassandra_port: str, keyspace: str) -> None:
    """Carga los marts de Gold a Cassandra (query-first, con colecciones)."""
    _ensure_schema(cassandra_host, cassandra_port, keyspace)

    spark.conf.set("spark.cassandra.connection.host", cassandra_host)
    spark.conf.set("spark.cassandra.connection.port", cassandra_port)

    # 1) org_daily_usage_by_service
    usage = spark.read.parquet(str(GOLD_DIR / "org_daily_usage_by_service"))
    usage_cass = usage.select(
        "org_id",
        "service",
        "usage_date",
        F.col("daily_cost_usd").alias("cost_usd"),
        "requests",
        "cpu_hours",
        "storage_gb_hours",
        "genai_tokens",
        "carbon_kg",
        "has_cost_anomaly",
    )
    _write(usage_cass, "org_daily_usage_by_service", keyspace)

    # 2) org_top_services_14d (helper para Top-N por costo en ultimos 14 dias)
    as_of_date = usage_cass.agg(F.max("usage_date").alias("as_of_date")).collect()[0]["as_of_date"]
    topn = (
        usage_cass.filter(F.col("usage_date") > F.date_sub(F.lit(as_of_date), 14))
        .groupBy("org_id", "service")
        .agg(F.sum("cost_usd").alias("cost_14d_usd"))
        .withColumn("as_of_date", F.lit(as_of_date))
    )
    _write(topn, "org_top_services_14d", keyspace)

    # 3) revenue_by_org_month (map<text,double> revenue_breakdown)
    revenue = spark.read.parquet(str(GOLD_DIR / "revenue_by_org_month"))
    _write(revenue, "revenue_by_org_month", keyspace)

    # 4) tickets_by_org_date (map<text,int> counts_by_severity)
    tickets = spark.read.parquet(str(GOLD_DIR / "tickets_by_org_date"))
    _write(tickets, "tickets_by_org_date", keyspace)

    # 5) genai_tokens_by_org_date
    genai = spark.read.parquet(str(GOLD_DIR / "genai_tokens_by_org_date"))
    _write(genai, "genai_tokens_by_org_date", keyspace)

    print(
        f"[cassandra] keyspace={keyspace} loaded "
        f"usage={usage_cass.count()} top14d={topn.count()} revenue={revenue.count()} "
        f"tickets={tickets.count()} genai={genai.count()}"
    )
