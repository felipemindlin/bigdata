import os

from pyspark.sql import SparkSession

SPARK_CASSANDRA_CONNECTOR = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"


def build_spark(app_name: str) -> SparkSession:
    existing = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if SPARK_CASSANDRA_CONNECTOR not in existing:
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            f"--packages {SPARK_CASSANDRA_CONNECTOR} pyspark-shell"
        )
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", SPARK_CASSANDRA_CONNECTOR)
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .getOrCreate()
    )
