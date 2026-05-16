import argparse
import sys
from pathlib import Path


BASE = Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from src.pipeline.bronze_batch import run_bronze_batch
from src.pipeline.bronze_stream import run_bronze_stream
from src.pipeline.cassandra_loader import run_cassandra_load
from src.pipeline.config import APP_NAME
from src.pipeline.gold import run_gold
from src.pipeline.silver import run_silver
from src.pipeline.spark_utils import build_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MVP tecnico - Segundo Parcial")
    parser.add_argument("--with-cassandra", action="store_true", help="Carga Gold a Cassandra")
    parser.add_argument("--cassandra-host", default="127.0.0.1")
    parser.add_argument("--cassandra-port", default="9042")
    parser.add_argument("--keyspace", default="cloud_analytics")
    args = parser.parse_args()

    spark = build_spark(APP_NAME)

    run_bronze_batch(spark)
    run_bronze_stream(spark)
    run_silver(spark)
    run_gold(spark)

    if args.with_cassandra:
        run_cassandra_load(
            spark,
            cassandra_host=args.cassandra_host,
            cassandra_port=args.cassandra_port,
            keyspace=args.keyspace,
        )

    spark.stop()


if __name__ == "__main__":
    main()
