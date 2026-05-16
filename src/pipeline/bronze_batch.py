from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import BATCH_SOURCES, BRONZE_BATCH_DIR
from .schemas import BATCH_SCHEMAS


def _enrich_technical_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
    )


def run_bronze_batch(spark) -> None:
    """Ingesta batch minima del segundo parcial (3 maestros)."""
    for dataset, conf in BATCH_SOURCES.items():
        schema = BATCH_SCHEMAS[dataset]
        key_cols = conf["key_cols"]

        df = (
            spark.read.option("header", True)
            .schema(schema)
            .csv(str(conf["path"]))
        )
        df = _enrich_technical_columns(df).dropDuplicates(key_cols)

        output_path = BRONZE_BATCH_DIR / dataset
        (
            df.write.mode("overwrite")
            .partitionBy("ingest_date")
            .parquet(str(output_path))
        )

        print(f"[bronze-batch] dataset={dataset} rows={df.count()} path={output_path}")
