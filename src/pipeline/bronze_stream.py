from pyspark.sql import functions as F

from .config import BRONZE_STREAM_DIR, CHECKPOINT_DIR, QUARANTINE_DIR, USAGE_STREAM_DIR
from .schemas import USAGE_EVENT_SCHEMA


def run_bronze_stream(spark) -> None:
    """Structured Streaming con schema explicito, watermark, dedupe y late data."""

    stream_df = (
        spark.readStream.format("json")
        .schema(USAGE_EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 1)
        .load(str(USAGE_STREAM_DIR))
    )

    stream_df = (
        stream_df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
        .withWatermark("event_ts", "1 hour")
        .dropDuplicates(["event_id"])
        .withColumn(
            "is_late_data",
            F.col("event_ts") < F.expr("current_timestamp() - INTERVAL 1 HOURS"),
        )
    )

    valid_stream = stream_df.filter(~F.col("is_late_data"))
    late_stream = stream_df.filter(F.col("is_late_data"))

    valid_query = (
        valid_stream.writeStream.format("parquet")
        .option("path", str(BRONZE_STREAM_DIR / "usage_events"))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "bronze_stream_usage"))
        .partitionBy("ingest_date")
        .trigger(availableNow=True)
        .start()
    )

    late_query = (
        late_stream.writeStream.format("parquet")
        .option("path", str(QUARANTINE_DIR / "late_data"))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "bronze_stream_late"))
        .partitionBy("ingest_date")
        .trigger(availableNow=True)
        .start()
    )

    valid_query.awaitTermination()
    late_query.awaitTermination()

    print("[bronze-stream] ingestion completed (availableNow)")
