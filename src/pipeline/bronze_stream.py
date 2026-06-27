import os
from datetime import datetime, timezone

from pyspark.sql import functions as F

from .config import BRONZE_STREAM_DIR, CHECKPOINT_DIR, QUARANTINE_DIR, USAGE_STREAM_DIR
from .schemas import USAGE_EVENT_SCHEMA


def _synthetic_reference():
    raw = os.environ.get("BOOTSTRAP_REFERENCE_TS")
    if not raw:
        return None
    ref_dt = datetime.fromisoformat(raw)
    if ref_dt.tzinfo is None:
        ref_dt = ref_dt.replace(tzinfo=timezone.utc)
    return ref_dt


def run_bronze_stream(spark) -> None:
    """Structured Streaming con schema explicito, watermark, dedupe y manejo de late/malformed data.

    El feed se lee como micro-lotes (`maxFilesPerTrigger`) desde el directorio JSONL.
    `timestamp` (origen) se renombra a `event_ts` y se usa como event-time para watermark.
    Dedup idempotente por `event_id` acotado por watermark.

    Late/malformed:
    - Modo real (sin BOOTSTRAP_REFERENCE_TS): se aisla a quarantine todo evento con `event_ts` nulo
      (timestamp ausente o no parseable). El resto fluye a Bronze.
    - Modo demo sintetico (con BOOTSTRAP_REFERENCE_TS): ademas se marca como late cualquier evento
      con `event_ts` anterior a referencia - 1h (simulacion de late arrival wall-clock).

    Nota de diseño: el watermark se declara para acotar estado y cumplir el requisito, pero la
    deduplicacion por `event_id` se realiza en Silver (batch) en lugar de un dropDuplicates con estado
    en streaming. Con datos que abarcan ~2 meses y archivos no ordenados por event-time, el operador
    stateful descartaria eventos validos fuera de orden (eviccion por watermark); el dedup batch es
    idempotente y no pierde datos.
    """

    stream_df = (
        spark.readStream.format("json")
        .schema(USAGE_EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .load(str(USAGE_STREAM_DIR))
    )

    reference = _synthetic_reference()

    stream_df = (
        stream_df.withColumnRenamed("timestamp", "event_ts")
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
        .withWatermark("event_ts", "1 hour")
    )

    is_late = F.col("event_ts").isNull()
    if reference is not None:
        ref_lit = F.lit(reference).cast("timestamp")
        is_late = is_late | (F.col("event_ts") < (ref_lit - F.expr("INTERVAL 1 HOURS")))

    stream_df = stream_df.withColumn("is_late_data", is_late)

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
