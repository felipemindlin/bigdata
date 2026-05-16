import sys
from pathlib import Path

from pyspark.sql import functions as F


BASE = Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from src.pipeline.config import APP_NAME, BRONZE_BATCH_DIR, BRONZE_STREAM_DIR, GOLD_DIR, QUARANTINE_DIR, SILVER_DIR
from src.pipeline.spark_utils import build_spark

REPORT_PATH = BASE / "docs" / "segundo_parcial" / "evidencia_ejecucion.md"


def table_line(name: str, rows: int, path: Path) -> str:
    return f"| {name} | {rows} | `{path}` |"


def main() -> None:
    spark = build_spark(APP_NAME + "-evidence")

    rows_customers = spark.read.parquet(str(BRONZE_BATCH_DIR / "customers_orgs")).count()
    rows_users = spark.read.parquet(str(BRONZE_BATCH_DIR / "users")).count()
    rows_billing = spark.read.parquet(str(BRONZE_BATCH_DIR / "billing_monthly")).count()

    rows_stream = spark.read.parquet(str(BRONZE_STREAM_DIR / "usage_events")).count()
    rows_late = spark.read.parquet(str(QUARANTINE_DIR / "late_data")).count()

    rows_silver = spark.read.parquet(str(SILVER_DIR / "usage_enriched")).count()
    rows_features = spark.read.parquet(str(SILVER_DIR / "daily_features")).count()
    rows_quality = spark.read.parquet(str(QUARANTINE_DIR / "silver_quality")).count()

    rows_gold = spark.read.parquet(str(GOLD_DIR / "org_daily_usage_by_service")).count()

    quarantine_sample = (
        spark.read.parquet(str(QUARANTINE_DIR / "silver_quality"))
        .select("event_id", "quality_issue", "service", "org_id")
        .limit(10)
        .collect()
    )

    gold_sample = (
        spark.read.parquet(str(GOLD_DIR / "org_daily_usage_by_service"))
        .orderBy(F.col("daily_cost_usd").desc())
        .limit(10)
        .collect()
    )

    lines = [
        "# Evidencia de ejecucion - Segundo Parcial (MVP tecnico)",
        "",
        "## Conteos por capa",
        "",
        "| Dataset | Filas | Path |",
        "|---|---:|---|",
        table_line("bronze_batch/customers_orgs", rows_customers, BRONZE_BATCH_DIR / "customers_orgs"),
        table_line("bronze_batch/users", rows_users, BRONZE_BATCH_DIR / "users"),
        table_line("bronze_batch/billing_monthly", rows_billing, BRONZE_BATCH_DIR / "billing_monthly"),
        table_line("bronze_stream/usage_events", rows_stream, BRONZE_STREAM_DIR / "usage_events"),
        table_line("quarantine/late_data", rows_late, QUARANTINE_DIR / "late_data"),
        table_line("silver/usage_enriched", rows_silver, SILVER_DIR / "usage_enriched"),
        table_line("silver/daily_features", rows_features, SILVER_DIR / "daily_features"),
        table_line("quarantine/silver_quality", rows_quality, QUARANTINE_DIR / "silver_quality"),
        table_line("gold/org_daily_usage_by_service", rows_gold, GOLD_DIR / "org_daily_usage_by_service"),
        "",
        "## Reglas de calidad y quarantine (muestra)",
        "",
    ]

    if quarantine_sample:
        lines.append("| event_id | quality_issue | service | org_id |")
        lines.append("|---|---|---|---|")
        for r in quarantine_sample:
            lines.append(f"| {r['event_id']} | {r['quality_issue']} | {r['service']} | {r['org_id']} |")
    else:
        lines.append("Sin registros en quarantine de calidad.")

    lines.extend(["", "## Muestra Gold (FinOps)", ""])

    if gold_sample:
        lines.append("| org_id | service | usage_date | daily_cost_usd | requests | genai_tokens | carbon_kg | has_cost_anomaly |")
        lines.append("|---|---|---|---:|---:|---:|---:|---|")
        for r in gold_sample:
            lines.append(
                "| {org_id} | {service} | {usage_date} | {daily_cost_usd} | {requests} | {genai_tokens} | {carbon_kg} | {has_cost_anomaly} |".format(
                    org_id=r["org_id"],
                    service=r["service"],
                    usage_date=r["usage_date"],
                    daily_cost_usd=r["daily_cost_usd"],
                    requests=r["requests"],
                    genai_tokens=r["genai_tokens"],
                    carbon_kg=r["carbon_kg"],
                    has_cost_anomaly=r["has_cost_anomaly"],
                )
            )

    lines.extend(
        [
            "",
            "## Validacion de idempotencia",
            "",
            "Este reporte debe generarse luego de ejecutar dos veces `python scripts/run_mvp.py`.",
            "Si los conteos de Gold permanecen estables, la re-ejecucion no esta duplicando filas.",
            "",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"Evidence report generated: {REPORT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
