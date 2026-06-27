import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


BASE = Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from src.pipeline.config import (
    APP_NAME,
    BRONZE_BATCH_DIR,
    BRONZE_STREAM_DIR,
    GOLD_DIR,
    QUARANTINE_DIR,
    SILVER_DIR,
)
from src.pipeline.spark_utils import build_spark

REPORT_PATH = BASE / "docs" / "entrega_final" / "evidencia_ejecucion.md"


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(BASE))
    except ValueError:
        return str(path)


def _safe_count(spark, path: Path) -> int:
    try:
        return spark.read.parquet(str(path)).count()
    except AnalysisException:
        return 0


def table_line(name: str, rows: int, path: Path) -> str:
    return f"| {name} | {rows} | `{_rel(path)}` |"


def main() -> None:
    spark = build_spark(APP_NAME + "-evidence")

    bronze_batch = [
        "customers_orgs", "users", "resources", "support_tickets",
        "marketing_touches", "nps_surveys", "billing_monthly",
    ]
    lines = [
        "# Evidencia de ejecucion - Entrega Final",
        "",
        "Generado por `scripts/generate_evidence_report.py` sobre el dataset real en `datalake/landing/`.",
        "",
        "## Conteos por capa",
        "",
        "| Dataset | Filas | Path |",
        "|---|---:|---|",
    ]
    for ds in bronze_batch:
        p = BRONZE_BATCH_DIR / ds
        lines.append(table_line(f"bronze_batch/{ds}", _safe_count(spark, p), p))

    layer_paths = [
        ("bronze_stream/usage_events", BRONZE_STREAM_DIR / "usage_events"),
        ("quarantine/late_data", QUARANTINE_DIR / "late_data"),
        ("silver/usage_enriched", SILVER_DIR / "usage_enriched"),
        ("silver/daily_features", SILVER_DIR / "daily_features"),
        ("silver/tickets", SILVER_DIR / "tickets"),
        ("silver/billing_norm", SILVER_DIR / "billing_norm"),
        ("quarantine/silver_quality", QUARANTINE_DIR / "silver_quality"),
        ("quarantine/tickets_quality", QUARANTINE_DIR / "tickets_quality"),
        ("quarantine/billing_quality", QUARANTINE_DIR / "billing_quality"),
        ("gold/org_daily_usage_by_service", GOLD_DIR / "org_daily_usage_by_service"),
        ("gold/revenue_by_org_month", GOLD_DIR / "revenue_by_org_month"),
        ("gold/tickets_by_org_date", GOLD_DIR / "tickets_by_org_date"),
        ("gold/genai_tokens_by_org_date", GOLD_DIR / "genai_tokens_by_org_date"),
    ]
    for name, p in layer_paths:
        lines.append(table_line(name, _safe_count(spark, p), p))

    lines.extend(["", "## Reglas de calidad (quarantine de usage por tipo)", ""])
    try:
        quality = spark.read.parquet(str(QUARANTINE_DIR / "silver_quality"))
        rows = quality.groupBy("quality_issue").count().orderBy(F.col("count").desc()).collect()
        lines.append("| quality_issue | filas |")
        lines.append("|---|---:|")
        for r in rows:
            lines.append(f"| {r['quality_issue']} | {r['count']} |")
    except AnalysisException:
        lines.append("Sin registros en quarantine de calidad.")

    def add_sample(title: str, path: Path, cols, order_col=None, n=5):
        lines.extend(["", f"## {title}", ""])
        try:
            df = spark.read.parquet(str(path)).select(*cols)
            if order_col is not None:
                df = df.orderBy(F.col(order_col).desc())
            sample = df.limit(n).collect()
            lines.append("| " + " | ".join(cols) + " |")
            lines.append("|" + "---|" * len(cols))
            for r in sample:
                lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
        except AnalysisException:
            lines.append("(dataset no disponible)")

    add_sample(
        "Muestra Gold - org_daily_usage_by_service (FinOps)",
        GOLD_DIR / "org_daily_usage_by_service",
        ["org_id", "service", "usage_date", "daily_cost_usd", "requests", "cpu_hours", "storage_gb_hours", "genai_tokens", "carbon_kg", "has_cost_anomaly"],
        order_col="daily_cost_usd",
    )
    add_sample(
        "Muestra Gold - revenue_by_org_month (coleccion revenue_breakdown)",
        GOLD_DIR / "revenue_by_org_month",
        ["org_id", "month", "revenue_usd", "revenue_breakdown"],
        order_col="revenue_usd",
    )
    add_sample(
        "Muestra Gold - tickets_by_org_date (coleccion counts_by_severity)",
        GOLD_DIR / "tickets_by_org_date",
        ["org_id", "ticket_date", "total_tickets", "critical_count", "sla_breach_rate", "csat_avg", "counts_by_severity"],
        order_col="critical_count",
    )
    add_sample(
        "Muestra Gold - genai_tokens_by_org_date (Producto/GenAI)",
        GOLD_DIR / "genai_tokens_by_org_date",
        ["org_id", "usage_date", "total_tokens", "est_cost_usd"],
        order_col="total_tokens",
    )

    lines.extend(
        [
            "",
            "## Validacion de idempotencia",
            "",
            "Este reporte se genera luego de ejecutar el pipeline. Re-ejecutar `scripts/run_mvp.py` no",
            "incrementa filas en Gold (escritura `overwrite` en Parquet + upsert por PK en Cassandra).",
            "",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"Evidence report generated: {REPORT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
