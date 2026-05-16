# Evidencia de ejecucion - Segundo Parcial (MVP tecnico)

## Conteos por capa

| Dataset | Filas | Path |
|---|---:|---|
| bronze_batch/customers_orgs | 3 | `/home/pipemind/big/datalake/bronze/batch/customers_orgs` |
| bronze_batch/users | 4 | `/home/pipemind/big/datalake/bronze/batch/users` |
| bronze_batch/billing_monthly | 3 | `/home/pipemind/big/datalake/bronze/batch/billing_monthly` |
| bronze_stream/usage_events | 5 | `/home/pipemind/big/datalake/bronze/stream/usage_events` |
| quarantine/late_data | 1 | `/home/pipemind/big/datalake/quarantine/late_data` |
| silver/usage_enriched | 3 | `/home/pipemind/big/datalake/silver/usage_enriched` |
| silver/daily_features | 3 | `/home/pipemind/big/datalake/silver/daily_features` |
| quarantine/silver_quality | 2 | `/home/pipemind/big/datalake/quarantine/silver_quality` |
| gold/org_daily_usage_by_service | 3 | `/home/pipemind/big/datalake/gold/org_daily_usage_by_service` |

## Reglas de calidad y quarantine (muestra)

| event_id | quality_issue | service | org_id |
|---|---|---|---|
| e_003 | cost_lt_-0.01 | storage | org_002 |
| e_006 | unit_null_with_value | compute | org_003 |

## Muestra Gold (FinOps)

| org_id | service | usage_date | daily_cost_usd | requests | genai_tokens | carbon_kg | has_cost_anomaly |
|---|---|---|---:|---:|---:|---:|---|
| org_001 | compute | 2026-05-16 | 8.4 | 120.0 | 0.0 | 1.6 | False |
| org_001 | genai | 2026-05-16 | 4.1 | 25.0 | 5000.0 | 0.3 | False |
| org_003 | database | 2026-05-16 | 1.8 | 200.0 | 0.0 | 0.0 | False |

## Validacion de idempotencia

Este reporte debe generarse luego de ejecutar dos veces `python scripts/run_mvp.py`.
Si los conteos de Gold permanecen estables, la re-ejecucion no esta duplicando filas.
