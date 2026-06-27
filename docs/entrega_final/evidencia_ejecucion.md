# Evidencia de ejecucion - Entrega Final

Generado por `scripts/generate_evidence_report.py` sobre el dataset real en `datalake/landing/`.

## Conteos por capa

| Dataset | Filas | Path |
|---|---:|---|
| bronze_batch/customers_orgs | 80 | `datalake/bronze/batch/customers_orgs` |
| bronze_batch/users | 800 | `datalake/bronze/batch/users` |
| bronze_batch/resources | 400 | `datalake/bronze/batch/resources` |
| bronze_batch/support_tickets | 1000 | `datalake/bronze/batch/support_tickets` |
| bronze_batch/marketing_touches | 1500 | `datalake/bronze/batch/marketing_touches` |
| bronze_batch/nps_surveys | 92 | `datalake/bronze/batch/nps_surveys` |
| bronze_batch/billing_monthly | 240 | `datalake/bronze/batch/billing_monthly` |
| bronze_stream/usage_events | 43200 | `datalake/bronze/stream/usage_events` |
| quarantine/late_data | 0 | `datalake/quarantine/late_data` |
| silver/usage_enriched | 40956 | `datalake/silver/usage_enriched` |
| silver/daily_features | 11050 | `datalake/silver/daily_features` |
| silver/tickets | 960 | `datalake/silver/tickets` |
| silver/billing_norm | 227 | `datalake/silver/billing_norm` |
| quarantine/silver_quality | 2244 | `datalake/quarantine/silver_quality` |
| quarantine/tickets_quality | 40 | `datalake/quarantine/tickets_quality` |
| quarantine/billing_quality | 13 | `datalake/quarantine/billing_quality` |
| gold/org_daily_usage_by_service | 11050 | `datalake/gold/org_daily_usage_by_service` |
| gold/revenue_by_org_month | 227 | `datalake/gold/revenue_by_org_month` |
| gold/tickets_by_org_date | 910 | `datalake/gold/tickets_by_org_date` |
| gold/genai_tokens_by_org_date | 848 | `datalake/gold/genai_tokens_by_org_date` |

## Reglas de calidad (quarantine de usage por tipo)

| quality_issue | filas |
|---|---:|
| unit_null_with_value | 2033 |
| cost_lt_-0.01 | 206 |
| cost_lt_-0.01;unit_null_with_value | 5 |

## Muestra Gold - org_daily_usage_by_service (FinOps)

| org_id | service | usage_date | daily_cost_usd | requests | cpu_hours | storage_gb_hours | genai_tokens | carbon_kg | has_cost_anomaly |
|---|---|---|---|---|---|---|---|---|---|
| org_rixa11dp | genai | 2025-08-24 | 340.5259 | 390.0 | 0.0473 | 8.3086 | 5646.0 | 0.07967099999999999 | False |
| org_c11ertj5 | genai | 2025-07-11 | 334.6767 | 237.0 | 0.0 | 12.9349 | 0.0 | 0.0 | False |
| org_tvhhpbmy | genai | 2025-07-04 | 313.4168 | 210.0 | 1.3851 | 0.0 | 0.0 | 0.0 | False |
| org_x7eedtjv | compute | 2025-07-09 | 311.255 | 249.0 | 1.8107 | 0.0 | 0.0 | 0.0 | False |
| org_5935a0l7 | compute | 2025-07-09 | 271.64040000000006 | 267.0 | 1.1745 | 7.2181 | 0.0 | 0.0 | False |

## Muestra Gold - revenue_by_org_month (coleccion revenue_breakdown)

| org_id | month | revenue_usd | revenue_breakdown |
|---|---|---|---|
| org_d14ve92m | 2025-07-01 | 3155.096592 | {'taxes': 551.989177, 'net': 3155.096592, 'credits': 25.4231048, 'subtotal': 2628.5305197999996} |
| org_5lxo6qji | 2025-06-01 | 2678.2667911999997 | {'taxes': 464.81921199999994, 'net': 2678.2667911999997, 'credits': 0.0, 'subtotal': 2213.4475792} |
| org_pbhsahxt | 2025-08-01 | 2533.817306 | {'taxes': 444.40178960000003, 'net': 2533.817306, 'credits': 26.7810226, 'subtotal': 2116.196539} |
| org_5lxo6qji | 2025-08-01 | 2334.6978644 | {'taxes': 416.3223006, 'net': 2334.6978644, 'credits': 64.0932298, 'subtotal': 1982.4687936} |
| org_c11ertj5 | 2025-06-01 | 2289.1325309999997 | {'taxes': 399.18497850000006, 'net': 2289.1325309999997, 'credits': 10.960519500000002, 'subtotal': 1900.908072} |

## Muestra Gold - tickets_by_org_date (coleccion counts_by_severity)

| org_id | ticket_date | total_tickets | critical_count | sla_breach_rate | csat_avg | counts_by_severity |
|---|---|---|---|---|---|---|
| org_i3qk2iag | 2025-07-02 | 1 | 1 | 0.0 | 4.0 | {'critical': 1} |
| org_ly8ozcyw | 2025-06-11 | 1 | 1 | 0.0 | 4.0 | {'critical': 1} |
| org_fel6246h | 2025-08-11 | 2 | 1 | 0.0 | 2.5 | {'critical': 1, 'low': 1} |
| org_h2m5zja8 | 2025-07-28 | 1 | 1 | 0.0 | None | {'critical': 1} |
| org_7e8g8jdp | 2025-07-31 | 1 | 1 | 0.0 | 3.0 | {'critical': 1} |

## Muestra Gold - genai_tokens_by_org_date (Producto/GenAI)

| org_id | usage_date | total_tokens | est_cost_usd |
|---|---|---|---|
| org_h2m5zja8 | 2025-08-17 | 10622.0 | 77.0881 |
| org_pbhsahxt | 2025-08-25 | 8631.0 | 92.0452 |
| org_pbhsahxt | 2025-07-18 | 8464.0 | 96.16969999999999 |
| org_h2m5zja8 | 2025-08-23 | 8458.0 | 108.6738 |
| org_h2m5zja8 | 2025-08-22 | 8249.0 | 63.870200000000004 |

## Validacion de idempotencia

Este reporte se genera luego de ejecutar el pipeline. Re-ejecutar `scripts/run_mvp.py` no
incrementa filas en Gold (escritura `overwrite` en Parquet + upsert por PK en Cassandra).
