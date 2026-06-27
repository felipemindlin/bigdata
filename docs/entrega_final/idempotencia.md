# Evidencia de idempotencia - Entrega Final

Se ejecuto `python scripts/run_mvp.py --with-cassandra` dos veces consecutivas **sin limpiar estado**
sobre el dataset real (`datalake/landing/`).

## Mecanismos de idempotencia

- **Bronze stream**: Structured Streaming con `checkpointLocation`. En la segunda corrida los archivos
  ya procesados no se vuelven a ingestar (`availableNow` + checkpoint).
- **Dedup por `event_id`**: aplicado en Silver (batch `dropDuplicates`), no pierde eventos fuera de orden.
- **Bronze/Silver/Gold Parquet**: escritura en modo `overwrite` por dataset.
- **Cassandra**: `INSERT`/`append` por clave primaria natural = upsert. Re-cargar las mismas filas no duplica.

## Conteos Cassandra (antes vs despues de la 2da corrida)

| Tabla | Antes | Despues |
|---|---:|---:|
| org_daily_usage_by_service | 11050 | 11050 |
| org_top_services_14d | 262 | 262 |
| revenue_by_org_month | 227 | 227 |
| tickets_by_org_date | 910 | 910 |
| genai_tokens_by_org_date | 848 | 848 |

## Conteos por dataset (Parquet)

| dataset | filas |
|---|---:|
| bronze_stream/usage_events | 43200 |
| silver/usage_enriched | 40956 |
| silver/daily_features | 11050 |
| silver/tickets | 960 |
| silver/billing_norm | 227 |
| gold/org_daily_usage_by_service | 11050 |
| gold/revenue_by_org_month | 227 |
| gold/tickets_by_org_date | 910 |
| gold/genai_tokens_by_org_date | 848 |

## Conclusion

Los conteos permanecen estables entre corridas: la re-ejecucion **no incrementa filas** ni en Parquet
ni en Cassandra. El pipeline es idempotente end-to-end.
