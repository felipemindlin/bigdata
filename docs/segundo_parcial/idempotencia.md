# Evidencia de idempotencia

Se ejecuto el pipeline completo dos veces consecutivas sobre el dataset sintetico
estabilizado (`BOOTSTRAP_REFERENCE_TS="2026-05-16T12:00:00+00:00"`).

## Procedimiento

```bash
# Run 1 (bronze -> silver -> gold -> cassandra)
BOOTSTRAP_REFERENCE_TS="2026-05-16T12:00:00+00:00" \
  python scripts/run_mvp.py --with-cassandra \
  --cassandra-host 127.0.0.1 --cassandra-port 9042 --keyspace cloud_analytics

# Run 2 (mismo comando, sin tocar landing ni checkpoints)
BOOTSTRAP_REFERENCE_TS="2026-05-16T12:00:00+00:00" \
  python scripts/run_mvp.py --with-cassandra \
  --cassandra-host 127.0.0.1 --cassandra-port 9042 --keyspace cloud_analytics
```

## Conteos por capa (Parquet)

Capturados con `scripts/generate_evidence_report.py` despues de cada corrida.

| Dataset | Run 1 | Run 2 | Delta |
|---|---:|---:|---:|
| bronze_batch/customers_orgs | 3 | 3 | 0 |
| bronze_batch/users | 4 | 4 | 0 |
| bronze_batch/billing_monthly | 3 | 3 | 0 |
| bronze_stream/usage_events | 5 | 5 | 0 |
| quarantine/late_data | 1 | 1 | 0 |
| silver/usage_enriched | 3 | 3 | 0 |
| silver/daily_features | 3 | 3 | 0 |
| quarantine/silver_quality | 2 | 2 | 0 |
| gold/org_daily_usage_by_service | 3 | 3 | 0 |

## Conteos en Cassandra (post run 1 y post run 2)

```sql
SELECT COUNT(*) FROM cloud_analytics.org_daily_usage_by_service;
SELECT COUNT(*) FROM cloud_analytics.org_top_services_14d;
```

| Tabla | Run 1 | Run 2 | Delta |
|---|---:|---:|---:|
| `org_daily_usage_by_service` | 3 | 3 | 0 |
| `org_top_services_14d` | 3 | 3 | 0 |

Salida real de `cqlsh` luego del run 2:

```
 count
-------
     3

(1 rows)
```

## Mecanismos que garantizan idempotencia

- **Bronze batch**: `dropDuplicates(key_cols)` + `write.mode("overwrite")` por dataset; las
  mismas filas se reescriben con el mismo `key_cols`, sin acumular versiones.
- **Bronze stream**: `withWatermark("event_ts","1 hour")` + `dropDuplicates(["event_id"])`
  + `checkpointLocation` activo; al reprocesar, los archivos ya consumidos no se reingestan.
- **Silver y Gold**: `write.mode("overwrite")` particionado por `usage_date`/`ingest_date`,
  y `dropDuplicates(["org_id","service","usage_date"])` antes de escribir Gold.
- **Cassandra**: la `PRIMARY KEY` de las dos tablas (`(org_id, service)` + `usage_date`
  para el mart diario; `(org_id, as_of_date) + cost_14d_usd + service` para Top-N) hace
  que un `INSERT` repetido con la misma clave actualice la fila en lugar de duplicarla.

## Conclusion

Los conteos son identicos entre run 1 y run 2 tanto en el data lake (Parquet) como en
las tablas Cassandra. La re-ejecucion del pipeline completo no introduce duplicados.
