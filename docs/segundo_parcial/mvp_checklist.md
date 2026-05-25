# Segundo Parcial - Checklist de evidencia (MVP tecnico)

## Objetivo
Demostrar flujo end-to-end minimo:
Landing -> Bronze -> Silver -> Gold -> Serving (Cassandra)

## Evidencias que deben adjuntar

- [x] Bronze batch generado para 3 maestros:
  - customers_orgs
  - users
  - billing_monthly
- [x] Bronze streaming generado desde `usage_events_stream/*.jsonl`.
- [x] Watermark + dedupe por `event_id` + checkpoint activo.
- [x] Silver con 3 reglas de calidad aplicadas.
- [x] Quarantine poblada con ejemplos invalidos.
- [x] Gold con `org_daily_usage_by_service` generado.
- [x] Cassandra: keyspace y tabla(s) creadas con `cassandra/schema.cql`.
- [x] Dos consultas minimas ejecutadas (`cassandra/queries_minimas.cql`).
- [x] Evidencia de idempotencia (re-ejecucion sin duplicados).

Evidencia capturada en:
- `docs/segundo_parcial/evidencia_ejecucion.md` (conteos por capa + muestras)
- `docs/segundo_parcial/cassandra_query_results.md` (output real de las 2 queries)
- `docs/segundo_parcial/idempotencia.md` (comparativa run 1 vs run 2)

## Comandos recomendados

```bash
python -m pip install -r requirements.txt
python scripts/run_mvp.py
```

Con carga a Cassandra:

```bash
python scripts/run_mvp.py --with-cassandra --cassandra-host 127.0.0.1 --cassandra-port 9042 --keyspace cloud_analytics
```

## Evidencia de idempotencia sugerida

1. Ejecutar `python scripts/run_mvp.py` dos veces.
2. Comparar conteos en Gold antes y despues:

```python
spark.read.parquet("datalake/gold/org_daily_usage_by_service").count()
```

3. Verificar que no crecen filas por duplicado al reprocesar mismos archivos.
