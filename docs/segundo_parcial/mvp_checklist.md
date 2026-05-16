# Segundo Parcial - Checklist de evidencia (MVP tecnico)

## Objetivo
Demostrar flujo end-to-end minimo:
Landing -> Bronze -> Silver -> Gold -> Serving (Cassandra)

## Evidencias que deben adjuntar

- [ ] Bronze batch generado para 3 maestros:
  - customers_orgs
  - users
  - billing_monthly
- [ ] Bronze streaming generado desde `usage_events_stream/*.jsonl`.
- [ ] Watermark + dedupe por `event_id` + checkpoint activo.
- [ ] Silver con 3 reglas de calidad aplicadas.
- [ ] Quarantine poblada con ejemplos invalidos.
- [ ] Gold con `org_daily_usage_by_service` generado.
- [ ] Cassandra: keyspace y tabla(s) creadas con `cassandra/schema.cql`.
- [ ] Dos consultas minimas ejecutadas (`cassandra/queries_minimas.cql`).
- [ ] Evidencia de idempotencia (re-ejecucion sin duplicados).

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
