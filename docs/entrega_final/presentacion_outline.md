# Outline de presentacion - Entrega Final (Cloud Provider Analytics)

Estructura sugerida de slides + guion breve para el video. Diseno visual a cargo del equipo.

## 1. Portada
Titulo, integrantes, materia 72.80 Big Data, ITBA 1C 2026.

## 2. Contexto de negocio (storytelling)
Somos el equipo de datos de un cloud provider. Dos necesidades: metricas operativas near real-time
(uso/costo) y batch de maestros + facturacion. Los datos llegan sucios (nulos, anomalias, evolucion de
schema). Objetivo: pipeline confiable que habilite FinOps, Soporte y Producto.

## 3. Arquitectura (1 slide diagrama)
Lambda + Data Lake por zonas (Landing/Bronze/Silver/Gold) + serving Cassandra. Usar el diagrama de
`arquitectura.md`. Justificar Lambda vs Kappa en una linea.

## 4. Tecnologias y recursos
PySpark 3.5.1 (Structured Streaming + batch), Parquet, Cassandra 4.1 (Docker), Python driver.
Ejecucion local, Java 21. Costo USD 0.

## 5. Datos provistos y las 5Vs
7 fuentes batch + stream JSONL (v1/v2, formato EAV). Tabla de 5Vs (de `arquitectura.md`).

## 6. Ingesta (Bronze)
Batch: schema explicito, dedup, `ingest_ts`/`source_file`, particion por fecha.
Stream: `availableNow`, `withWatermark`, late/malformed -> quarantine. Decision: dedup en Silver
(evitar perdida por eviccion de watermark).

## 7. Calidad y conformacion (Silver)
Reglas por path + 4 zonas de quarantine. Pivot EAV (metric->columnas). Compatibilizacion v1/v2.
Enriquecimiento (orgs/resources). Anomalia de costo por p-tiles. Normalizacion de revenue a USD con
`exchange_rate_to_usd` por factura. Mostrar conteos: 43.200 eventos -> 40.956 validos / 2.244 quarantine.

## 8. Marts de negocio (Gold) y modelo Cassandra
5 marts query-first. Destacar colecciones: `counts_by_severity` (map) y `revenue_breakdown` (map).
`org_top_services_14d` preagregada para Top-N. Un unico keyspace `cloud_analytics`.

## 9. Demo: las 5 consultas CQL
Mostrar en vivo `docker exec -i cassandra-bigdata cqlsh < cassandra/queries_finales.cql`.
Q1 costos diarios | Q2 Top-N servicios 14d | Q3 tickets criticos + SLA breach | Q4 revenue USD | Q5 GenAI.
Resultados en `cassandra_query_results.md`.

## 10. Idempotencia y performance
Checkpoints + dedup + overwrite/upsert -> re-correr no duplica (tabla antes/despues).
Particionado por fecha, coalesce en marts chicos.

## 11. Profundidad: decisiones y trade-offs
Lambda vs Kappa, watermark vs perdida de datos, EAV pivot, p-tiles, colecciones con valor analitico,
FX por dato. Ver `decisiones_y_tradeoffs.md`.

## 12. Cierre
Que entrega valor a FinOps/Soporte/Producto, que quedaria para produccion (Kafka real, AstraDB,
BI sobre Cassandra), aprendizajes.

## Guion de demo (para el video)
1. `python scripts/run_mvp.py` (mostrar logs por capa).
2. Levantar Cassandra, `--with-cassandra`, mostrar conteos cargados.
3. Correr las 5 CQL y leer resultados.
4. Re-correr y mostrar que los conteos no cambian (idempotencia).
