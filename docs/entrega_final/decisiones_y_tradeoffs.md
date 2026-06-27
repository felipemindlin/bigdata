# Decisiones y trade-offs - Entrega Final

## Patron arquitectonico: Lambda

Se eligio **Lambda** (batch + streaming) sobre Kappa.

- **Batch**: maestros (orgs, users, resources), facturacion (billing), encuestas (NPS) y marketing.
  Son datos periodicos que no requieren latencia baja; Parquet particionado + reproceso controlado.
- **Streaming**: `usage_events_stream/*.jsonl` via Spark Structured Streaming (micro-lotes).
- **Trade-off**: Kappa unificaria todo como stream, pero forzar maestros/billing a stream agrega
  complejidad sin beneficio (no necesitan near real-time). Lambda mantiene cada path en su modo natural.

## Streaming: watermark vs. perdida de datos

- Se declara `withWatermark("event_ts", "1 hour")` para acotar estado (requisito).
- **Decision clave**: la deduplicacion por `event_id` se hace en **Silver (batch)**, no con un
  `dropDuplicates` con estado en streaming. Con datos que abarcan ~2 meses y archivos no ordenados por
  event-time, el operador stateful descarta eventos validos por eviccion de watermark (se observo una
  caida de 43.200 -> ~6.800 filas). El dedup batch es idempotente y no pierde datos.
- **Late/malformed**: se aisla a quarantine todo evento con `event_ts` nulo. Con replay de archivos el
  concepto de "late arrival" wall-clock no aplica; se mantiene el modo sintetico (`BOOTSTRAP_REFERENCE_TS`)
  para demostrar deteccion de late por referencia temporal.

## Eventos en formato EAV (long) -> pivot

Los eventos vienen en formato **metric + value + unit** (long/EAV), no en columnas anchas.
`requests`, `cpu_hours` y `storage_gb_hours` se derivan por **pivot condicional** sobre `metric`.
`carbon_kg` y `genai_tokens` son campos top-level que solo existen en `schema_version=2`.

## Evolucion de schema v1/v2

Schema explicito unico (`USAGE_EVENT_SCHEMA`) que incluye los campos v2 (`carbon_kg`, `genai_tokens`).
Para eventos v1 esos campos llegan nulos y se `coalesce` a 0. No se rompe el pipeline ante drift.

## Normalizacion de revenue a USD

`billing_monthly` trae `exchange_rate_to_usd` **por factura** (monedas ARS y USD).
`revenue_usd = (subtotal - credits + taxes) * exchange_rate_to_usd`. Se usa la tasa provista por el
dato en lugar de una tabla de FX estatica (mas fiel y sin asunciones externas).

## Anomalias de costo: p-tiles

Metodo **percentil** (`cost_usd_increment > p99 * 2`). Robusto en distribuciones con cola pesada y
simple de justificar. Alternativas (z-score, MAD) requieren mas tuning; p-tiles cumple el requisito
con minimo riesgo. El flag se materializa como `has_cost_anomaly` en el mart de uso diario.

## Modelo Cassandra: query-first + colecciones

- Una tabla por consulta de negocio (las 5 obligatorias) -> sin full-scans, particion alineada a la query.
- **Colecciones** (criterio de evaluacion): `map<text,int> counts_by_severity` en `tickets_by_org_date`
  y `map<text,double> revenue_breakdown` en `revenue_by_org_month`. Uso con valor analitico real
  (desglose por categoria en una sola fila), no decorativo.
- `org_top_services_14d` es una tabla **preagregada** para el Top-N (clustering por costo desc), porque
  Cassandra no ordena ni agrega por columnas no-clave.
- Todo vive en un unico keyspace `cloud_analytics`.

## Performance

- Particionado por fecha (`usage_date`, `ingest_date`) en Bronze/Silver/Gold de uso.
- `coalesce(1)` en marts chicos (revenue/tickets/genai) para evitar small-files.
- `maxFilesPerTrigger=10` en el stream para simular micro-lotes sin overhead excesivo de 120 triggers.

## Entorno

Ejecucion **local** (Spark local + Cassandra en Docker), Java 21. Se descarto Colab/AstraDB para esta
entrega: las capturas de evidencia se generan con scripts reproducibles y el demo corre 100% local.
