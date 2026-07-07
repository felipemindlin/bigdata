# Arquitectura - Entrega Final

## Patrón: Lambda (batch + streaming) sobre un Data Lake por zonas + serving en Cassandra

```text
FUENTES (Landing, fila inmutable)
  batch:  customers_orgs, users, resources, support_tickets, marketing_touches, nps_surveys, billing_monthly
  stream: usage_events_stream/*.jsonl  (schema v1/v2, formato EAV metric+value+unit)
      |
      v
+--------------------------------------------------------------------------+
|  DATA LAKE (Parquet por zonas, gestionado por Spark)                     |
|                                                                          |
|  LANDING  -->  BRONZE  -->  SILVER  -->  GOLD                            |
|  raw           tipado +     calidad +     5 marts de negocio             |
|                ingest_ts    quarantine    (FinOps / Soporte / Producto)  |
|                watermark     + pivot EAV                                 |
|                (stream)      + joins                                     |
+--------------------------------------------------------------------------+
      |
      v
+----------------------------+      +---------------------------+
| SERVING: Cassandra         |      | CONSUMO                   |
| keyspace cloud_analytics   |----->| 5 consultas CQL           |
| 5 tablas query-first       |      | (dashboards / demo)       |
| + colecciones (map<>)      |      |                           |
+----------------------------+      +---------------------------+
```

## Flujo por path

**Batch**: Landing CSV -> `bronze_batch` (schema explícito, deduplicado por clave, `ingest_*`, partición por
`ingest_date`) -> `silver` (tickets, billing normalizado a USD, quality + quarantine) -> `gold`
(revenue, tickets marts) -> Cassandra.

**Streaming**: `usage_events_stream` -> `bronze_stream` (Structured Streaming `availableNow`,
`withWatermark`, late/malformed -> quarantine, partición `ingest_date`) -> `silver` (dedup por
`event_id`, cast con fallback, pivot EAV, anomalia p99, enriquecimiento orgs/resources) -> `gold`
(org_daily, genai marts) -> Cassandra.

## Mapeos

- **Volumen**: 43.200 eventos + 7 maestros; Parquet particionado por fecha.
- **Velocidad**: Structured Streaming en micro-lotes (`maxFilesPerTrigger`), watermark 1h.
- **Variedad**: 7 CSV (batch) + JSONL multi-schema (v1/v2) en formato EAV.
- **Veracidad**: reglas de calidad por path, flags por registro, 4 zonas de quarantine, dedup idempotente.
- **Valor**: 5 marts (FinOps: uso/revenue; Soporte: tickets/SLA; Producto: GenAI) servidos query-first.

## Componentes (código)

| Capa | Modulo |
|---|---|
| Bronze batch | `src/pipeline/bronze_batch.py` |
| Bronze stream | `src/pipeline/bronze_stream.py` |
| Silver | `src/pipeline/silver.py` |
| Gold | `src/pipeline/gold.py` |
| Serving | `src/pipeline/cassandra_loader.py` |
| Schemas / config | `src/pipeline/schemas.py`, `src/pipeline/config.py` |
| Orquestacion | `scripts/run_mvp.py` |
| Evidencia | `scripts/generate_evidence_report.py` |
| CQL | `cassandra/schema.cql`, `cassandra/queries_finales.cql` |
