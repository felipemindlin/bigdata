# Cassandra CQL Execution Results

## Preparacion del entorno

Cassandra se levanto como contenedor local (equivalente al protocolo de AstraDB para el MVP):

```bash
docker run --name cassandra-local -d -p 9042:9042 cassandra:4.1
# wait until cqlsh responde
docker cp cassandra/schema.cql cassandra-local:/schema.cql
docker exec cassandra-local cqlsh -f /schema.cql
```

Carga del mart Gold a Cassandra:

```bash
BOOTSTRAP_REFERENCE_TS="2026-05-16T12:00:00+00:00" \
  python scripts/run_mvp.py --with-cassandra \
    --cassandra-host 127.0.0.1 --cassandra-port 9042 --keyspace cloud_analytics
# [cassandra] loaded rows=3 keyspace=cloud_analytics
```

## Estado de las tablas tras la carga

`cloud_analytics.org_daily_usage_by_service`:

```
 org_id  | service  | usage_date | cost_usd | requests | genai_tokens | carbon_kg
---------+----------+------------+----------+----------+--------------+-----------
 org_001 |  compute | 2026-05-16 |      8.4 |      120 |            0 |       1.6
 org_001 |    genai | 2026-05-16 |      4.1 |       25 |         5000 |       0.3
 org_003 | database | 2026-05-16 |      1.8 |      200 |            0 |         0

(3 rows)
```

`cloud_analytics.org_top_services_14d`:

```
 org_id  | as_of_date | service  | cost_14d_usd
---------+------------+----------+--------------
 org_003 | 2026-05-16 | database |          1.8
 org_001 | 2026-05-16 |  compute |          8.4
 org_001 | 2026-05-16 |    genai |          4.1

(3 rows)
```

## Resultados de las 2 consultas minimas

Comando:

```bash
docker exec cassandra-local cqlsh -f /queries_minimas.cql
```

### Query 1: costos y requests diarios de `org_001` / `compute` (mayo 2026)

CQL (de `cassandra/queries_minimas.cql`):

```sql
SELECT usage_date, cost_usd, requests, genai_tokens, carbon_kg
FROM org_daily_usage_by_service
WHERE org_id = 'org_001'
  AND service = 'compute'
  AND usage_date >= '2026-05-01'
  AND usage_date <= '2026-05-31';
```

Resultado:

```
 usage_date | cost_usd | requests | genai_tokens | carbon_kg
------------+----------+----------+--------------+-----------
 2026-05-16 |      8.4 |      120 |            0 |       1.6

(1 rows)
```

### Query 2: Top-N servicios por costo acumulado en ultimos 14 dias

CQL:

```sql
SELECT service, cost_14d_usd
FROM org_top_services_14d
WHERE org_id = 'org_001'
  AND as_of_date = '2026-05-16'
LIMIT 5;
```

Resultado:

```
 service | cost_14d_usd
---------+--------------
 compute |          8.4
   genai |          4.1

(2 rows)
```

## Interpretacion

- Las tablas se cargaron sin errores con el `spark-cassandra-connector` (3 filas en cada una para el dataset sintetico).
- Ambas consultas minimas devuelven filas no vacias: el modelado query-first responde por la `PRIMARY KEY` definida en `cassandra/schema.cql`.
- `as_of_date` se fija deterministicamente a `max(usage_date)` desde el loader (`src/pipeline/cassandra_loader.py`) para que la query #2 sea reproducible entre corridas.
- Para AstraDB real, basta con sustituir `cassandra.connection.host/port` por `spark.cassandra.connection.config.cloud.path` apuntando al secure-connect-bundle y agregar credenciales.
