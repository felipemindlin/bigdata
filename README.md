# bigdata - Cloud Provider Analytics

Este proyecto fue realizado por Jeremias Feferovich, Felipe Mindlin, Martin Zahnd, Florencia Carrica y Gianfranco Magliotti, en el contexto de la materia 72.80 - Big Data.

## Contexto

El proyecto simula el rol del equipo de datos de un proveedor cloud que debe cubrir:

- metricas operativas near real-time de uso/costo
- procesamiento batch diario/mensual para maestros y facturacion

El pipeline esta pensado para datos con nulos, duplicados, inconsistencias y evolucion de schema (v1/v2).

## Estado actual

- Primer parcial: diseño preliminar en `docs/primer_parcial_diseno_preliminar.md`.
- Segundo parcial (MVP técnico): flujo end-to-end mínimo.
- **Entrega final**: pipeline completo sobre el dataset real (7 fuentes batch + stream),
  5 marts de negocio y 5 consultas CQL sobre Cassandra. Documentacion en `docs/entrega_final/`.

## Estructura del repo

- `src/pipeline/`: jobs por capa (`bronze_batch.py`, `bronze_stream.py`, `silver.py`, `gold.py`, `cassandra_loader.py`)
- `scripts/run_mvp.py`: runner end-to-end del pipeline
- `scripts/generate_evidence_report.py`: conteos por capa + muestras de Gold/quarantine
- `cassandra/schema.cql`: keyspace `cloud_analytics` y las 5 tablas (referencia)
- `cassandra/queries_finales.cql`: las 5 consultas obligatorias
- `docs/entrega_final/`: arquitectura, diccionario de datos, decisiones/trade-offs, evidencia, idempotencia, presentación.

## Requisitos

Antes de correr el proyecto, asegurate de tener:

- **Java 17 o 21 (JDK)** (probado con OpenJDK 21)
- **Python 3.9 o superior**
- **Docker** (para Cassandra)

El dataset real debe estar en `datalake/landing/` (7 CSV + `usage_events_stream/*.jsonl`). El repo
incluye los 7 CSV, pero `usage_events_stream/*.jsonl` esta gitignoreado por tamano (ver
`.gitignore`): hay que descomprimir el zip de datos provisto por la catedra y copiar su carpeta
`usage_events_stream/` dentro de `datalake/landing/` antes de correr el pipeline. Sin ese paso,
`scripts/bootstrap_sample_landing.py` queda como fallback para generar un stream de ejemplo minimo
(no reproduce los conteos reales documentados en `docs/`).

## Quickstart

1. Crear y activar un entorno virtual e instalar dependencias:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. Ejecutar pipeline local (sin carga Cassandra):

```bash
python scripts/run_mvp.py
```

Recorre Bronze (7 maestros + stream de eventos) -> Silver (calidad, pivot, enriquecimiento) -> Gold (5 marts).

3. Validar la corrida:

```bash
python scripts/generate_evidence_report.py
```

Genera conteos por capa y muestras de quarantine y Gold en `docs/entrega_final/evidencia_ejecucion.md`.

4. Ejecutar pipeline con carga a Cassandra:

```bash
docker run -d --name cassandra-bigdata -p 9042:9042 cassandra:4.1
```

Esperar a que este lista (al inicio falla con "Connection refused" mientras arranca):

```bash
docker exec cassandra-bigdata cqlsh -e "SELECT release_version FROM system.local"
```

Correr el pipeline con carga a Cassandra:

```bash
python scripts/run_mvp.py --with-cassandra --cassandra-host 127.0.0.1 --cassandra-port 9042 --keyspace cloud_analytics
```

El loader crea el keyspace y las tablas automaticamente, asi que `cassandra/schema.cql` es opcional: queda como referencia para crear el schema manualmente sin Spark.

5. Correr las 5 consultas (`cqlsh` va dentro del contenedor):

```bash
docker exec -i cassandra-bigdata cqlsh < cassandra/queries_finales.cql
```

Para apagar y limpiar Cassandra al terminar:

```bash
docker stop cassandra-bigdata && docker rm cassandra-bigdata
```

## Re-correr desde cero / limpiar estado

`bronze_stream` usa checkpoints en `datalake/checkpoints/`. Si se vuelve a correr el pipeline **sin limpiar**, el stream considera los archivos como ya procesados e **ignora los datos nuevos** (comportamiento idempotente).

Para reprocesar todo desde cero, borrar las capas derivadas, los checkpoints y vaciar las tablas de Cassandra:

```bash
rm -rf datalake/bronze datalake/silver datalake/gold datalake/quarantine datalake/checkpoints
docker exec -i cassandra-bigdata cqlsh -e "TRUNCATE cloud_analytics.org_daily_usage_by_service; TRUNCATE cloud_analytics.org_top_services_14d; TRUNCATE cloud_analytics.revenue_by_org_month; TRUNCATE cloud_analytics.tickets_by_org_date; TRUNCATE cloud_analytics.genai_tokens_by_org_date;"
```

## Evidencia y entrega

Dentro de `docs/entrega_final`, se encuentran los archivos:

- Arquitectura y flujo: `arquitectura.md`.
- Diccionario de datos: `diccionario_de_datos.md`.
- Decisiones y trade-offs: `decisiones_y_tradeoffs.md`.
- Reporte de ejecucion (conteos + muestras): `evidencia_ejecucion.md`.
- Resultados de las 5 consultas CQL: `cassandra_query_results.md`.
- Idempotencia: `idempotencia.md`.
- Presentacion: `Cloud_Provider_Analytics_Presentacion.pptx`.
- Video de presentacion: [`https://youtu.be/s0kXyakbjSg`](https://youtu.be/s0kXyakbjSg)

## Nota

Los paths de landing esperados estan definidos en `src/pipeline/config.py` bajo `datalake/landing/`.
El generador sintetico `scripts/bootstrap_sample_landing.py` queda como fallback para demo sin dataset real.
