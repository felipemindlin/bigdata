# bigdata - Cloud Provider Analytics

Este proyecto fue realizado por Jeremias Feferovich, Felipe Mindlin, Martin Zahnd, Florencia Carrica y Gianfranco Magliotti, en el contexto de la materia 72.80 - Big Data.

## Contexto

El proyecto simula el rol del equipo de datos de un proveedor cloud que debe cubrir:

- metricas operativas near real-time de uso/costo
- procesamiento batch diario/mensual para maestros y facturacion

El pipeline esta pensado para datos con nulos, duplicados, inconsistencias y evolucion de schema (v1/v2).

## Estado actual

- Primer parcial: diseno preliminar completo en `docs/primer_parcial_diseno_preliminar.md`.
- Segundo parcial (MVP tecnico): implementado en PySpark + Cassandra scripts.

## Estructura del repo

- `src/pipeline/`: jobs por capa (`bronze_batch.py`, `bronze_stream.py`, `silver.py`, `gold.py`, `cassandra_loader.py`)
- `scripts/run_mvp.py`: runner end-to-end del MVP tecnico
- `cassandra/schema.cql`: keyspace y tablas
- `cassandra/queries_minimas.cql`: consultas minimas (#1 y #2)
- `docs/segundo_parcial/mvp_checklist.md`: checklist de evidencia de entrega

## Requisitos

Antes de correr el proyecto, asegurate de tener:

- **Java 17 (JDK)**
- **Python 3.9 o superior**
- **Docker**

## Quickstart (MVP segundo parcial)

1. Crear y activar un entorno virtual (recomendado) e instalar dependencias:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. Ejecutar pipeline local (sin carga Cassandra):

```bash
# generar datos de ejemplo (si no hay landing real).
# Fijar fecha de referencia para que la demo sea reproducible
# (la query #1 espera usage_date en junio 2026).
export BOOTSTRAP_REFERENCE_TS="2026-06-15T12:00:00+00:00"
python scripts/bootstrap_sample_landing.py

# ejecutar flujo end-to-end (bronze -> silver -> gold)
python scripts/run_mvp.py
```

3. Validar la corrida:

```bash
python scripts/generate_evidence_report.py
```

Recorre todas las capas, cuenta filas y genera muestras de quarantine y Gold en `docs/segundo_parcial/evidencia_ejecucion.md`. Abrir ese archivo y verificar los conteos por capa y las muestras contra los valores documentados.

4. Ejecutar pipeline con carga a Cassandra:

Levantar Cassandra en Docker:

```bash
docker run -d --name cassandra-bigdata -p 9042:9042 cassandra:4.1
```

Esperar a que este lista (repetir hasta que devuelva la version sin error; al inicio falla con "Connection refused" mientras arranca):

```bash
docker exec cassandra-bigdata cqlsh -e "SELECT release_version FROM system.local"
```

Correr el pipeline con carga a Cassandra:

```bash
python scripts/run_mvp.py --with-cassandra --cassandra-host 127.0.0.1 --cassandra-port 9042 --keyspace cloud_analytics
```

El loader crea el keyspace y las tablas automaticamente, asi que `cassandra/schema.cql` es opcional: queda como referencia para crear el schema manualmente sin Spark.

5. Correr las consultas minimas (`cqlsh` va dentro del contenedor):

```bash
docker exec -i cassandra-bigdata cqlsh < cassandra/queries_minimas.cql
```

Para apagar y limpiar Cassandra al terminar:

```bash
docker stop cassandra-bigdata && docker rm cassandra-bigdata
```

## Re-correr desde cero / limpiar estado

`bronze_stream` es un job de streaming que usa checkpoints en `datalake/checkpoints/`. Si se regenera el landing y se vuelve a correr el pipeline **sin limpiar**, el stream considera los archivos como ya procesados e **ignora los datos nuevos**.

Para reprocesar todo desde cero, borrar las capas derivadas, los checkpoints y vaciar las tablas de Cassandra:

```bash
rm -rf datalake/bronze datalake/silver datalake/gold datalake/quarantine datalake/checkpoints
docker exec -i cassandra-bigdata cqlsh -e "TRUNCATE cloud_analytics.org_daily_usage_by_service; TRUNCATE cloud_analytics.org_top_services_14d;"
```

## Evidencia y entrega

- Reporte de ejecucion (conteos por capa, muestras de quarantine y Gold): `docs/segundo_parcial/evidencia_ejecucion.md`.
- Reporte de idempotencia: `docs/segundo_parcial/idempotencia.md`.
- Compilado final listo para entrega: `docs/segundo_parcial/entrega_final.pdf`.

Para regenerar la entrega final localmente (requiere `pandoc`):

```bash
pandoc docs/segundo_parcial/entrega_final.md \
  -o docs/segundo_parcial/entrega_final.pdf \
  --pdf-engine=xelatex \
  -H docs/segundo_parcial/pandoc_header.tex \
  -V geometry:left=25mm \
  -V geometry:top=20mm \
  -V geometry:right=20mm \
  -V geometry:bottom=20mm
```

## Nota

Los paths de landing esperados estan definidos en `src/pipeline/config.py` bajo `datalake/landing/`.
