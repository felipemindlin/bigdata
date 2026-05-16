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
- `plan/roadmap_entregas.md`: roadmap actualizado (Parcial 2 + Final)
- `docs/segundo_parcial/mvp_checklist.md`: checklist de evidencia de entrega
- `docs/segundo_parcial/bitacora_apropiacion_tecnica.md`: decisiones y trade-offs

## Quickstart (MVP segundo parcial)

1. Instalar dependencias:
1. Crear y activar un entorno virtual (recomendado) e instalar dependencias:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. Ejecutar pipeline local (sin carga Cassandra):

```bash
# generar datos de ejemplo (si no hay landing real)
python scripts/bootstrap_sample_landing.py

# ejecutar flujo end-to-end (bronze -> silver -> gold)
python scripts/run_mvp.py
```

3. Ejecutar pipeline con carga a Cassandra (si dispone de Cassandra local/remote):

```bash
python scripts/run_mvp.py --with-cassandra --cassandra-host 127.0.0.1 --cassandra-port 9042 --keyspace cloud_analytics
```

4. Crear schema y correr consultas:

```bash
cqlsh -f cassandra/schema.cql
cqlsh -f cassandra/queries_minimas.cql
```

## Evidencia y entrega

- Reporte de ejecucion (conteos por capa, muestras de quarantine y Gold): `docs/segundo_parcial/evidencia_ejecucion.md`.
- Reporte de idempotencia: `docs/segundo_parcial/idempotencia.md`.
- Compilado final listo para entrega: `docs/segundo_parcial/entrega_final.pdf`.

Para regenerar la entrega final localmente (requiere `pandoc`):

```bash
# concatenar documentos y generar PDF
pandoc docs/segundo_parcial/entrega_final.md -o docs/segundo_parcial/entrega_final.pdf
```

## Nota

Los paths de landing esperados estan definidos en `src/pipeline/config.py` bajo `datalake/landing/`.
