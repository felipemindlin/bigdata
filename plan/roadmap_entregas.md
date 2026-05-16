# Roadmap de ejecucion (Parcial 2 y Final)

Este roadmap refleja el estado actual del repo y define la ruta de cierre al final.

## Estado actual (MVP Segundo Parcial)

Objetivo de esta etapa: demostrar flujo minimo end-to-end
Landing -> Bronze -> Silver -> Gold (FinOps) -> Cassandra.

Checklist operativo (estado actualizado):
- [x] Ingesta batch de al menos 3 maestros a Bronze Parquet con tipado explicito.
- [x] Ingesta streaming de `usage_events_stream/*.jsonl` con schema explicito.
- [x] Watermark + dedupe por `event_id` + checkpointing activo.
- [x] Silver minimo: limpieza de eventos + enriquecimiento con maestro de organizaciones.
- [x] 3 reglas de calidad activas + salida a quarantine.
- [x] Gold minimo: `org_daily_usage_by_service`.
- [x] Keyspace y tabla Cassandra query-first para el mart minimo.
- [x] 2 consultas CQL minimas preparadas (query #1 y #2).
- [x] Estrategia de idempotencia para reproceso sin duplicados.

Artefactos implementados en repo:
- Codigo reproducible PySpark (`src/pipeline/*.py`, `scripts/run_mvp.py`).
- Zonas de datos estandarizadas (pathing por capa definido en config).
- Scripts CQL (`cassandra/schema.cql`, `cassandra/queries_minimas.cql`).
- Checklist y evidencia de ejecucion (`docs/segundo_parcial/mvp_checklist.md`).

## Pendientes para entrega del Segundo Parcial

Estos puntos son de evidencia/ejecucion en entorno, no de diseno:
- [ ] Ejecutar el pipeline completo con dataset real provisto.
- [ ] Capturar evidencia de conteos por capa (Bronze/Silver/Gold/quarantine).
- [ ] Ejecutar y capturar resultados de las 2 consultas en Cassandra.
- [ ] Adjuntar evidencia de idempotencia (re-run sin duplicados).

Evidencias generadas localmente (entorno de desarrollo):
- [x] Conteos por capa y muestras: `docs/segundo_parcial/evidencia_ejecucion.md`.
- [x] Idempotencia (re-ejecucion estable): `docs/segundo_parcial/idempotencia.md`.
- [x] Compilado final (documentos + evidencia): `docs/segundo_parcial/entrega_final.pdf`.

Notas:
- La ejecucion y verificacion con el *dataset real provisto* y la carga/consulta en Cassandra dependen de disponer el servicio de Cassandra (`cqlsh`) accesible; estos pasos quedan pendientes si no se dispone del cluster en el entorno de entrega.
- Los artefactos anteriores se generaron en un entorno virtual local (`.venv`) con `pyspark` instalado; ver `README.md` para reproducir exactamente los comandos usados.

## Etapa Final (expansion)

Objetivo: ampliar cobertura funcional y reforzar demo de negocio.

Checklist de ampliacion:
- [ ] Completar marts Gold faltantes: `revenue_by_org_month`, `cost_anomaly_mart`, `tickets_by_org_date`, `genai_tokens_by_org_date`.
- [ ] Implementar metodo formal de anomalias (z-score, MAD o percentiles) y justificar.
- [ ] Completar consultas minimas #3, #4 y #5 en Cassandra con evidencia.
- [ ] Optimizar performance (particiones, repartition/coalesce, compactacion/reparquet).
- [ ] Cerrar documentacion final de trade-offs y decisiones.
- [ ] Preparar presentacion y video final.

## Plan de iteraciones hacia Final

- Iteracion F1: ampliar Silver y features para Soporte/Producto.
- Iteracion F2: construir marts Gold restantes.
- Iteracion F3: ampliar serving Cassandra para consultas #3/#4/#5.
- Iteracion F4: tuning, pruebas de idempotencia/performance y cierre narrativo.

## Criterio de "listo"

- Cada iteracion debe cerrar con evidencia reproducible (comando + salida + captura).
- No avanzar etapa sin control de calidad ni validacion de idempotencia.
- Toda salida de Gold debe existir en Parquet y en Cassandra cuando aplique.

## Apropiacion tecnica (accion correctiva)

Para responder a la observacion academica sobre uso de IA:
- Mantener bitacora de decisiones del equipo (`docs/segundo_parcial/bitacora_apropiacion_tecnica.md`).
- Defender oralmente trade-offs y decisiones de modelado (no solo mostrar resultados).
- Adjuntar evidencia de ejecucion propia (logs, capturas, conteos, consultas).
