# Roadmap de ejecucion (Parcial 2 y Final)

Este roadmap toma como base el diseno del primer parcial y lo convierte en pasos ejecutables.

## Etapa siguiente: Segundo Parcial (MVP tecnico)

Objetivo: lograr un flujo end-to-end minimo Landing -> Bronze -> Silver -> Gold (FinOps) -> Cassandra.

Checklist operativo:
- [ ] Ingesta batch de al menos 3 maestros a Bronze Parquet con tipado explicito.
- [ ] Ingesta streaming de usage_events_stream con schema explicito.
- [ ] Watermark + dedupe por event_id + checkpointing activo.
- [ ] Silver minimo: limpieza de eventos + 1 maestro enriquecido.
- [ ] Activar 3 reglas de calidad y quarantine con muestras.
- [ ] Gold minimo: org_daily_usage_by_service.
- [ ] Crear keyspace y tabla Cassandra query-first para ese mart.
- [ ] Ejecutar 2 consultas CQL minimas y guardar evidencias.
- [ ] Validar idempotencia: reproceso sin duplicados.

Entregables:
- Codigo reproducible (notebook o .py).
- Estructura de zonas Bronze/Silver/Gold en Parquet.
- Scripts CQL.
- README con quickstart.

## Etapa final: Proyecto completo

Objetivo: ampliar cobertura funcional y de negocio, y fortalecer narrativa de demo.

Checklist de ampliacion:
- [ ] Completar marts Gold restantes (Soporte y Producto/GenAI).
- [ ] Incorporar revenue_by_org_month y cost_anomaly_mart.
- [ ] Implementar metodo(s) de anomalia (z-score, MAD o percentiles).
- [ ] Optimizar performance (particionado, repartition/coalesce, reparquet).
- [ ] Responder 5 consultas minimas sobre Cassandra con CQL y evidencia.
- [ ] Completar documentacion de decisiones y trade-offs.
- [ ] Preparar presentacion y video con storytelling tecnico.

## Estrategia de implementacion recomendada
- Iteracion 1: Bronze batch + streaming con idempotencia.
- Iteracion 2: Silver + calidad + quarantine.
- Iteracion 3: Gold FinOps + Cassandra (consultas 1 y 2).
- Iteracion 4: Marts adicionales + consultas 3, 4 y 5.
- Iteracion 5: performance, pulido, demo final.

## Criterio de "listo"
- Cada iteracion queda cerrada con evidencia reproducible.
- No se avanza a la siguiente sin chequeo de calidad e idempotencia.
- Cada salida de Gold existe en Parquet y en Cassandra segun corresponda.
