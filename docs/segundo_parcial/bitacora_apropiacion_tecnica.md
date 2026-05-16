# Bitacora de apropiacion tecnica

Este archivo registra decisiones tomadas por el equipo para demostrar apropiacion del trabajo tecnico.

## Decisiones clave tomadas por el equipo

1. Se eligio Lambda y no Kappa porque el dominio combina fuentes naturalmente batch y streaming.
2. En streaming se uso watermark de 1 hora para balancear precision y complejidad operativa.
3. Se implemento quarantine en Silver para no bloquear el pipeline por errores de calidad.
4. Se modelo Cassandra por consulta (query-first), incluyendo tabla auxiliar para Top-N 14 dias.
5. Se priorizo idempotencia en todas las capas para permitir reproceso seguro.

## Trade-offs explicitados

- Simplicidad en Colab vs escalabilidad total de produccion.
- Tabla auxiliar en Cassandra para query #2 vs agregacion ad-hoc por cliente.
- `availableNow` para reproducibilidad del MVP vs stream continuo de larga vida.

## Preguntas de defensa sugeridas (oral)

- Por que watermark de 1h y no 10m o 6h.
- Como cambiaria el diseno al pasar de JSONL files a Kafka.
- Que impacto tienen las claves de particion elegidas en Cassandra.
- Que reglas de calidad se endurecerian primero para produccion.
