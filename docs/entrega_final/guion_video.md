# Guion de video - Entrega Final (Cloud Provider Analytics)

Guion palabra por palabra para grabar el video explicativo, mapeado 1:1 a las 12 slides de
`Cloud_Provider_Analytics_Presentacion.pptx`. Duración estimada: 8-10 minutos. Repartir los
bloques entre los integrantes del equipo (indicado como sugerencia, ajustar según quién domine
mejor cada parte).

## 1. Portada (15s) — Todo el equipo / quien abre

"Hola, somos Jeremias, Felipe, Martin, Florencia y Gianfranco. Este es nuestro proyecto final de
Big Data, Cloud Provider Analytics: un pipeline de ETL, streaming y serving en Cassandra para un
proveedor de nube."

## 2. Contexto de negocio (40s)

"Nos ponemos en el lugar del equipo de datos de un proveedor cloud. Tenemos que cubrir dos
necesidades en paralelo: métricas operativas en near real-time —uso y costo— y procesamiento
batch para maestros de clientes y facturación. El desafío es que los datos llegan crudos: con
nulos, tipos ambiguos, anomalías, y una evolución de esquema a mitad del histórico, donde
aparecen campos nuevos como `carbon_kg` y `genai_tokens`. Nuestro objetivo final es habilitar
analítica para tres áreas de negocio: FinOps, Soporte y Producto."

## 3. Arquitectura (45s)

"Elegimos un patrón Lambda: batch para los maestros y la facturación, y streaming estructurado
para los eventos de uso, porque son naturalmente distintos en su necesidad de latencia. Ambos
caminos convergen en un Data Lake organizado en zonas —Landing, Bronze, Silver y Gold— construido
en Parquet y gestionado por Spark. Desde Gold, publicamos los marts de negocio a Cassandra, en un
único keyspace llamado `cloud_analytics`, modelado query-first: una tabla por cada consulta que
necesitábamos responder."

## 4. Tecnologías y recursos (30s)

"Usamos PySpark 3.5.1 con Structured Streaming y batch, Parquet como storage intermedio,
Cassandra 4.1 corriendo en Docker, y el conector Spark-Cassandra para la carga. Todo corre sobre
Java 21, en un entorno local: para esta entrega priorizamos evidencia 100% reproducible con
scripts locales en vez de depender de Colab o de AstraDB en la nube, así que el costo de
ejecución completo fue de cero dólares."

## 5. Datos y las 5V (35s)

"El dataset real incluye 7 fuentes en batch —clientes, usuarios, recursos, tickets, marketing,
NPS y facturación— más un stream de 43.200 eventos de uso en formato JSONL, con dos versiones de
esquema conviviendo. Si lo miramos con el lente de las 5V del Big Data: hay volumen en los
eventos y los maestros, velocidad por el streaming con watermark de una hora, variedad por los
formatos y versiones de esquema, veracidad porque tuvimos que aislar datos inválidos en
quarantine, y valor porque todo termina en cinco marts de negocio listos para consultarse."

## 6. Ingesta - Bronze (35s)

"En el camino batch, leemos los 7 maestros con esquema explícito, les agregamos columnas
técnicas como `ingest_ts` y `source_file`, deduplicamos por clave natural, y particionamos por
fecha de ingesta. En el camino streaming, leemos el feed de eventos con `Structured Streaming`,
declaramos un watermark de una hora sobre el event-time, y separamos los eventos con fecha nula o
tardía hacia una zona de quarantine específica, `late_data`, con checkpointing activo para poder
reprocesar sin duplicar."

## 7. Calidad y conformación - Silver (50s)

"De los 43.200 eventos que ingresan, 40.956 pasan las reglas de calidad y 2.244 quedan en
quarantine, principalmente por unidad nula cuando había un valor, o por costos negativos fuera de
rango. En Silver hacemos varias cosas clave: convertimos los eventos de formato largo,
metric-value-unit, a columnas anchas por servicio; compatibilizamos las versiones de esquema uno
y dos completando con cero los campos que no existían en la versión anterior; enriquecemos con
los datos de organización y de recursos; calculamos un flag de anomalía de costo usando el
percentil 99; y normalizamos la facturación a dólares usando el tipo de cambio que viene
específicamente en cada factura, en vez de una tabla de conversión externa."

## 8. Marts de negocio y modelo Cassandra (40s)

"Publicamos cinco marts: uso diario por organización y servicio, el top de servicios por costo en
los últimos 14 días, revenue mensual, tickets por organización y día, y consumo de tokens de
GenAI. Todas viven en el mismo keyspace, modeladas por consulta para evitar full-scans. Y en dos
de las tablas usamos colecciones de Cassandra con valor analítico real: un mapa de cantidad de
tickets por severidad, y un mapa con el desglose de revenue en subtotal, créditos, impuestos y
neto."

## 9. Demo: las 5 consultas (60s) — grabar pantalla ejecutando

"Ahora la demo. Vamos a correr las cinco consultas obligatorias contra Cassandra."

[Grabar pantalla mientras se ejecuta `docker exec -i cassandra-bigdata cqlsh < cassandra/queries_finales.cql`]

"La consulta uno nos da costos y requests diarios por organización y servicio en un rango de
fechas. La dos, el top de servicios por costo acumulado en los últimos 14 días. La tres, la
evolución de tickets críticos y la tasa de incumplimiento de SLA por día. La cuatro, el revenue
mensual ya normalizado a dólares, con su desglose. Y la cinco, los tokens de GenAI y el costo
estimado por día. Los resultados completos están documentados en
`cassandra_query_results.md`."

## 10. Idempotencia y performance (35s)

"Un requisito clave era poder reprocesar sin duplicar. Corrimos el pipeline completo dos veces
seguidas sin limpiar el estado, y comparamos los conteos en las cinco tablas de Cassandra antes y
después: exactamente los mismos números en las cinco. Esto lo logramos combinando checkpointing
en el streaming, deduplicación por `event_id` en Silver, escritura en modo overwrite en Parquet,
y upserts por clave primaria natural en Cassandra. Para performance, particionamos por fecha en
todas las capas derivadas y usamos coalesce en los marts más chicos para evitar el problema de
muchos archivos pequeños."

## 11. Decisiones y trade-offs (45s)

"Algunas decisiones que vale la pena explicar. Elegimos Lambda por sobre Kappa porque forzar los
maestros y la facturación a comportarse como stream no aportaba ningún beneficio real. Decidimos
hacer la deduplicación de eventos en Silver, en modo batch, y no con estado dentro del streaming,
porque con casi dos meses de datos desordenados el operador con estado terminaba descartando
eventos válidos por la eviccion del watermark. Para las anomalías de costo elegimos el método de
percentiles, por ser robusto ante colas pesadas y fácil de justificar frente a alternativas como
z-score o MAD. Y para el tipo de cambio, en vez de mantener una tabla externa, usamos el que
viene directamente en cada factura, que es más fiel a la realidad del dato."

## 12. Cierre (30s)

"En resumen: este pipeline entrega valor concreto a FinOps, a Soporte y a Producto, con datos
confiables, trazables e idempotentes. Si esto pasara a producción, los próximos pasos serían
reemplazar los archivos JSONL por un broker Kafka real, mover Cassandra a AstraDB en la nube, y
conectar herramientas de BI directamente sobre el keyspace. Gracias por ver nuestro proyecto."

---

## Notas de producción

- Grabar la sección 9 (demo) como captura de pantalla real corriendo los comandos del
  [README](../../README.md), no como relato: es lo que más valora el criterio de "Diseño y
  desarrollo" de la rúbrica.
- Mantener el tono técnico y directo; evitar relleno. El criterio de "Storytelling" valora
  claridad narrativa, no extensión.
- Si se graba por partes, dejar 1-2 segundos de silencio al inicio/fin de cada bloque para
  facilitar el corte en edición.
