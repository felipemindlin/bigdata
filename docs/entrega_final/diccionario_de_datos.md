# Diccionario de datos - Entrega Final

## Landing (raw, inmutable)

### customers_orgs.csv (maestro de organizaciones)
`org_id`, `org_name`, `industry`, `hq_region`, `plan_tier`, `is_enterprise`, `signup_date`,
`sales_rep`, `lifecycle_stage`, `marketing_source`, `nps_score`.

### users.csv (usuarios por org)
`user_id`, `org_id`, `email`, `role`, `active`, `created_at`, `last_login`.

### resources.csv (recursos cloud)
`resource_id`, `org_id`, `service` (compute/storage/database/networking/analytics/genai),
`region`, `created_at`, `state`, `tags_json`.

### support_tickets.csv (soporte)
`ticket_id`, `org_id`, `category`, `severity` (critical/high/medium/low), `created_at`,
`resolved_at`, `csat` (1-5), `sla_breached` (bool).

### marketing_touches.csv (marketing)
`touch_id`, `org_id`, `campaign`, `channel`, `timestamp`, `clicked`, `converted`.

### nps_surveys.csv (NPS temporal)
`org_id`, `survey_date`, `nps_score`, `comment`.

### billing_monthly.csv (facturacion)
`invoice_id`, `org_id`, `month`, `subtotal`, `credits`, `taxes`, `currency` (USD/ARS),
`exchange_rate_to_usd`.

### usage_events_stream/*.jsonl (eventos de uso - formato EAV)
`event_id`, `timestamp`, `org_id`, `resource_id`, `service`, `region`, `metric`
(requests/cpu_hours/storage_gb_hours), `value`, `unit`, `cost_usd_increment`, `schema_version`
(1 o 2), `carbon_kg` (solo v2), `genai_tokens` (solo v2/genai).

## Bronze

Mismo grano que Landing + columnas tecnicas: `ingest_ts`, `source_file`, `ingest_date`.
Stream: `timestamp` -> `event_ts`; watermark 1h declarado; particionado por `ingest_date`.

## Silver

### usage_enriched (evento valido + enriquecido)
Campos de evento + `value_num` (cast con fallback), `usage_date`, `cost_anomaly_flag`,
enriquecimiento: `industry`, `plan_tier`, `hq_region` (join orgs) y `resource_state`,
`resource_region` (join resources). Reglas: `event_id` no nulo; `cost_usd_increment >= -0.01`;
`unit` no nulo si `value` no nulo.

### daily_features (agregado por org/servicio/dia)
`org_id`, `service`, `usage_date`, `daily_cost_usd`, `requests`, `cpu_hours`, `storage_gb_hours`
(pivot de `metric`), `genai_tokens`, `carbon_kg`, `has_cost_anomaly`.

### tickets (conformado)
Tickets validos + `ticket_date`, `severity` normalizada. Reglas: `org_id`/`ticket_date` no nulos;
severidad en dominio; `csat` en [1,5].

### billing_norm (normalizado a USD)
`subtotal_usd`, `credits_usd`, `taxes_usd`, `revenue_usd`, `month_date`. Reglas: `org_id`/`month`
no nulos; `subtotal >= 0`.

### quarantine/
`silver_quality` (eventos), `tickets_quality`, `billing_quality`, `late_data` (event-time nulo).
Cada uno con columna `quality_issue`.

## Gold (marts de negocio) = tablas Cassandra

| Mart | Grano (PK) | Metricas | Coleccion |
|---|---|---|---|
| org_daily_usage_by_service | ((org_id, service), usage_date) | cost_usd, requests, cpu_hours, storage_gb_hours, genai_tokens, carbon_kg, has_cost_anomaly | - |
| org_top_services_14d | ((org_id, as_of_date), cost_14d_usd, service) | cost_14d_usd | - |
| revenue_by_org_month | ((org_id), month) | revenue_usd, subtotal_usd, credits_usd, taxes_usd | `revenue_breakdown` map<text,double> |
| tickets_by_org_date | ((org_id), ticket_date) | total_tickets, critical_count, sla_breach_rate, csat_avg | `counts_by_severity` map<text,int> |
| genai_tokens_by_org_date | ((org_id), usage_date) | total_tokens, est_cost_usd | - |
