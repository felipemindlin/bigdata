USE cloud_analytics;

-- Las 5 consultas obligatorias de la entrega final, contra el keyspace cloud_analytics.
-- org de demo: org_sg65kxvf (presente en los 5 marts). Fechas alineadas al dataset real (2025).

-- Q1: costos y requests diarios por org y servicio en un rango de fechas.
SELECT usage_date, cost_usd, requests, cpu_hours, storage_gb_hours, genai_tokens, carbon_kg, has_cost_anomaly
FROM org_daily_usage_by_service
WHERE org_id = 'org_sg65kxvf'
  AND service = 'database'
  AND usage_date >= '2025-07-01'
  AND usage_date <= '2025-08-31';

-- Q2: Top-N servicios por costo acumulado en los ultimos 14 dias para una organizacion.
SELECT service, cost_14d_usd
FROM org_top_services_14d
WHERE org_id = 'org_sg65kxvf'
  AND as_of_date = '2025-08-31'
LIMIT 5;

-- Q3: evolucion de tickets criticos y tasa de SLA breach por dia (ultimos 30 dias).
SELECT ticket_date, total_tickets, critical_count, sla_breach_rate, csat_avg, counts_by_severity
FROM tickets_by_org_date
WHERE org_id = 'org_sg65kxvf'
  AND ticket_date >= '2025-08-01'
  AND ticket_date <= '2025-08-31';

-- Q4: revenue mensual con creditos/impuestos aplicados (normalizado a USD).
SELECT month, revenue_usd, revenue_breakdown
FROM revenue_by_org_month
WHERE org_id = 'org_sg65kxvf';

-- Q5: tokens GenAI y costo estimado por dia.
SELECT usage_date, total_tokens, est_cost_usd
FROM genai_tokens_by_org_date
WHERE org_id = 'org_sg65kxvf'
  AND usage_date >= '2025-08-01'
  AND usage_date <= '2025-08-31';
