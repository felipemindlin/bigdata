USE cloud_analytics;

-- Query 1: costos y requests diarios por org y servicio en rango de fechas
SELECT usage_date, cost_usd, requests, genai_tokens, carbon_kg
FROM org_daily_usage_by_service
WHERE org_id = 'org_001'
  AND service = 'compute'
  AND usage_date >= '2026-04-01'
  AND usage_date <= '2026-04-30';

-- Query 2: Top-N servicios por costo acumulado en ultimos 14 dias
-- (se consulta tabla preagregada, generada desde Spark)
SELECT service, cost_14d_usd
FROM org_top_services_14d
WHERE org_id = 'org_001'
  AND as_of_date = '2026-05-16'
LIMIT 5;
