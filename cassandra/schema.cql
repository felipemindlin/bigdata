-- Keyspace
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cloud_analytics;

-- Q1: costos y requests diarios por org y servicio (rango de fechas)
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
  org_id text,
  service text,
  usage_date date,
  cost_usd double,
  requests double,
  cpu_hours double,
  storage_gb_hours double,
  genai_tokens double,
  carbon_kg double,
  has_cost_anomaly boolean,
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Q2: Top-N servicios por costo acumulado en ultimos 14 dias
CREATE TABLE IF NOT EXISTS org_top_services_14d (
  org_id text,
  as_of_date date,
  service text,
  cost_14d_usd double,
  PRIMARY KEY ((org_id, as_of_date), cost_14d_usd, service)
) WITH CLUSTERING ORDER BY (cost_14d_usd DESC, service ASC);

-- Q3: evolucion de tickets criticos y SLA breach por dia
CREATE TABLE IF NOT EXISTS tickets_by_org_date (
  org_id text,
  ticket_date date,
  total_tickets int,
  critical_count int,
  sla_breach_rate double,
  csat_avg double,
  counts_by_severity map<text, int>,
  PRIMARY KEY ((org_id), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- Q4: revenue mensual normalizado a USD
CREATE TABLE IF NOT EXISTS revenue_by_org_month (
  org_id text,
  month date,
  revenue_usd double,
  subtotal_usd double,
  credits_usd double,
  taxes_usd double,
  revenue_breakdown map<text, double>,
  PRIMARY KEY ((org_id), month)
) WITH CLUSTERING ORDER BY (month DESC);

-- Q5: tokens GenAI y costo estimado por dia
CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
  org_id text,
  usage_date date,
  total_tokens double,
  est_cost_usd double,
  PRIMARY KEY ((org_id), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);
