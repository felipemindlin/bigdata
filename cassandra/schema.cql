-- Keyspace
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cloud_analytics;

-- Mart minimo del segundo parcial (query #1)
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
  org_id text,
  service text,
  usage_date date,
  cost_usd double,
  requests double,
  genai_tokens double,
  carbon_kg double,
  has_cost_anomaly boolean,
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);

-- Tabla auxiliar para query #2 Top-N servicios por costo (ultimos 14 dias)
CREATE TABLE IF NOT EXISTS org_top_services_14d (
  org_id text,
  as_of_date date,
  service text,
  cost_14d_usd double,
  PRIMARY KEY ((org_id, as_of_date), cost_14d_usd, service)
) WITH CLUSTERING ORDER BY (cost_14d_usd DESC, service ASC);
