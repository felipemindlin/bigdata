# Resultados de las 5 consultas (CQL sobre Cassandra)

Keyspace `cloud_analytics`, org de demo `org_sg65kxvf`. Generado con:
```bash
docker exec -i cassandra-bigdata cqlsh < cassandra/queries_finales.cql
```

## Q1 - Costos y requests diarios por org y servicio (rango de fechas)
```

 usage_date | cost_usd | requests | cpu_hours | storage_gb_hours | carbon_kg
------------+----------+----------+-----------+------------------+-----------
 2025-08-31 |   8.1999 |      110 |    4.2198 |          14.6573 |  0.025775
 2025-08-30 |   25.117 |      507 |         0 |          32.3424 |  0.107869
 2025-08-29 |  12.9267 |      245 |    4.1115 |          12.2223 |  0.052267
 2025-08-28 |   0.1333 |        0 |    0.8583 |           1.7784 |  0.000528
 2025-08-27 |  12.4456 |      253 |    7.6446 |           6.7146 |  0.053473
 2025-08-26 |   8.5305 |      125 |    3.2946 |                0 |  0.025659
 2025-08-25 |  24.6536 |      492 |    2.3563 |          10.3237 |  0.100936
 2025-08-24 |  13.0918 |      254 |    1.7082 |          16.8217 |  0.054507
 2025-08-23 |   8.2309 |      123 |     2.415 |           5.9887 |  0.026281
 2025-08-21 |  11.4524 |      216 |    3.8403 |           6.5681 |  0.045282

(10 rows)
```

## Q2 - Top-N servicios por costo acumulado (ultimos 14 dias)
```

 service    | cost_14d_usd
------------+--------------
      genai |     326.2802
   database |     143.9189
  analytics |      85.6481
 networking |       24.423

(4 rows)
```

## Q3 - Tickets criticos y SLA breach por dia (coleccion counts_by_severity)
```

 ticket_date | total_tickets | critical_count | sla_breach_rate | csat_avg | counts_by_severity
-------------+---------------+----------------+-----------------+----------+--------------------
  2025-08-15 |             1 |              0 |               0 |        4 |         {'low': 1}
  2025-07-07 |             1 |              0 |               0 |        4 |        {'high': 1}

(2 rows)
```

## Q4 - Revenue mensual normalizado a USD (coleccion revenue_breakdown)
```

 month      | revenue_usd | revenue_breakdown
------------+-------------+-----------------------------------------------------------------------------------
 2025-08-01 |   599.31872 | {'credits': 4.14897, 'net': 599.31872, 'subtotal': 498.73583, 'taxes': 104.73186}
 2025-07-01 |   826.27094 | {'credits': 6.75536, 'net': 826.27094, 'subtotal': 688.45494, 'taxes': 144.57137}
 2025-06-01 |  1152.95671 |      {'credits': 0, 'net': 1152.95671, 'subtotal': 952.85782, 'taxes': 200.09889}

(3 rows)
```

## Q5 - Tokens GenAI y costo estimado por dia
```

 usage_date | total_tokens | est_cost_usd
------------+--------------+--------------
 2025-08-31 |         1142 |       0.8038
 2025-08-30 |         4048 |      17.7911
 2025-08-29 |         4497 |      46.1605
 2025-08-28 |         4457 |      16.4916
 2025-08-27 |         2931 |      28.8878
 2025-08-26 |         3150 |      18.2715
 2025-08-25 |         4769 |      28.2574
 2025-08-24 |         4010 |      37.1842
 2025-08-23 |         2863 |      19.5124
 2025-08-22 |         2329 |       21.309
 2025-08-21 |         1643 |      11.9809
 2025-08-20 |         1939 |      12.2368

(12 rows)
```
