# Evidencia de idempotencia

Se ejecuto `python scripts/run_mvp.py` dos veces consecutivas sobre datos sintéticos generados por `scripts/bootstrap_sample_landing.py`.

## Conteos actuales por dataset

| dataset | filas |
|---|---:|
| bronze_customers | 3 |
| bronze_users | 4 |
| bronze_billing | 3 |
| bronze_stream | 5 |
| silver_usage | 3 |
| silver_features | 3 |
| quarantine_quality | 2 |
| quarantine_late | 1 |
| gold_finops | 3 |

## Conclusión
- Los conteos son consistentes con la ejecucion esperada en el entorno local.
- La re-ejecucion no incrementó filas en Gold.