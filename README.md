# Healthcare Sample Claims ETL

Sample Lakeflow Job that runs a three-step ETL over Databricks' **HealthVerity synthetic claims** dataset.

| Step | Task | Output |
|------|------|--------|
| Extract | Read `samples.healthverity.claims_sample_synthetic` | `workspace.default.hc_claims_staging` |
| Transform | Cast/clean/enrich (age band, claim type label, service month) | `workspace.default.hc_claims_cleaned` |
| Load | Monthly aggregates by state / claim type / age / payer | `workspace.default.hc_claims_monthly_summary` |

Source data is **synthetic** and for educational/non-production use only.

## Deploy and run

```powershell
databricks bundle validate --strict --target dev --profile dbc-7c3eed4c
databricks bundle deploy -t dev --profile dbc-7c3eed4c
databricks bundle run healthcare_sample_etl -t dev --profile dbc-7c3eed4c
```
