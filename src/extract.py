# Databricks notebook source
# MAGIC %md
# MAGIC # Extract — Healthcare synthetic claims
# MAGIC Reads HealthVerity sample claims and writes a staging Delta table.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("source_table", "samples.healthverity.claims_sample_synthetic")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_table = dbutils.widgets.get("source_table")

staging_table = f"{catalog}.{schema}.hc_claims_staging"

# COMMAND ----------

from pyspark.sql import functions as F

# Select claim/ops fields only (synthetic demo dataset; keep payload focused)ddd
columns = [
    "hvid",
    "claim_id",
    "claim_type",
    "date_service",
    "diagnosis_code",
    "procedure_code",
    "patient_gender",
    "patient_year_of_birth",
    "patient_zip3",
    "patient_state",
    "place_of_service_std_id",
    "service_line_number",
    "diagnosis_code_qual",
    "procedure_code_qual",
    "procedure_units_billed",
    "line_charge",
    "line_allowed",
    "payer_type",
    "prov_rendering_npi",
    "prov_billing_npi",
]

raw = spark.table(source_table).select(*columns)

staging = (
    raw.withColumn("extracted_at", F.current_timestamp())
    .withColumn("source_system", F.lit("healthverity_synthetic"))
)

(
    staging.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(staging_table)
)

print(f"Wrote {staging.count():,} rows to {staging_table}")