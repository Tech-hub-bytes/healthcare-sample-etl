# Databricks notebook source
# MAGIC %md
# MAGIC # Transform — Clean and enrich claims
# MAGIC Cast types, filter invalid rows, derive age band and service month.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

staging_table = f"{catalog}.{schema}.hc_claims_staging"
cleaned_table = f"{catalog}.{schema}.hc_claims_cleaned"

# COMMAND ----------

from pyspark.sql import functions as F

staging = spark.table(staging_table)

cleaned = (
    staging.withColumn("service_date", F.to_date("date_service"))
    .withColumn("line_charge_amt", F.col("line_charge").cast("double"))
    .withColumn("line_allowed_amt", F.col("line_allowed").cast("double"))
    .withColumn("units_billed", F.col("procedure_units_billed").cast("double"))
    .withColumn("birth_year", F.col("patient_year_of_birth").cast("int"))
    .withColumn(
        "claim_type_label",
        F.when(F.col("claim_type") == "P", "professional")
        .when(F.col("claim_type") == "I", "institutional")
        .otherwise("unknown"),
    )
    .withColumn(
        "patient_age_approx",
        F.year("service_date") - F.col("birth_year"),
    )
    .withColumn(
        "age_band",
        F.when(F.col("patient_age_approx") < 18, "0-17")
        .when(F.col("patient_age_approx") < 35, "18-34")
        .when(F.col("patient_age_approx") < 50, "35-49")
        .when(F.col("patient_age_approx") < 65, "50-64")
        .when(F.col("patient_age_approx") >= 65, "65+")
        .otherwise("unknown"),
    )
    .withColumn("service_month", F.date_trunc("month", F.col("service_date")).cast("date"))
    .withColumn("transformed_at", F.current_timestamp())
    # Quality filters for demo ETL
    .filter(F.col("service_date").isNotNull())
    .filter(F.col("claim_id").isNotNull())
    .filter(F.col("hvid").isNotNull())
    .filter(
        (F.col("line_charge_amt").isNull())
        | (F.col("line_charge_amt") >= 0)
    )
)

# Keep analytic columns
output_cols = [
    "hvid",
    "claim_id",
    "claim_type",
    "claim_type_label",
    "service_date",
    "service_month",
    "diagnosis_code",
    "procedure_code",
    "patient_gender",
    "birth_year",
    "patient_age_approx",
    "age_band",
    "patient_zip3",
    "patient_state",
    "place_of_service_std_id",
    "service_line_number",
    "diagnosis_code_qual",
    "procedure_code_qual",
    "units_billed",
    "line_charge_amt",
    "line_allowed_amt",
    "payer_type",
    "prov_rendering_npi",
    "prov_billing_npi",
    "source_system",
    "extracted_at",
    "transformed_at",
]

result = cleaned.select(*output_cols)

(
    result.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cleaned_table)
)

print(f"Wrote {result.count():,} cleaned rows to {cleaned_table}")
