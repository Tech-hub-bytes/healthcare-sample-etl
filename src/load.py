# Databricks notebook source
# MAGIC %md
# MAGIC # Load — Monthly claims summary
# MAGIC Aggregate cleaned claims into a curated healthcare metrics table.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

cleaned_table = f"{catalog}.{schema}.hc_claims_cleaned"
summary_table = f"{catalog}.{schema}.hc_claims_monthly_summary"

# COMMAND ----------

from pyspark.sql import functions as F

cleaned = spark.table(cleaned_table)

summary = (
    cleaned.groupBy(
        "service_month",
        "patient_state",
        "claim_type_label",
        "age_band",
        "payer_type",
    )
    .agg(
        F.count("*").alias("service_line_count"),
        F.countDistinct("claim_id").alias("claim_count"),
        F.countDistinct("hvid").alias("patient_count"),
        F.round(F.sum("line_charge_amt"), 2).alias("total_charge"),
        F.round(F.sum("line_allowed_amt"), 2).alias("total_allowed"),
        F.round(F.avg("line_charge_amt"), 2).alias("avg_line_charge"),
        F.round(F.avg("units_billed"), 2).alias("avg_units_billed"),
    )
    .withColumn("loaded_at", F.current_timestamp())
    .orderBy("service_month", "patient_state", "claim_type_label")
)

(
    summary.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(summary_table)
)

print(f"Wrote {summary.count():,} aggregate rows to {summary_table}")
display(summary.limit(20))
