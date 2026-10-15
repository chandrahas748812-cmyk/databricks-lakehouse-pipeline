# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Aggregates (star-schema marts + KPIs)
# MAGIC
# MAGIC **What this does:** Silver → Gold. Builds a small star schema
# MAGIC (`fact_claims` + `dim_date`) on top of the Silver dimensions, then
# MAGIC publishes business-ready aggregate marts and a sample KPI query.
# MAGIC
# MAGIC Gold = aggregated, BI-ready, one row per business question.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "healthcare_lakehouse", "Catalog")

CATALOG = dbutils.widgets.get("catalog")
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_date + fact_claims

# COMMAND ----------

dates = (
    spark.table(f"{SILVER}.claims_silver")
    .select(F.col("service_date").alias("date"))
    .distinct()
)
dim_date = (
    dates.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("day_of_week", F.date_format("date", "E"))
)
dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.dim_date")

claims = spark.table(f"{SILVER}.claims_silver")
providers = spark.table(f"{SILVER}.dim_provider").filter("is_current = true")

# as-of join: provider tier that was current on the service date (SCD2 demo)
providers_hist = spark.table(f"{SILVER}.dim_provider")
fact_claims = (
    claims.alias("c")
    .join(
        providers_hist.alias("p"),
        (F.col("c.provider_id") == F.col("p.provider_id"))
        & (F.col("c.service_date") >= F.col("p.effective_date"))
        & (
            F.col("p.end_date").isNull()
            | (F.col("c.service_date") < F.col("p.end_date"))
        ),
        "left",
    )
    .select(
        F.col("c.claim_id"),
        F.col("c.patient_id"),
        F.col("c.provider_id"),
        F.col("p.network_tier"),
        F.col("c.service_date"),
        F.date_format(F.col("c.service_date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("c.diagnosis_code"),
        F.col("c.procedure_code"),
        F.col("c.claim_amount"),
        F.col("c.claim_status"),
        F.col("c.payer"),
    )
)
fact_claims.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD}.fact_claims")
print(f"fact_claims: {fact_claims.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gold marts (business aggregates)

# COMMAND ----------

# Mart 1: monthly cost & volume by payer
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.mart_monthly_payer AS
SELECT d.year, d.month, d.month_name, f.payer,
       COUNT(*)                       AS claim_count,
       ROUND(SUM(f.claim_amount), 2)   AS total_paid,
       ROUND(AVG(f.claim_amount), 2)   AS avg_claim,
       SUM(CASE WHEN f.claim_status = 'denied' THEN 1 ELSE 0 END) AS denied_count
FROM {GOLD}.fact_claims f
JOIN {GOLD}.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, f.payer
""")

# Mart 2: provider performance by network tier
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.mart_provider_tier AS
SELECT f.network_tier, f.provider_id,
       COUNT(*)                     AS claim_count,
       ROUND(SUM(f.claim_amount), 2) AS total_amount,
       ROUND(AVG(f.claim_amount), 2) AS avg_amount
FROM {GOLD}.fact_claims f
GROUP BY f.network_tier, f.provider_id
""")

# Mart 3: diagnosis cost summary
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.mart_diagnosis_cost AS
SELECT f.diagnosis_code,
       COUNT(*)                     AS claim_count,
       COUNT(DISTINCT f.patient_id)  AS patient_count,
       ROUND(SUM(f.claim_amount), 2) AS total_amount
FROM {GOLD}.fact_claims f
GROUP BY f.diagnosis_code
""")

print("Gold marts created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Optimize for BI queries (Z-ORDER on the join/filter columns)

# COMMAND ----------

spark.sql(f"OPTIMIZE {GOLD}.fact_claims ZORDER BY (payer, service_date)")
# NOTE: on Unity Catalog, Liquid Clustering is the modern replacement:
#   CREATE TABLE ... CLUSTER BY (payer, service_date);
# Z-ORDER is shown here because it works on any Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sample KPI query + Delta Time Travel

# COMMAND ----------

# KPI: denial rate trend by payer (the question a stakeholder actually asks)
spark.sql(f"""
SELECT payer, year, month,
       claim_count,
       denied_count,
       ROUND(100.0 * denied_count / claim_count, 2) AS denial_rate_pct,
       total_paid
FROM {GOLD}.mart_monthly_payer
ORDER BY year DESC, month DESC, denial_rate_pct DESC
LIMIT 20
""").show()

# Delta Time Travel — query the fact table as of an earlier version
detail = spark.sql(f"DESCRIBE HISTORY {GOLD}.fact_claims").select("version").orderBy("version").collect()
if len(detail) > 1:
    v0 = detail[0]["version"]
    n_then = spark.sql(f"SELECT COUNT(*) c FROM {GOLD}.fact_claims VERSION AS OF {v0}").collect()[0]["c"]
    print(f"fact_claims at version {v0}: {n_then:,} rows (time travel works)")
