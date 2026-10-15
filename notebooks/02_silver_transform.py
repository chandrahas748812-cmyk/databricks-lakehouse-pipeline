# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Transform (cleaned, conformed, quality-checked)
# MAGIC
# MAGIC **What this does:** Bronze → Silver. Deduplicates, enforces schema,
# MAGIC runs data-quality checks (reusable helpers in `src/quality.py`),
# MAGIC quarantines bad rows instead of dropping them silently, and maintains
# MAGIC two dimensions: `dim_patient` (**SCD Type 1** — overwrite) and
# MAGIC `dim_provider` (**SCD Type 2** — full history).
# MAGIC
# MAGIC Silver = clean, queryable, one row per business entity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters & shared helpers

# COMMAND ----------

dbutils.widgets.text("catalog", "healthcare_lakehouse", "Catalog")

CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.bronze.claims_bronze"
SILVER = f"{CATALOG}.silver"
QUARANTINE = f"{CATALOG}.silver.claims_quarantine"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

# reusable DQ helpers live in src/quality.py (Repos: ../src/quality)
%run ../src/quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Bronze, enforce schema, deduplicate
# MAGIC Keep the latest ingested copy per `claim_id` (re-deliveries happen).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze = spark.table(BRONZE)

silver = (
    bronze
    .withColumn("service_date", F.to_date("service_date"))
    .withColumn("claim_amount", F.col("claim_amount").cast("double"))
    .withColumn(
        "clean_rank",
        F.row_number().over(
            Window.partitionBy("claim_id").orderBy(F.col("_ingested_at").desc())
        ),
    )
    .filter("clean_rank = 1")
    .drop("clean_rank")
)

print(f"Bronze rows: {bronze.count():,}  ->  deduped: {silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data-quality checks — fail loudly, quarantine bad rows

# COMMAND ----------

# blocking checks: duplicates or null keys = stop the pipeline
run_checks(
    silver,
    [
        {"name": "claim_id unique",
         "fn": lambda d: check_unique(d, ["claim_id"])},
        {"name": "key columns not null",
         "fn": lambda d: check_not_null(d, ["claim_id", "patient_id", "provider_id", "service_date"])},
    ],
    fail_fast=True,
)

# non-blocking: negative amounts get quarantined for review, not dropped silently
valid = (
    F.col("claim_amount").isNotNull()
    & (F.col("claim_amount") >= 0)
    & F.col("service_date").isNotNull()
)
clean = quarantine_invalid(silver, valid, QUARANTINE)
print(f"Clean silver rows: {clean.count():,}")

(
    clean.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.claims_silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_patient — SCD Type 1 (overwrite with latest)
# MAGIC Patient attributes are corrected in place; we only need the current view.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.dim_patient (
  patient_id STRING,
  first_seen_date DATE,
  last_seen_date DATE,
  claim_count INT
) USING DELTA
""")

patient_updates = (
    clean.groupBy("patient_id")
    .agg(
        F.min("service_date").alias("first_seen_date"),
        F.max("service_date").alias("last_seen_date"),
        F.count("*").alias("claim_count"),
    )
)

patient_updates.createOrReplaceTempView("patient_updates")

spark.sql(f"""
MERGE INTO {SILVER}.dim_patient t
USING patient_updates s
ON t.patient_id = s.patient_id
WHEN MATCHED THEN UPDATE SET
  t.last_seen_date = s.last_seen_date,
  t.claim_count = s.claim_count
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_provider — SCD Type 2 (full history)
# MAGIC Provider "network tier" changes over time and history matters for
# MAGIC point-in-time reporting, so we version rows with
# MAGIC `effective_date` / `end_date` / `is_current`.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER}.dim_provider (
  provider_key BIGINT GENERATED ALWAYS AS IDENTITY,
  provider_id STRING,
  network_tier STRING,
  effective_date DATE,
  end_date DATE,
  is_current BOOLEAN
) USING DELTA
""")

# derive a slowly-changing attribute from the data (demo logic)
provider_snapshot = (
    clean.groupBy("provider_id")
    .agg(
        F.avg("claim_amount").alias("avg_claim"),
        F.max("service_date").alias("as_of"),
    )
    .withColumn(
        "network_tier",
        F.when(F.col("avg_claim") < 1000, "tier_1")
        .when(F.col("avg_claim") < 4000, "tier_2")
        .otherwise("tier_3"),
    )
    .select("provider_id", "network_tier", F.col("as_of").alias("effective_date"))
)
provider_snapshot.createOrReplaceTempView("provider_snapshot")

# 4a. expire rows whose tier changed
spark.sql(f"""
MERGE INTO {SILVER}.dim_provider t
USING provider_snapshot s
ON t.provider_id = s.provider_id AND t.is_current = true
WHEN MATCHED AND t.network_tier <> s.network_tier THEN UPDATE SET
  t.end_date = s.effective_date,
  t.is_current = false
""")

# 4b. insert new versions (new providers, or changed tiers)
spark.sql(f"""
MERGE INTO {SILVER}.dim_provider t
USING provider_snapshot s
ON t.provider_id = s.provider_id AND t.is_current = true
  AND t.network_tier = s.network_tier
WHEN NOT MATCHED THEN INSERT
  (provider_id, network_tier, effective_date, end_date, is_current)
VALUES
  (s.provider_id, s.network_tier, s.effective_date, NULL, true)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify

# COMMAND ----------

for tbl in ["claims_silver", "dim_patient", "dim_provider", "claims_quarantine"]:
    n = spark.sql(f"SELECT COUNT(*) c FROM {SILVER}.{tbl}").collect()[0]["c"]
    print(f"{tbl}: {n:,} rows")

# SCD2 history sample
spark.sql(f"""
SELECT provider_id, network_tier, effective_date, end_date, is_current
FROM {SILVER}.dim_provider
ORDER BY provider_id, effective_date
LIMIT 10
""").show()
