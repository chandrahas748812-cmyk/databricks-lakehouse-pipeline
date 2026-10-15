# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingest (Auto Loader → Delta)
# MAGIC
# MAGIC **What this does:** lands raw healthcare-claims JSON with Databricks
# MAGIC **Auto Loader** (`cloudFiles`) and writes an append-only **Bronze Delta**
# MAGIC table. Bronze = raw, immutable, full history. Schema is inferred on
# MAGIC read and tracked in a schema location so new columns don't break the stream.
# MAGIC
# MAGIC **Prereqs:** a Unity Catalog volume to land files in (see
# MAGIC `04_unity_catalog.sql`), or replace the volume path with a DBFS path.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "healthcare_lakehouse", "Catalog")
dbutils.widgets.text("schema", "bronze", "Schema")
dbutils.widgets.text("landing_path", "/Volumes/healthcare_lakehouse/bronze/landing/claims", "Landing path")
dbutils.widgets.text("schema_path", "/Volumes/healthcare_lakehouse/bronze/_schemas/claims", "Schema location")
dbutils.widgets.text("checkpoint_path", "/Volumes/healthcare_lakehouse/bronze/_checkpoints/claims", "Checkpoint location")
dbutils.widgets.text("num_rows", "50000", "Synthetic rows to generate")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LANDING = dbutils.widgets.get("landing_path")
SCHEMA_LOC = dbutils.widgets.get("schema_path")
CHECKPOINT = dbutils.widgets.get("checkpoint_path")
N = int(dbutils.widgets.get("num_rows"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate synthetic raw claims (stand-in for the source system)
# MAGIC In production this step doesn't exist — files arrive from the payer /
# MAGIC provider feed. Here we generate them so the pipeline is end-to-end runnable.

# COMMAND ----------

import json
import random
from datetime import date, timedelta

random.seed(42)

DIAG = ["J06.9", "I10", "E11.65", "M54.5", "R51", "Z00.00", "F32.9", "G43.109"]
PROC = ["99213", "99214", "93000", "80053", "97110", "90834", "G0439"]
PAYER = ["Aetna", "BCBS", "Cigna", "UnitedHealth", "Humana", "Medicare"]
STATUS = ["submitted", "submitted", "submitted", "paid", "denied", "pending"]

def rand_claim(i: int) -> dict:
    svc = date(2024, 1, 1) + timedelta(days=random.randint(0, 560))
    return {
        "claim_id": f"CLM-{i:08d}",
        "patient_id": f"PAT-{random.randint(1, 8000):06d}",
        "provider_id": f"PRV-{random.randint(1, 400):04d}",
        "service_date": svc.isoformat(),
        "diagnosis_code": random.choice(DIAG),
        "procedure_code": random.choice(PROC),
        # sprinkle in a few bad rows on purpose — Silver quarantines them
        "claim_amount": round(random.uniform(25, 9500), 2) if random.random() > 0.01 else -10.0,
        "claim_status": random.choice(STATUS),
        "payer": random.choice(PAYER),
    }

# write as multi-line JSON files, a few "batches" like real drops
dbutils.fs.mkdirs(LANDING)
for batch in range(4):
    rows = [rand_claim(batch * (N // 4) + i) for i in range(N // 4)]
    path = f"{LANDING}/batch_{batch:02d}.json"
    # write via driver-local temp then cp — keeps it simple and portable
    tmp = f"/tmp/claims_batch_{batch:02d}.json"
    with open(tmp, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    dbutils.fs.cp(f"file:{tmp}", path)

print(f"Landed {N} synthetic claim records in 4 files -> {LANDING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Auto Loader stream → Bronze Delta
# MAGIC `cloudFiles` incrementally discovers new files (no LIST storms), infers
# MAGIC schema once and tracks evolution in `schemaLocation`. `availableNow=True`
# MAGIC processes everything currently landed, then stops — ideal for batch-style
# MAGIC demos and scheduled Jobs.

# COMMAND ----------

from pyspark.sql import functions as F

bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.inferColumnTypes", "true")
    # rescue malformed rows instead of failing the stream
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(LANDING)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

query = (
    bronze.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.{SCHEMA}.claims_bronze")
)

query.awaitTermination()
print("Bronze ingest complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS bronze_rows FROM {CATALOG}.{SCHEMA}.claims_bronze").show()
spark.sql(f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.claims_bronze").select(
    "numFiles", "sizeInBytes", "lastModified"
).show(truncate=False)
