-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 — Unity Catalog Setup (governance as code)
-- MAGIC
-- MAGIC **What this does:** creates the catalog / schemas / volume used by
-- MAGIC notebooks 01–03, grants least-privilege access, and demonstrates
-- MAGIC **row-level security** (row filter) and **column masking**.
-- MAGIC
-- MAGIC Run top-to-bottom once per environment. Catalog creation needs an
-- MAGIC **account admin or metastore admin**; everything below that can be run
-- MAGIC by a catalog owner.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Catalog, schemas, volume

-- COMMAND ----------

-- run as: account admin / metastore admin
CREATE CATALOG IF NOT EXISTS healthcare_lakehouse
  COMMENT 'Portfolio lakehouse: medallion claims pipeline';

-- run as: catalog owner (or anyone with USE CATALOG + CREATE SCHEMA)
CREATE SCHEMA IF NOT EXISTS healthcare_lakehouse.bronze
  COMMENT 'Raw, append-only landing zone (Auto Loader -> Delta)';
CREATE SCHEMA IF NOT EXISTS healthcare_lakehouse.silver
  COMMENT 'Cleaned, conformed entities + quarantine';
CREATE SCHEMA IF NOT EXISTS healthcare_lakehouse.gold
  COMMENT 'BI-ready star schema and aggregate marts';

-- landing zone for raw files (Auto Loader source)
CREATE VOLUME IF NOT EXISTS healthcare_lakehouse.bronze.landing
  COMMENT 'Raw claims JSON drops';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Least-privilege grants
-- MAGIC Replace `data-analysts` with your real group name.

-- COMMAND ----------

-- analysts read Gold only — they never see raw Bronze
GRANT USE CATALOG ON CATALOG healthcare_lakehouse TO `data-analysts`;
GRANT USE SCHEMA ON SCHEMA healthcare_lakehouse.gold TO `data-analysts`;
GRANT SELECT ON SCHEMA healthcare_lakehouse.gold TO `data-analysts`;

-- pipeline service principal owns the write path
GRANT USE SCHEMA ON SCHEMA healthcare_lakehouse.bronze TO `pipeline-sp`;
GRANT USE SCHEMA ON SCHEMA healthcare_lakehouse.silver TO `pipeline-sp`;
GRANT CREATE TABLE, MODIFY ON SCHEMA healthcare_lakehouse.bronze TO `pipeline-sp`;
GRANT CREATE TABLE, MODIFY ON SCHEMA healthcare_lakehouse.silver TO `pipeline-sp`;
GRANT READ VOLUME ON VOLUME healthcare_lakehouse.bronze.landing TO `pipeline-sp`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Row-level security: analysts only see non-denied claims
-- MAGIC Row filters are SQL UDFs attached to the table; they apply to every
-- MAGIC query, including BI tools.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION healthcare_lakehouse.gold.claim_visibility_filter(status STRING)
RETURNS BOOLEAN
RETURN
  -- members of data-stewards see everything; everyone else skips denied rows
  is_member('data-stewards') OR status <> 'denied';

ALTER TABLE healthcare_lakehouse.gold.fact_claims
  SET ROW FILTER healthcare_lakehouse.gold.claim_visibility_filter
  ON (claim_status);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Column masking: hide raw claim amounts from non-privileged roles
-- MAGIC Masks apply at read time — the stored data is untouched.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION healthcare_lakehouse.gold.mask_claim_amount(amount DOUBLE)
RETURNS DOUBLE
RETURN CASE
  WHEN is_member('data-stewards') THEN amount
  ELSE ROUND(amount, -3)  -- analysts see amounts rounded to the nearest thousand
END;

ALTER TABLE healthcare_lakehouse.gold.fact_claims
  ALTER COLUMN claim_amount
  SET MASK healthcare_lakehouse.gold.mask_claim_amount;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Verify governance

-- COMMAND ----------

SHOW GRANTS ON SCHEMA healthcare_lakehouse.gold;
SELECT table_name, row_filter, column_masks
FROM system.information_schema.tables
WHERE table_catalog = 'healthcare_lakehouse'
  AND table_schema = 'gold';
