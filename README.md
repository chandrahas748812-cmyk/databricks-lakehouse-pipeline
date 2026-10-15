# Databricks Lakehouse Pipeline — Medallion ETL on Delta Lake

A portfolio project I built to learn Databricks properly: a production-style
**medallion-architecture** ETL pipeline (Bronze → Silver → Gold) over synthetic
healthcare-claims data, governed by **Unity Catalog**. Every notebook is real,
runnable PySpark/SQL — no pseudocode.

> **Honest framing:** this is a learning/portfolio project, not work I did at an
> employer. I built it to get hands-on with the Databricks pieces that show up
> in every Data Engineer interview: Auto Loader, Delta Lake, medallion pattern,
> SCD handling, and Unity Catalog governance.

## Architecture

```
┌─────────────┐      ┌──────────────────┐      ┌─────────────────────────┐
│ Raw claims  │      │  01_bronze_ingest │      │ BRONZE (append-only)    │
│ JSON drops  ├─────►│  Auto Loader     ├─────►│ claims_bronze (Delta)   │
│ (4 batches) │      │  cloudFiles      │      │ + _ingested_at,         │
└─────────────┘      │  schema rescue   │      │   _source_file          │
                     └──────────────────┘      └────────────┬────────────┘
                                                            │
                     ┌──────────────────┐                   ▼
                     │ 02_silver_       │      ┌─────────────────────────┐
                     │ transform        ├─────►│ SILVER (clean)          │
                     │ • dedup          │      │ claims_silver           │
                     │ • DQ checks      │      │ dim_patient (SCD T1)    │
                     │ • quarantine     │      │ dim_provider (SCD T2)   │
                     │ • SCD T1 / T2    │      │ claims_quarantine       │
                     └──────────────────┘      └────────────┬────────────┘
                                                            │
                     ┌──────────────────┐                   ▼
                     │ 03_gold_         │      ┌─────────────────────────┐
                     │ aggregate        ├─────►│ GOLD (BI-ready)         │
                     │ • star schema    │      │ fact_claims             │
                     │ • 3 marts        │      │ dim_date                │
                     │ • OPTIMIZE/      │      │ mart_monthly_payer      │
                     │   Z-ORDER        │      │ mart_provider_tier      │
                     └──────────────────┘      │ mart_diagnosis_cost     │
                                               └─────────────────────────┘

  04_unity_catalog.sql wraps everything: catalog → schemas → volume,
  least-privilege GRANTs, row filter, column mask.
```

## What's inside

| Path | Description |
|---|---|
| `notebooks/01_bronze_ingest.py` | Generates 50k synthetic claims, lands JSON, ingests with Auto Loader (`cloudFiles`) into a Bronze Delta table. Schema inference + rescue mode. |
| `notebooks/02_silver_transform.py` | Dedup (windowed), schema enforcement, blocking DQ checks, quarantine pattern for bad rows, `dim_patient` **SCD Type 1** and `dim_provider` **SCD Type 2** via Delta `MERGE`. |
| `notebooks/03_gold_aggregate.py` | Star schema (`fact_claims` with as-of SCD2 join + `dim_date`), 3 Gold marts, `OPTIMIZE … ZORDER`, a KPI query, and a Delta **Time Travel** demo. |
| `notebooks/04_unity_catalog.sql` | Catalog/schema/volume creation, least-privilege `GRANT`s, **row filter** and **column mask** examples. |
| `src/quality.py` | Reusable PySpark DQ helpers (`check_not_null`, `check_unique`, `check_range`, `quarantine_invalid`, `run_checks`). Pure PySpark — unit-testable locally. |

## Delta Lake features used

- **Time Travel** (`VERSION AS OF`) — audit/reproduce any historical state
- **MERGE** — SCD Type 1 & 2 upserts without full rewrites
- **Schema evolution** (`mergeSchema`, Auto Loader `rescue` mode) — new columns don't break the stream
- **OPTIMIZE + Z-ORDER** — file compaction and data skipping for BI queries (Liquid Clustering noted as the UC-native successor)
- **Checkpoints & schema locations** — exactly-once streaming semantics for Auto Loader

## Medallion, in one paragraph

**Bronze** is raw and immutable — what the source gave us, plus lineage columns.
**Silver** is clean and conformed — deduped, typed, quality-checked, one row per
entity, with history where it matters (SCD2). **Gold** is business-ready —
aggregated, denormalized for specific questions. Each layer is a Delta table, so
every layer gets ACID, time travel, and auditability for free.

## How to run it (Databricks workspace)

**Prerequisites:** a Databricks workspace with Unity Catalog, DBR 14+ (Photon
optional), and permission to create a catalog (or use an existing one and adjust
the `catalog` widget).

1. **Import the repo:** in Databricks go to *Workspace → Import from Git* and
   paste this repo's URL (or use Repos: *Add → Clone*). The `notebooks/` folder
   imports as native Databricks notebooks.
2. **Run `notebooks/04_unity_catalog.sql`** top-to-bottom once (needs metastore
   admin for catalog creation; adjust group names in the GRANTs).
3. **Run notebooks 01 → 02 → 03 in order**, accepting the default widget values.
   Each notebook is idempotent — re-running is safe.
4. Query Gold: `SELECT * FROM healthcare_lakehouse.gold.mart_monthly_payer`.

**Without a workspace?** `src/quality.py` is pure PySpark — `pip install pyspark`
and run its functions against local DataFrames. The notebook logic (dedup,
SCD2 MERGE, quarantine) ports 1:1 to any Spark 3.x + Delta environment.

## What I learned (Chandrahas)

- Auto Loader's `cloudFiles` is the difference between a demo and a pipeline:
  incremental file discovery with a schema location means new files and new
  columns don't page anyone.
- SCD Type 2 with Delta `MERGE` is two statements (expire + insert), not a
  framework — the hard part is deciding *which* attributes are slowly changing.
- Data quality belongs *in* the pipeline, not beside it: blocking checks raise,
  non-blocking checks quarantine. Silent drops are how you lose trust.
- Unity Catalog governance (row filters, column masks) is just SQL — but
  designing *who sees what* is the actual work.
- `OPTIMIZE`/`ZORDER` on the fact table cut the KPI query's files scanned
  dramatically; partitioning would have been the wrong call at this data size.

## Tech

PySpark · Delta Lake · Databricks Auto Loader · Unity Catalog · Spark SQL
