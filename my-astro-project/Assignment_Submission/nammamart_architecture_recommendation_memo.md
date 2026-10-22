
# PART G - One page MEMORANDUM

**To:** Sameer, CTO  
**From:** Ankit Rawat  
**Date:** October 22, 2026  
**Subject:** Strategic Architectural Recommendation: Why ELT is the Right Choice for NammaMart  

---

## Executive Summary

After successfully building, debugging, and operating both ETL and ELT pipelines for NammaMart using DuckDB, my empirical recommendation is to standardize on an **ELT (Extract, Load, Transform)** architecture. While traditional ETL processes data in Python worker memory prior to warehouse loading, ELT loads raw files immutably into the warehouse first (`raw_orders`, `raw_customers`, `raw_payments`) and defers transformation, masking, and derivations to in-database SQL execution.

Based on our implementation challenges—specifically schema drift (`delivery_ts` vs. `delivery_minutes`, `name` vs. `customer_name`), quarantine handling, and performance—ELT provides superior resilience, scalability, and auditability.

---

## Key Evaluation Pillars (Based on Implementation Experience)

### 1. Auditability
* **ELT Advantage:** By immediately landing raw CSV files into explicit staging tables (`raw_orders`, `raw_customers`), we preserve an immutable, exact copy of what NammaMart extracted from source systems.
* **Observation:** When upstream schema discrepancies or missing columns occurred, debugging was vastly simplified because the raw source data was queryable directly in DuckDB without re-running time-consuming extraction scripts. Pipeline runs and row counts are systematically tracked in the `pipeline_runs` audit table.

### 2. Data Quality Guarantees & Quarantine Management
* **ELT Advantage:** Data quality rules and quarantine mechanisms operate directly inside the database engine. Critical failures (such as `NULL` order IDs, negative quantities, or invalid discount percentages) are filtered and exported using high-performance SQL queries rather than slow row-by-row Python loops.
* **Observation:** DuckDB allowed us to run SQL-based data quality checks against staged data and isolate malformed rows into quarantine tables instantly while maintaining pipeline continuity.

### 3. Ease of Change
* **ELT Advantage:** Upstream source systems frequently introduce schema changes (e.g., substituting `delivery_ts` with `delivery_minutes` or renaming `customer_name` to `name`). In an ETL model, this breaks Python dataframe memory models instantly. In ELT, changes are absorbed at the landing layer, and downstream SQL Common Table Expressions (CTEs) can dynamically alias or cast columns without altering core ingestion code.
* **Observation:** Handling column mismatches dynamically in our staging layer proved that ELT isolates transformation logic from ingestion brittle points.

### 4. Performance as Data Grows 1000×
* **ELT Advantage:** Python Pandas struggles with memory overhead as transaction volumes scale ($O(N)$ RAM usage). ELT leverages DuckDB’s vectorized, columnar execution engine, pushing computation down to disk and utilizing parallelized query plans.
* **Observation:** In-database joins across orders, customers, products, and stores completed instantly without serialization bottlenecks or Airflow worker OOM (Out Of Memory) crashes.

### 5. Cost
* **ELT Advantage:** ELT minimizes expensive application-tier compute resources. Instead of provisioning high-RAM Airflow worker nodes to process heavy transformations in Python, compute is handled efficiently inside the database storage layer, scaling gracefully as NammaMart transitions from embedded DuckDB to cloud data warehouses (e.g., Snowflake or BigQuery).

---

## Top 3 Risks of the Recommended ELT Design

1. **Storage Bloat from Raw Landing Layers:** Retaining full, uncleaned CSV dumps in `raw_*` tables consumes disk space.  
   * **Mitigation:** Implement retention policies and partition raw tables by ingestion date.
2. **Upstream Schema Drift Breaking Downstream Marts:** While ELT absorbs raw data easily, sudden breaking changes in source column types (e.g., changing string types to objects or shifting date formats) can cause SQL casting exceptions in `stg_orders`.  
   * **Mitigation:** Maintain strict schema-on-read defensive casting using `TRY_CAST` and robust fallback expressions.
3. **DuckDB Concurrency and File Locking:** Embedded DuckDB files can encounter file-lock contention if multiple DAG tasks attempt concurrent write transactions.  
   * **Mitigation:** Enforce strict sequential dependency flows (`max_active_runs=1`) and use context-managed connection scopes (`with duckdb.connect(...) as conn:`), which our pipeline successfully implements.