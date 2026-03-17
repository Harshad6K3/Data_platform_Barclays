# ADR-002: Data Lake Table Format — Apache Iceberg on S3

**Date:** 2024-01  
**Status:** Accepted

---

## Context

Raw and curated data in S3 needs ACID transactions, schema evolution, and time-travel
for regulatory replay. Candidates: Apache Iceberg, Delta Lake, Apache Hudi.

## Decision

**Apache Iceberg on S3 with AWS Glue as metastore.**

## Rationale

- **Engine-agnostic:** Works with Spark, Flink, Athena, Redshift Spectrum natively
- **Time travel:** `SELECT * FROM table FOR SYSTEM_TIME AS OF '2024-01-01'` — critical for audit
- **Schema evolution:** Add/drop/rename columns without rewrite
- **Partition evolution:** Change partitioning strategy without full table rewrite
- **AWS native support:** Athena, Glue, EMR all support Iceberg; no extra tooling

Delta Lake was ruled out due to vendor proximity to Databricks.
Hudi's complexity for our team size was deemed unnecessary.

## Consequences

- Glue catalog becomes the single metastore — all engines point here
- Iceberg compaction jobs needed to manage small file problem
- `iceberg.expire_snapshots` scheduled weekly to control S3 costs
