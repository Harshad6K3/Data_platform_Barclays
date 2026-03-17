# 01 — Architecture

## System Overview (C4 Level 1 — Context)

```
[Trade Systems]──events──▶ [Kafka Cluster] ──▶ [Flink Stream Processor]
                                                        │
                                              ┌─────────┴──────────┐
                                              ▼                    ▼
                                     [S3 Data Lake]       [Redshift DWH]
                                              │                    │
                                     [Glue Catalog]         [dbt Models]
                                              │                    │
                                        [Spark Jobs]        [Data APIs]
                                                    └────┬───┘
                                                         ▼
                                                  [ML Feature Store]
                                                  [BI / Analytics]
```

## Architecture Decision Records (ADRs)

See `decisions/` folder.

- ADR-001: Why Kafka over Kinesis
- ADR-002: S3 + Iceberg over Delta Lake
- ADR-003: Redshift Serverless vs Provisioned
- ADR-004: Flink on EKS vs Managed Flink
- ADR-005: dbt Core vs dbt Cloud
