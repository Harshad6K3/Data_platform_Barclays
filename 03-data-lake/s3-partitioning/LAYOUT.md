# 03 — Data Lake: S3 Layout & Partitioning Strategy

## Zone Architecture

```
s3://dp-datalake-{env}/
├── raw/                         # Immutable. Exact copy of source events.
│   ├── trading/
│   │   └── trade_events/
│   │       └── year=2024/month=01/day=15/hour=09/
│   │           └── trade_20240115_090000_part0.parquet
│   └── customers/
│       └── customer_events/
│           └── year=2024/month=01/day=15/
├── curated/                     # Cleansed, deduplicated, enriched.
│   ├── trading/
│   │   ├── trades/              # Iceberg table
│   │   └── positions/           # Iceberg table
│   └── customers/
│       └── profiles/            # Iceberg table
├── consumption/                 # Aggregated, business-ready datasets.
│   ├── risk/
│   └── reporting/
└── _metadata/
    ├── glue-catalog/
    └── lineage/
```

## Partitioning Strategy

| Zone     | Table              | Partition Keys              | Rationale                          |
|----------|--------------------|-----------------------------|------------------------------------|
| raw      | trade_events       | year/month/day/hour         | Time-based pruning for replay      |
| curated  | trades             | trade_date/asset_class      | Query pattern: date + asset class  |
| curated  | positions          | position_date/book_id       | Risk queries by book               |
| consumption | daily_pnl       | report_date/region          | BI queries by date/region          |

## Iceberg Table DDL (Glue / Athena)

```sql
CREATE TABLE dp_curated.trades (
    trade_id        STRING,
    instrument_id   STRING,
    asset_class     STRING,
    notional        DECIMAL(20,4),
    currency        STRING,
    trade_date      DATE,
    trader_id       STRING,
    book_id         STRING,
    counterparty_lei STRING,
    status          STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
)
PARTITIONED BY (trade_date, asset_class)
LOCATION 's3://dp-datalake-prod/curated/trading/trades/'
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format'='parquet',
    'write_compression'='snappy',
    'compaction.enabled'='true'
);
```

## File Size & Compaction Rules

- Target file size: **128 MB** (Parquet)
- Compaction job: Runs **nightly** via Spark (see `05-data-pipeline/spark-jobs/`)
- Snapshot expiry: **30 days** (regulatory minimum = 7 years in archive tier)
- S3 lifecycle: raw → Standard-IA after 90 days → Glacier after 1 year
