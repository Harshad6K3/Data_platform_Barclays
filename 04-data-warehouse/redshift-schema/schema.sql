-- ============================================================
-- Redshift Schema Design — Trading Data Warehouse
-- Strategy: Star schema. DISTKEY on join keys. SORTKEY on query filters.
-- ============================================================

-- ── Dimension: Instruments ────────────────────────────────────────────────
CREATE TABLE dim_instruments (
    instrument_sk       BIGINT IDENTITY(1,1),
    instrument_id       VARCHAR(50)     NOT NULL,
    instrument_name     VARCHAR(200)    NOT NULL,
    isin                VARCHAR(12),
    asset_class         VARCHAR(50)     NOT NULL,  -- EQUITY, FX, RATES, CREDIT
    sector              VARCHAR(100),
    issuer              VARCHAR(200),
    currency            VARCHAR(3)      NOT NULL,
    is_active           BOOLEAN         DEFAULT TRUE,
    valid_from          TIMESTAMP       NOT NULL,
    valid_to            TIMESTAMP,
    dbt_loaded_at       TIMESTAMP       DEFAULT GETDATE(),
    PRIMARY KEY (instrument_sk)
)
DISTSTYLE ALL   -- Replicated: small dimension, joined frequently
SORTKEY (asset_class, instrument_id);


-- ── Dimension: Traders ────────────────────────────────────────────────────
CREATE TABLE dim_traders (
    trader_sk           BIGINT IDENTITY(1,1),
    trader_id           VARCHAR(50)     NOT NULL,
    trader_name         VARCHAR(200)    NOT NULL,
    desk                VARCHAR(100),
    region              VARCHAR(50),
    cost_centre         VARCHAR(50),
    is_active           BOOLEAN         DEFAULT TRUE,
    valid_from          TIMESTAMP       NOT NULL,
    valid_to            TIMESTAMP,
    PRIMARY KEY (trader_sk)
)
DISTSTYLE ALL
SORTKEY (desk, trader_id);


-- ── Dimension: Books ──────────────────────────────────────────────────────
CREATE TABLE dim_books (
    book_sk             BIGINT IDENTITY(1,1),
    book_id             VARCHAR(50)     NOT NULL,
    book_name           VARCHAR(200),
    region              VARCHAR(50),
    legal_entity        VARCHAR(100),
    PRIMARY KEY (book_sk)
)
DISTSTYLE ALL
SORTKEY (book_id);


-- ── Dimension: Counterparties ─────────────────────────────────────────────
CREATE TABLE dim_counterparties (
    counterparty_sk     BIGINT IDENTITY(1,1),
    lei_code            VARCHAR(20)     NOT NULL,
    counterparty_name   VARCHAR(200),
    credit_rating       VARCHAR(10),
    country             VARCHAR(50),
    PRIMARY KEY (counterparty_sk)
)
DISTSTYLE ALL
SORTKEY (lei_code);


-- ── Fact: Trades ─────────────────────────────────────────────────────────
CREATE TABLE fct_trades (
    trade_sk            BIGINT          NOT NULL,
    trade_id            VARCHAR(100)    NOT NULL,
    event_type          VARCHAR(50)     NOT NULL,  -- NEW, AMEND, CANCEL, SETTLE
    instrument_id       VARCHAR(50),
    instrument_name     VARCHAR(200),
    asset_class         VARCHAR(50),
    notional            DECIMAL(20,4),
    currency            VARCHAR(3),
    notional_usd        DECIMAL(20,4),
    trade_date          DATE            NOT NULL,
    settlement_date     DATE,
    trader_id           VARCHAR(50),
    desk                VARCHAR(100),
    book_id             VARCHAR(50),
    region              VARCHAR(50),
    counterparty_lei    VARCHAR(20),
    counterparty_name   VARCHAR(200),
    credit_rating       VARCHAR(10),
    status              VARCHAR(50),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    dbt_loaded_at       TIMESTAMP       DEFAULT GETDATE(),
    dbt_invocation_id   VARCHAR(100),
    PRIMARY KEY (trade_sk)
)
DISTKEY (trade_id)          -- Collocate with position/risk tables
COMPOUND SORTKEY (trade_date, asset_class, desk);  -- Primary query pattern


-- ── Aggregate: Daily PnL by Book ─────────────────────────────────────────
CREATE TABLE agg_daily_pnl_by_book (
    report_date         DATE            NOT NULL,
    book_id             VARCHAR(50)     NOT NULL,
    book_name           VARCHAR(200),
    region              VARCHAR(50),
    asset_class         VARCHAR(50),
    total_notional_usd  DECIMAL(20,4),
    trade_count         INT,
    realized_pnl_usd    DECIMAL(20,4),
    dbt_loaded_at       TIMESTAMP       DEFAULT GETDATE(),
    PRIMARY KEY (report_date, book_id, asset_class)
)
DISTKEY (book_id)
COMPOUND SORTKEY (report_date, region, asset_class);
