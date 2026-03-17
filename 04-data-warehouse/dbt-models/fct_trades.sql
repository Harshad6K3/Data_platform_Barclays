-- models/marts/trading/fct_trades.sql
-- ============================================================
-- Fact table: fct_trades
-- Grain: one row per trade lifecycle event
-- Sources: stg_trades (curated S3 via Redshift Spectrum)
-- Materialization: incremental (merge on trade_id + event_type)
-- SLA: available by 07:00 UTC for daily risk reporting
-- ============================================================

{{
  config(
    materialized   = 'incremental',
    unique_key     = ['trade_id', 'event_type'],
    incremental_strategy = 'merge',
    dist           = 'trade_id',
    sort           = ['trade_date', 'asset_class'],
    tags           = ['trading', 'fact', 'tier-1'],
    meta           = {
      'owner': 'data-platform-team',
      'pii': false,
      'sla_utc': '07:00'
    }
  )
}}

with source as (

    select * from {{ ref('stg_trades') }}

    {% if is_incremental() %}
    -- Only process new/updated records since last run
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

enriched as (

    select
        t.trade_id,
        t.event_type,
        t.instrument_id,
        i.instrument_name,
        i.asset_class,
        i.sector,
        t.notional,
        t.currency,
        fx.usd_rate,
        t.notional * fx.usd_rate            as notional_usd,
        t.trade_date,
        t.settlement_date,
        t.trader_id,
        tr.trader_name,
        tr.desk,
        t.book_id,
        b.book_name,
        b.region,
        t.counterparty_lei,
        cp.counterparty_name,
        cp.credit_rating,
        t.status,
        t.created_at,
        t.updated_at,
        -- Audit columns
        current_timestamp                   as dbt_loaded_at,
        '{{ invocation_id }}'               as dbt_invocation_id

    from source t

    left join {{ ref('dim_instruments') }}   i  on t.instrument_id    = i.instrument_id
    left join {{ ref('dim_fx_rates') }}      fx on t.currency          = fx.currency
                                               and t.trade_date        = fx.rate_date
    left join {{ ref('dim_traders') }}       tr on t.trader_id         = tr.trader_id
    left join {{ ref('dim_books') }}         b  on t.book_id           = b.book_id
    left join {{ ref('dim_counterparties') }} cp on t.counterparty_lei = cp.lei_code

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['trade_id', 'event_type']) }} as trade_sk,
        *
    from enriched

)

select * from final
