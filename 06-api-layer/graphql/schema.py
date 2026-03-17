"""
GraphQL API — Data Platform
============================

Exposes trade, instrument, and analytics data via GraphQL.
Complements the REST API for consumers needing flexible field selection
(e.g., ML feature pipelines, self-service analytics).

Stack:  Strawberry (Python) + FastAPI
Auth:   JWT via middleware (same as REST API)
Depth:  Max query depth 5 (prevents expensive nested queries)
Rate:   100 req/min per client_id (enforced at API Gateway level)

Example queries:

  # Fetch trades for a date with specific fields
  query {
    trades(fromDate: "2024-01-15", toDate: "2024-01-15", assetClass: "EQUITY") {
      tradeId instrumentName notional currency desk counterpartyName
    }
  }

  # Instrument with its recent trade summary
  query {
    instrument(instrumentId: "AAPL") {
      instrumentId instrumentName assetClass
      recentTrades(days: 7) {
        tradeId notionalUsd tradeDate status
      }
    }
  }
"""

import strawberry
from strawberry.fastapi import GraphQLRouter
from strawberry.types import Info
from typing import Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)


# ── Types ─────────────────────────────────────────────────────────────────────

@strawberry.type
class TradeType:
    trade_id:           str
    instrument_id:      str
    instrument_name:    Optional[str]
    asset_class:        Optional[str]
    notional:           Optional[float]
    notional_usd:       Optional[float]
    currency:           Optional[str]
    trade_date:         Optional[date]
    settlement_date:    Optional[date]
    trader_id:          Optional[str]
    desk:               Optional[str]
    book_id:            Optional[str]
    counterparty_name:  Optional[str]
    credit_rating:      Optional[str]
    status:             Optional[str]


@strawberry.type
class InstrumentType:
    instrument_id:   str
    instrument_name: str
    asset_class:     str
    sector:          Optional[str]
    currency:        str
    is_active:       bool

    @strawberry.field
    def recent_trades(
        self,
        info:      Info,
        days:      int = 7,
        limit:     int = 100,
    ) -> list[TradeType]:
        """Fetch recent trades for this instrument (nested resolver)."""
        context = info.context["security_context"]
        repo    = info.context["trade_repository"]
        return repo.find_by_instrument(
            instrument_id = self.instrument_id,
            days          = days,
            limit         = min(limit, 500),
            security_ctx  = context,
        )


@strawberry.type
class DailyPnlType:
    report_date:       date
    book_id:           str
    book_name:         Optional[str]
    region:            Optional[str]
    asset_class:       Optional[str]
    total_notional_usd: Optional[float]
    trade_count:       Optional[int]
    realized_pnl_usd:  Optional[float]


@strawberry.type
class PageInfo:
    total_count:   int
    has_next_page: bool
    next_cursor:   Optional[str]


@strawberry.type
class TradeConnection:
    trades:    list[TradeType]
    page_info: PageInfo


# ── Query Root ────────────────────────────────────────────────────────────────

@strawberry.type
class Query:

    @strawberry.field
    def trades(
        self,
        info:        Info,
        from_date:   date,
        to_date:     date,
        asset_class: Optional[str]  = None,
        desk:        Optional[str]  = None,
        book_id:     Optional[str]  = None,
        status:      Optional[str]  = None,
        limit:       int            = 100,
        cursor:      Optional[str]  = None,
    ) -> TradeConnection:
        """
        Paginated trade search. Max limit 1000.
        Requires: ROLE_TRADE_READ
        """
        ctx  = info.context["security_context"]
        repo = info.context["trade_repository"]
        audit = info.context["audit_logger"]

        _assert_role(ctx, "ROLE_TRADE_READ")
        audit.log(ctx, "GQL_TRADE_SEARCH", "trades",
                  {"from_date": str(from_date), "asset_class": asset_class})

        safe_limit = min(limit, 1000)
        results = repo.search(
            from_date=from_date, to_date=to_date,
            asset_class=asset_class, desk=desk,
            book_id=book_id, status=status,
            limit=safe_limit + 1, cursor=cursor,
            security_ctx=ctx,
        )

        has_next = len(results) > safe_limit
        trades   = results[:safe_limit]

        return TradeConnection(
            trades    = [_map_trade(t) for t in trades],
            page_info = PageInfo(
                total_count   = len(trades),
                has_next_page = has_next,
                next_cursor   = trades[-1].trade_id if has_next else None,
            )
        )

    @strawberry.field
    def trade(self, info: Info, trade_id: str) -> Optional[TradeType]:
        """Fetch a single trade by ID. Requires: ROLE_TRADE_READ"""
        ctx  = info.context["security_context"]
        repo = info.context["trade_repository"]
        _assert_role(ctx, "ROLE_TRADE_READ")
        result = repo.find_by_id(trade_id, security_ctx=ctx)
        return _map_trade(result) if result else None

    @strawberry.field
    def instrument(
        self, info: Info, instrument_id: str
    ) -> Optional[InstrumentType]:
        """Fetch instrument with optional nested trades. Requires: ROLE_TRADE_READ"""
        ctx  = info.context["security_context"]
        repo = info.context["instrument_repository"]
        _assert_role(ctx, "ROLE_TRADE_READ")
        result = repo.find_by_id(instrument_id)
        return _map_instrument(result) if result else None

    @strawberry.field
    def daily_pnl(
        self,
        info:        Info,
        report_date: date,
        region:      Optional[str] = None,
    ) -> list[DailyPnlType]:
        """Daily PnL summary. Requires: ROLE_TRADE_ADMIN"""
        ctx  = info.context["security_context"]
        repo = info.context["trade_repository"]
        audit = info.context["audit_logger"]

        _assert_role(ctx, "ROLE_TRADE_ADMIN")
        audit.log(ctx, "GQL_DAILY_PNL", "daily_pnl",
                  {"report_date": str(report_date), "region": region})

        return repo.get_daily_pnl(report_date=report_date, region=region)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _assert_role(ctx, role: str):
    if role not in ctx.roles and "ROLE_PLATFORM_ADMIN" not in ctx.roles:
        raise PermissionError(f"Requires {role}")


def _map_trade(t) -> TradeType:
    """Map repository model to GraphQL type."""
    return TradeType(
        trade_id          = t.trade_id,
        instrument_id     = t.instrument_id,
        instrument_name   = t.instrument_name,
        asset_class       = t.asset_class,
        notional          = float(t.notional) if t.notional else None,
        notional_usd      = float(t.notional_usd) if t.notional_usd else None,
        currency          = t.currency,
        trade_date        = t.trade_date,
        settlement_date   = t.settlement_date,
        trader_id         = t.trader_id,
        desk              = t.desk,
        book_id           = t.book_id,
        counterparty_name = t.counterparty_name,
        credit_rating     = t.credit_rating,
        status            = t.status,
    )


def _map_instrument(i) -> InstrumentType:
    return InstrumentType(
        instrument_id   = i.instrument_id,
        instrument_name = i.instrument_name,
        asset_class     = i.asset_class,
        sector          = i.sector,
        currency        = i.currency,
        is_active       = i.is_active,
    )


# ── Schema & App ──────────────────────────────────────────────────────────────

schema = strawberry.Schema(
    query = Query,
    config = strawberry.schema.config.StrawberryConfig(
        max_depth = 5,   # prevent deeply nested expensive queries
    ),
)

# Mount via FastAPI in app.py:
# app.include_router(GraphQLRouter(schema, context_getter=get_context), prefix="/graphql")
