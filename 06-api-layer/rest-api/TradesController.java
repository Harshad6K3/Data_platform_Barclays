package com.dataplatform.api.trades;

import com.dataplatform.api.model.TradeResponse;
import com.dataplatform.api.model.TradeSearchRequest;
import com.dataplatform.api.model.PagedResponse;
import com.dataplatform.api.security.RequiresRole;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;

/**
 * TradesController
 *
 * Exposes curated trade data from Redshift to downstream consumers.
 *
 * Design principles:
 *  - Read-only API. All mutations flow through the event pipeline (Kafka).
 *  - Zero-trust: every request validated via JWT (OAuth 2.0 / Entra ID).
 *  - RBAC: role-based access (ROLE_TRADE_READ, ROLE_TRADE_ADMIN).
 *  - Rate-limited: 100 req/min per client_id (via API Gateway).
 *  - Paginated: max 1000 rows per response.
 *  - Audit logged: all requests logged with userId, clientId, query params.
 *
 * Base path: /api/v1/trades
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/trades")
@RequiredArgsConstructor
@Validated
@Tag(name = "Trades", description = "Trade data access API")
@SecurityRequirement(name = "bearerAuth")
public class TradesController {

    private final TradeQueryService tradeQueryService;
    private final AuditLogger auditLogger;

    /**
     * GET /api/v1/trades
     * Search trades by date range, asset class, desk, book.
     * Paginated. Max page size 1000.
     */
    @GetMapping
    @RequiresRole("ROLE_TRADE_READ")
    @Operation(summary = "Search trades", description = "Returns paginated trades matching filters")
    public ResponseEntity<PagedResponse<TradeResponse>> searchTrades(
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
        @RequestParam(required = false) String assetClass,
        @RequestParam(required = false) String desk,
        @RequestParam(required = false) String bookId,
        @RequestParam(required = false) String status,
        @PageableDefault(size = 100, max = 1000) Pageable pageable
    ) {
        auditLogger.log("TRADE_SEARCH", fromDate, toDate, assetClass, desk);

        TradeSearchRequest request = TradeSearchRequest.builder()
            .fromDate(fromDate)
            .toDate(toDate)
            .assetClass(assetClass)
            .desk(desk)
            .bookId(bookId)
            .status(status)
            .build();

        return ResponseEntity.ok(tradeQueryService.search(request, pageable));
    }

    /**
     * GET /api/v1/trades/{tradeId}
     * Retrieve a single trade with full lifecycle history.
     */
    @GetMapping("/{tradeId}")
    @RequiresRole("ROLE_TRADE_READ")
    @Operation(summary = "Get trade by ID", description = "Returns full trade lifecycle history")
    public ResponseEntity<TradeResponse> getTradeById(@PathVariable String tradeId) {
        auditLogger.log("TRADE_GET", tradeId);
        return tradeQueryService.findById(tradeId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * GET /api/v1/trades/summary/daily-pnl
     * Aggregated daily PnL by book. Used by risk dashboards.
     * Requires elevated role: ROLE_TRADE_ADMIN
     */
    @GetMapping("/summary/daily-pnl")
    @RequiresRole("ROLE_TRADE_ADMIN")
    @Operation(summary = "Daily PnL summary by book")
    public ResponseEntity<?> getDailyPnl(
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate reportDate,
        @RequestParam(required = false) String region
    ) {
        auditLogger.log("DAILY_PNL_ACCESS", reportDate, region);
        return ResponseEntity.ok(tradeQueryService.getDailyPnl(reportDate, region));
    }
}
