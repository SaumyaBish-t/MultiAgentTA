# Forge Trading System - Audit Report

## 1. Services Status
- Phase 1 Data API (Port 8000): Running (returns 200 on /health). Note that /prices endpoints returned 403 or timed out.
- Phase 8 Monitor API (Port 8001): Running (returns 200 on /health/detailed). Note that /portfolio timed out.
- Real-time Collector: Stale data. Market might be closed for US stocks (last tick was at 20:00 UTC, i.e., 4 PM EST). Indian stocks like `RELIANCE.NS` are not showing up, likely because of the incomplete schema migration.

## 2. Database Status
- **Realtime 1-min data status**: Only US stocks are present (AAPL, MSFT, etc.) but they are stale (last recorded at market close). No Indian stocks are present.
- **`ohlcv_bars.ticker`**: Width is 10 chars (NOT MIGRATED). This is causing `RELIANCE.NS` to be rejected.
- **Postgres Ticker Columns**:
  - `research_hypotheses`, `sentiment_scores`: **MIGRATED** (VARCHAR 20)
  - `trading_signals`, `portfolio_positions`, `approved_signals`, `orders`, `technical_signals`, `fundamental_scores`, `backtest_results`, `compliance_checks`, `audit_log`, `pnl_attribution`: **NOT MIGRATED** (VARCHAR 10)
- **Timeframe Constraints**: `chk_expected_timeframe` successfully updated to accept `n/a`.
- **Row Counts**: Tables have data (`trading_signals`: 37, `backtest_results`: 17, `audit_log`: 136).

## 3. API Endpoint Status
- 403 `8000/prices/AAPL/latest?timeframe=1min` (Requires API Key header)
- TIMEOUT `8000/prices/RELIANCE.NS/latest?timeframe=1min`
- TIMEOUT `8001/portfolio`
- OK `8001/strategy-comparison/AAPL?period=1m`
- OK `8001/strategy-comparison/AAPL/live-price`

## 4. Paper Trading Status
- Verified: **100% PAPER TRADING**
- Cash: $100,000.00
- Portfolio Value: $100,000.00
- Mode: Alpaca Paper API keys are correctly configured and in use.

## 5. Priority Fix List
1. **Schema Migration**: Complete the `ticker` column migration from VARCHAR(10) to VARCHAR(20) for all tables across both TimescaleDB and Postgres.
2. **Real-time Charting**: Implement Server-Sent Events (SSE) router for prices and pipeline progress.
3. **Frontend Implementation**: Add `TradingViewChart.tsx` (using Lightweight Charts) and wire it into the `StrategyComparison` page, complete with SSE streaming and pipeline triggering logic.
4. **Fix Broken Endpoints**: Ensure `/portfolio` and others do not time out, and recalculate risk snapshots or audit log hashes if they broke during migration.
