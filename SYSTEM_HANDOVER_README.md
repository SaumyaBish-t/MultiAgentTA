# Forge Trading System - System Handover Document

**Target Audience:** Future AI Agents / Developers  
**Purpose:** To provide a comprehensive summary of recent architectural upgrades, schema changes, and UI rewrites, and to outline the remaining gap between the current system and a professional "TradingView-grade" platform.

---

## 1. What Has Been Built & Changed (Recent Upgrades)

### A. Database & Schema Migration
- **Indian Market Support**: We migrated the `ticker` columns globally across 12+ PostgreSQL tables (e.g., `trading_signals`, `portfolio_positions`, `research_hypotheses`) and the TimescaleDB `ohlcv_bars` hypertable. 
- **Change Made**: Expanded column widths from `VARCHAR(10)` to `VARCHAR(20)` to natively support Yahoo Finance suffixes like `.NS` or `.BO` (e.g., `RELIANCE.NS`).
- **TimescaleDB Fix**: We explicitly handled compressed chunks in TimescaleDB by dynamically disabling compression/decompressing chunks to allow the `ALTER TABLE` command to succeed on the `ohlcv_bars` table.

### B. AI Pipeline Constraint Fixes
- **Issue**: The `DocumentAgent` and research pipeline were crashing when the AI returned a "rejected" or "neutral" hypothesis because the database strictly expected a timeframe like `intraday` or `swing`.
- **Fix**: We altered the `chk_expected_timeframe` constraint in the `research_hypotheses` table to accept `"n/a"`, allowing the AI to safely persist "no-trade" hypotheses without causing backend 500 errors.

### C. Backend API Enhancements
- **SSE Endpoints**: Created a dedicated `realtime.py` router inside the Dashboard API (Port 8001). It exposes Server-Sent Events (SSE) for:
  - Live Prices (`/stream/prices/{ticker}?timeframe=1min`)
  - Live Portfolio State (`/stream/portfolio`)
  - AI Pipeline Progress (`/stream/pipeline/{run_id}`)
- **Manual Pipeline Trigger**: Created a `pipeline_trigger.py` router that allows the frontend to explicitly send a POST request to kick off the `ResearchPipeline`, bypassing the need to run terminal scripts.

### D. Frontend Modernization
- **Lightweight Charts Migration**: Replaced the React Recharts library with TradingView's `lightweight-charts` (`TradingViewChart.tsx`). This significantly improved chart rendering performance and supports Candlesticks, Volume Histograms, Moving Averages, and Bollinger Bands natively.
- **Pipeline Overlays**: The `StrategyComparison.tsx` page was completely rewritten. It now features an animated overlay that tracks the AI research progress (Collecting Data -> Sentiment -> Technical -> Strategy Gen) dynamically via the SSE connection.

---

## 2. Identified Gaps: Why It's Not "TradingView" Yet

Despite the upgrades, the system currently falls short of a production-ready TradingView clone in several critical areas. Future AI assistants must focus on these gaps:

### A. True Real-Time Data & Indian Market Visibility
- **Current State**: The `start_realtime_collector.py` script polls Yahoo Finance every 60 seconds and writes 1-minute bars to TimescaleDB. The UI streams these via SSE.
- **The Problem**: Indian market data (`.NS` tickers) often appears delayed or fails to stream if the collector encounters rate limits or timezone mismatch bugs during Asian market hours. True TradingView uses direct WebSockets (e.g., via Polygon.io or Interactive Brokers) for sub-second tick data.
- **Action Required**: Replace the polling collector with an asynchronous WebSocket client (e.g., Alpaca Streams) connected directly to the database writer.

### B. Charting Limitations (Lightweight Charts vs. Advanced Charts)
- **Current State**: The UI uses the open-source `lightweight-charts` package.
- **The Problem**: This package is designed for basic display. It completely lacks drawing tools (trendlines, Fibonacci retracements, text annotations) and custom PineScript-style indicators.
- **Action Required**: To achieve full TradingView parity, the project must be upgraded to the proprietary `charting_library` (which requires a TradingView license) or heavily extend `lightweight-charts` with complex custom canvas overlays.

### C. Automated Pipeline Execution
- **Current State**: When a user adds a new ticker to the dashboard, it queries the database. If no strategy exists, the user must explicitly click the "Initiate AI Research" button.
- **The Problem**: A seamless system should utilize a background task queue (like Celery or RedisRQ). The moment a user adds a stock to their watchlist, the backend should silently dispatch a worker to run the multi-agent research pipeline.
- **Action Required**: Implement a robust Redis-backed job queue for the `ResearchPipeline` and trigger it automatically from the watchlist UI component.

### D. Multi-Pane & Multi-Asset Layouts
- **Current State**: The UI supports comparing one strategy/ticker at a time.
- **The Problem**: TradingView is known for split-screen layouts (e.g., 4 charts at once) and overlaying different assets (e.g., SPY vs AAPL) on the same Y-axis scale.
- **Action Required**: Refactor the React layout engine to support a customizable grid system (using `react-grid-layout`) and update the `TradingViewChart` component to accept multiple series from different endpoints.

### E. Broken or Missing Pages
- **Current State**: While the `StrategyComparison` page was updated, other legacy pages (like deep `Risk Analysis` or `Performance Attribution`) may not be fetching the newly migrated `VARCHAR(20)` tickers correctly or might be timing out if they rely on synchronous database calls.
- **Action Required**: Audit all frontend React Query hooks to ensure they point to the robust, asynchronous Port 8001 endpoints.
