# 🔥 Forge Trading System (MultiModelTA)

> An autonomous, multi-agent quantitative trading platform powered by a fleet of free-tier LLMs. The system researches, generates, backtests, and monitors trading strategies end-to-end — all orchestrated through a real-time React dashboard with live-streaming TradingView charts.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [System Architecture](#-system-architecture)
- [Module Deep Dive](#-module-deep-dive)
  - [1. Data Ingestion](#1-data-ingestion-data_ingestion)
  - [2. Alpha Research](#2-alpha-research-alpha_research)
  - [3. Signal Generation](#3-signal-generation-signal_generation)
  - [4. Risk Management](#4-risk-management-risk_management)
  - [5. Portfolio Construction](#5-portfolio-construction-portfolio_construction)
  - [6. Execution](#6-execution-execution)
  - [7. Compliance](#7-compliance-compliance)
  - [8. Monitoring Dashboard](#8-monitoring-dashboard-monitoring)
  - [9. Frontend (React)](#9-frontend-trading-frontend)
  - [10. Config & LLM Orchestration](#10-config--llm-orchestration-config)
- [Real-Time Data Streaming](#-real-time-data-streaming)
- [LLM Provider Architecture](#-llm-provider-architecture)
- [Tech Stack](#-tech-stack)
- [Getting Started](#-getting-started)
- [Environment Variables](#-environment-variables)
- [Project Structure](#-project-structure)
- [License](#-license)

---

## 🌟 Overview

Forge is a **full-stack, AI-driven quantitative trading system** designed to operate autonomously across US and Indian equity markets. It combines:

- **30+ specialized AI agents** spanning research, signal generation, risk, execution, compliance, and monitoring.
- A **multi-provider LLM backbone** (Groq, Cerebras, OpenRouter, Mistral, NVIDIA NIM) that rotates API keys and falls back across providers to stay entirely within free tiers.
- A **real-time React dashboard** with TradingView-powered charts that stream live market data via Server-Sent Events (SSE) without requiring manual page refreshes.

### Supported Markets
| Market | Tickers (Default) |
|--------|-------------------|
| 🇺🇸 US Equities | AAPL, MSFT, GOOGL, AMZN, NVDA, TSLA, JPM, SPY, QQQ |
| 🇮🇳 Indian (NSE) | RELIANCE.NS, TCS.NS, HDFCBANK.NS, INFY.NS, ICICIBANK.NS |

---

## 🏗️ System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      REACT FRONTEND (Vite)                       │
│  TradingView Charts │ Command Center │ Portfolio │ Risk │ Audit  │
│  ← SSE Live Ticks ──┘                                           │
└───────────────────────────────┬──────────────────────────────────┘
                                │ HTTP + SSE
┌───────────────────────────────▼──────────────────────────────────┐
│                   FASTAPI MONITORING DASHBOARD (:8001)            │
│  strategy_comparison │ realtime │ pipeline_trigger │ signals      │
│  portfolio_detail    │ risk_detail │ audit_detail                 │
└─────┬───────────────────────┬──────────────────────┬─────────────┘
      │                       │                      │
      ▼                       ▼                      ▼
┌───────────┐         ┌─────────────┐        ┌────────────┐
│TimescaleDB│         │ PostgreSQL  │        │   Redis    │
│  (OHLCV)  │         │(Signals,    │        │ (Pub/Sub,  │
│ Port 5435 │         │ Backtests,  │        │  Pipeline  │
│           │         │ Compliance) │        │  State)    │
│           │         │ Port 5434   │        │ Port 6379  │
└───────────┘         └─────────────┘        └────────────┘
      ▲                       ▲
      │                       │
┌─────┴───────────────────────┴────────────────────────────────────┐
│                    DATA INGESTION API (:8000)                     │
│  /prices │ /fundamentals │ /news │ /macro │ /health              │
│  Sources: Yahoo Finance v8 │ Alpaca │ FMP │ FRED │ Indian API    │
└──────────────────────────────────────────────────────────────────┘
      ▲
      │ Consumed by
┌─────┴────────────────────────────────────────────────────────────┐
│                      AI AGENT PIPELINE                           │
│                                                                  │
│  Alpha Research ─→ Signal Generation ─→ Risk Gate ─→ Portfolio   │
│  (6 agents)        (6 agents)           (6 agents)   (5 agents)  │
│                                                    ─→ Execution  │
│                                                       (5 agents) │
│                                                    ─→ Compliance │
│                                                       (5 agents) │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🔬 Module Deep Dive

### 1. Data Ingestion (`data_ingestion/`)

The data backbone responsible for sourcing, normalizing, and persisting market data from multiple providers.

| Sub-module | Purpose |
|------------|---------|
| `api/routers/prices.py` | REST endpoints for OHLCV bars — supports TimescaleDB, Yahoo Finance (v8 direct), Alpaca, and Indian Stock API fallbacks |
| `api/routers/fundamentals.py` | Company financial summaries via Financial Modeling Prep (FMP) |
| `api/routers/news.py` | News article search and sentiment via NewsAPI |
| `api/routers/macro.py` | Macroeconomic snapshots via FRED (Federal Reserve Economic Data) |
| `api/routers/health.py` | System health checks and data freshness validation |
| `collectors/` | Background data collection workers |
| `cleaners/` | Data quality and cleaning pipelines |
| `normalizers/` | Cross-source schema normalization |
| `storage/` | TimescaleDB models and staging (Parquet) |

**Key Design Decision:** Data is fetched on-demand by agents and also stored in a Parquet-based staging area (`storage/staging/`) for offline analysis. TimescaleDB uses hypertables with `time_bucket()` aggregation for fast OHLCV queries at any resolution.

---

### 2. Alpha Research (`alpha_research/`)

The "brain" — a team of 6 AI agents that collaboratively research market conditions and generate trading hypotheses.

| Agent | LLM Provider | Role |
|-------|-------------|------|
| **Hypothesis Agent** | OpenRouter (DeepSeek R1) | Synthesizes all research outputs into a directional trade idea with a conviction score (0.0–1.0). Rejects if conviction < 0.3 |
| **Technical Agent** | Groq (Llama 3.3 70B) | Analyzes price action, trend, momentum, and support/resistance levels |
| **Fundamental Agent** | OpenRouter (DeepSeek R1) | Evaluates earnings, revenue growth, valuation ratios, and competitive positioning |
| **Sentiment Agent** | Groq (Llama 3.3 70B) | Processes news articles and social media for market sentiment scoring |
| **Macro Agent** | Groq (Llama 3.3 70B) | Assesses macroeconomic conditions (interest rates, GDP, inflation) via FRED data |
| **Document Agent** | Cerebras (Qwen 3 235B) | Extracts insights from uploaded PDFs, SEC filings, and research reports |

**Capital Protection Logic:** The hypothesis agent enforces a hardcoded conviction threshold of **0.3**. If the combined sentiment, technical, and fundamental signals yield a conviction score below this threshold, the hypothesis is **automatically rejected** to protect capital. This is by design — the system acts as a conservative risk filter.

---

### 3. Signal Generation (`signal_generation/`)

Converts validated hypotheses into executable trading signals through backtesting and optimization.

| Agent | Role |
|-------|------|
| **Strategy Coder Agent** | Generates Python backtesting code from the hypothesis using an LLM |
| **Backtester Agent** | Executes the generated strategy against historical OHLCV data and produces equity curves |
| **Optimizer Agent** | Tunes strategy parameters (entry/exit thresholds, stop-loss levels) |
| **Walk-Forward Agent** | Validates strategy robustness using rolling out-of-sample windows |
| **Signal Scorer Agent** | Ranks signals by risk-adjusted return (Sharpe, Sortino, max drawdown) |
| **Decay Monitor Agent** | Tracks signal performance decay over time and triggers re-evaluation |

**Data Sanitization:** The backtester explicitly uses subset-scoped `dropna(subset=['open', 'high', 'low', 'close', 'volume'])` to prevent empty dataframe errors when optional metadata columns contain NaN values.

---

### 4. Risk Management (`risk_management/`)

A comprehensive risk layer with 6 specialized agents that evaluate every trade before execution.

| Agent | Role |
|-------|------|
| **Risk Gate Agent** | Central gatekeeper — blocks trades exceeding portfolio risk limits |
| **VaR Agent** | Calculates Value-at-Risk using historical simulation and parametric methods |
| **Position Sizing Agent** | Determines optimal position sizes using Kelly Criterion and volatility-adjusted models |
| **Drawdown Monitor Agent** | Tracks real-time portfolio drawdowns and triggers circuit breakers at configurable thresholds |
| **Correlation Agent** | Monitors inter-asset correlations to prevent over-concentration |
| **Liquidity Agent** | Evaluates market depth and spread to ensure executable order sizes |

---

### 5. Portfolio Construction (`portfolio_construction/`)

Manages the overall portfolio allocation and rebalancing process.

| Agent | Role |
|-------|------|
| **Optimizer Agent** | Mean-variance optimization with constraint handling (sector limits, position caps) |
| **Allocation Agent** | Converts optimizer outputs into concrete position targets |
| **Rebalancing Agent** | Determines when and how to rebalance based on drift thresholds |
| **Factor Agent** | Factor exposure analysis (momentum, value, quality, volatility) |
| **Cost Estimator Agent** | Estimates transaction costs (commissions, slippage, market impact) to ensure rebalancing is cost-effective |

---

### 6. Execution (`execution/`)

Handles the final mile — converting portfolio decisions into actual market orders.

| Agent | Role |
|-------|------|
| **Order Generation Agent** | Creates market/limit orders from portfolio targets |
| **Smart Order Router Agent** | Routes orders to the best venue/exchange for optimal fills |
| **Execution Monitor Agent** | Tracks order status, partial fills, and execution quality |
| **Post-Trade Agent** | Reconciles executed trades against intended orders |
| **Emergency Handler** | Implements emergency position liquidation protocols |

**Broker Integration:** The `brokers/` sub-module contains Alpaca integration for paper trading (Paper Trading Mode displayed on the frontend).

---

### 7. Compliance (`compliance/`)

Ensures all trading activity meets regulatory and internal policy requirements.

| Agent | Role |
|-------|------|
| **Pre-Trade Compliance** | Validates orders against rules *before* submission (position limits, restricted lists) |
| **Position Limit Agent** | Enforces maximum position sizes per ticker and sector |
| **Wash Sale / PDT Tracker** | Tracks wash sale violations and Pattern Day Trader (PDT) rule compliance |
| **Audit Logger** | Creates immutable audit trail entries with cryptographic hashing |
| **Report Generator** | Generates compliance reports for review |

---

### 8. Monitoring Dashboard (`monitoring/`)

The FastAPI backend serving the real-time dashboard.

| Router | Endpoint | Purpose |
|--------|----------|---------|
| `strategy_comparison.py` | `GET /strategy-comparison/{ticker}` | Returns OHLCV + strategy equity curve + trade markers for chart rendering |
| `realtime.py` | `GET /realtime/stream/prices/{ticker}` | SSE stream delivering live price ticks every 5 seconds |
| `realtime.py` | `GET /realtime/stream/pipeline/{run_id}` | SSE stream for pipeline execution progress |
| `realtime.py` | `GET /realtime/stream/portfolio` | SSE stream for portfolio value updates |
| `pipeline_trigger.py` | `POST /pipeline/run/{ticker}` | Triggers the AI research pipeline for a ticker |
| `portfolio_detail.py` | `GET /portfolio/*` | Portfolio positions, performance, and history |
| `signals_detail.py` | `GET /signals/*` | Active signals and their execution status |
| `risk_detail.py` | `GET /risk/*` | Risk metrics, VaR, and exposure analysis |
| `audit_detail.py` | `GET /audit/*` | Compliance audit trail |

**Additional Endpoints on `dashboard_api.py`:** `/status`, `/alerts`, `/performance`, `/regime`, `/health/detailed`, `/feedback`, and a WebSocket at `/ws/live`.

---

### 9. Frontend (`trading-frontend/`)

A React/TypeScript single-page application with 6 main pages.

| Page | File | Features |
|------|------|----------|
| **Command Center** | `CommandCenter.tsx` | System overview, pipeline status, quick actions |
| **Strategy Lab** | `StrategyComparison.tsx` | TradingView chart with live SSE ticks, AI pipeline trigger, strategy equity overlay, MA20/MA50/Bollinger Bands, trade entry/exit markers |
| **Signal Intel** | `Signals.tsx` | Active signal list, conviction scores, P&L tracking |
| **Portfolio** | `Portfolio.tsx` | Holdings, allocation breakdown, performance metrics |
| **Risk Terminal** | `Risk.tsx` | Real-time VaR, drawdown monitoring, correlation matrix |
| **Audit Ledger** | `Audit.tsx` | Immutable compliance audit trail viewer |

**Chart Component (`TradingViewChart.tsx`):**
- Uses `lightweight-charts` (TradingView) for high-performance financial charting
- Historical data loaded once via React Query (`setData()` + `fitContent()`)
- Live intra-candle updates via SSE using `update()` — preserves user zoom/pan state
- Toggle overlays: MA20, MA50, Bollinger Bands, Strategy Curve, Volume

---

### 10. Config & LLM Orchestration (`config/`)

| File | Purpose |
|------|---------|
| `settings.py` | Pydantic-settings v2 configuration — all API keys, DB URLs, tickers, timeframes, and tuning parameters loaded from `.env` |
| `llm_config.py` | `LLMFactory` class that manages all LLM instances with provider-specific fallback chains |

---

## 📡 Real-Time Data Streaming

The system implements a multi-layer real-time architecture:

```
Yahoo Finance / Indian Stock API
        │ (polled every 5s)
        ▼
  realtime.py SSE Generator
        │
        ▼ EventSource (SSE)
  TradingViewChart.tsx
        │
        ▼ candleSeries.update()
  Live Candle Animation
  (zoom/scroll preserved)
```

1. **SSE Backend** (`realtime.py`): Directly fetches live prices from Yahoo Finance (v8 API) or the Indian Stock API every 5 seconds — does NOT rely on TimescaleDB being continuously populated.
2. **React Query** (`StrategyComparison.tsx`): Fetches historical chart data once on load. The `livePrice` query refreshes every 10 seconds for the price header.
3. **Chart Guard** (`TradingViewChart.tsx`): An `initialLoadDoneRef` ensures `setData()` + `fitContent()` only runs once per ticker/timeframe change. All subsequent updates use incremental `update()` calls to preserve the user's zoom state.

---

## 🧠 LLM Provider Architecture

The system uses **zero-cost LLM infrastructure** by orchestrating across multiple free-tier providers:

```
┌─────────────────────────┬────────────────┬──────────────────────────┐
│ Agent Role              │ Provider       │ Model                    │
├─────────────────────────┼────────────────┼──────────────────────────┤
│ Orchestrator            │ Groq           │ Llama 3.3 70B Versatile  │
│ Signal Generation       │ Groq           │ Llama 3.3 70B            │
│ Risk / Speed-Critical   │ Groq           │ Llama 3.3 70B            │
│ Compliance (Simple)     │ Groq           │ Llama 3.1 8B Instant     │
│ Research / Documents    │ Cerebras       │ Qwen 3 235B              │
│ Agentic Tool-Use        │ Cerebras       │ Qwen 3 235B              │
│ Hypothesis / Reasoning  │ OpenRouter     │ DeepSeek R1              │
│ Embeddings (RAG)        │ NVIDIA NIM     │ NV-EmbedQA-E5-v5         │
│ Ultimate Fallback       │ Mistral        │ Mistral Small Latest     │
└─────────────────────────┴────────────────┴──────────────────────────┘
```

**Key Resilience Features:**
- **API Key Rotation:** Multiple Groq and OpenRouter keys are rotated round-robin via `_groq_key_index`.
- **Automatic Fallbacks:** Every Groq call is wrapped with `.with_fallbacks()` using all remaining keys + Mistral as the ultimate safety net.
- **Audit Logging:** Every LLM call is logged to the PostgreSQL `audit_log` table with model name, token count, and timestamp via the `@log_llm_call` decorator.

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Backend Framework** | FastAPI |
| **Frontend Framework** | React 18 + TypeScript + Vite |
| **Styling** | Tailwind CSS |
| **Charting** | `lightweight-charts` (TradingView) |
| **State Management** | TanStack Query (React Query) |
| **Time-Series DB** | TimescaleDB (PostgreSQL extension) |
| **Relational DB** | PostgreSQL 16 |
| **Cache / Pub-Sub** | Redis 7 |
| **Vector Store** | ChromaDB (local) |
| **Workflow Orchestration** | Prefect 3 |
| **LLM Framework** | Langchain (Groq, OpenAI, Mistral adapters) |
| **Market Data** | Yahoo Finance v8, Alpaca, FMP, FRED, Indian Stock API |
| **Icons** | Lucide React |
| **Containerization** | Docker Compose |

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Node.js 18+
- Docker & Docker Compose (for databases)

### 1. Start Infrastructure
```bash
docker-compose up -d
```
This starts TimescaleDB (port 5435), PostgreSQL (port 5434), Redis (port 6379), and Prefect (port 4200).

### 2. Backend Setup
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Configure Environment
Create a `.env` file in the project root with all required API keys (see [Environment Variables](#-environment-variables)).

### 4. Run Backend Services
```bash
# Terminal 1: Data Ingestion API (port 8000)
python -m data_ingestion.api.main

# Terminal 2: Monitoring Dashboard API (port 8001)
python -m uvicorn monitoring.dashboard.dashboard_api:app --port 8001 --reload
```

### 5. Run Frontend
```bash
cd trading-frontend
npm install
npm run dev
```
Open `http://localhost:5173` (or 5174 if 5173 is occupied).

---

## 🔐 Environment Variables

| Variable | Description |
|----------|-------------|
| `POLYGON_API_KEY` | Polygon.io market data |
| `FMP_API_KEY` | Financial Modeling Prep fundamentals |
| `ALPACA_API_KEY` / `ALPACA_SECRET_KEY` | Alpaca trading + market data |
| `FRED_API_KEY` | Federal Reserve Economic Data |
| `NEWS_API_KEY` | NewsAPI.org |
| `GROQ_API_KEY` / `GROQ_API_KEYS` | Groq LLM (comma-separated for rotation) |
| `CEREBRAS_API_KEY` | Cerebras LLM |
| `OPENROUTER_API_KEY` / `OPENROUTER_API_KEYS` | OpenRouter LLM |
| `MISTRAL_API_KEY` | Mistral fallback LLM |
| `NVIDIA_API_KEY` | NVIDIA NIM embeddings |
| `TIMESCALE_URL` | TimescaleDB connection string |
| `POSTGRES_URL` | PostgreSQL connection string |
| `REDIS_URL` | Redis connection string |

---

## 📁 Project Structure

```
trading-system/
├── config/                     # Settings & LLM factory
│   ├── settings.py             # Pydantic-settings v2 config
│   └── llm_config.py           # Multi-provider LLMFactory
├── data_ingestion/             # Market data sourcing
│   ├── api/routers/            # REST endpoints (prices, fundamentals, news, macro)
│   ├── collectors/             # Background data workers
│   ├── cleaners/               # Data quality pipelines
│   ├── normalizers/            # Cross-source normalization
│   └── storage/                # TimescaleDB models + Parquet staging
├── alpha_research/             # AI research agents
│   ├── agents/                 # hypothesis, technical, fundamental, sentiment, macro, document
│   ├── flows/                  # Prefect flow orchestration
│   └── pipeline/               # Research pipeline coordinator
├── signal_generation/          # Strategy creation & validation
│   ├── agents/                 # backtester, strategy_coder, optimizer, walk_forward, scorer, decay
│   ├── pipeline/               # Signal pipeline coordinator
│   └── storage/                # TradingSignal, BacktestResult models
├── risk_management/            # Risk assessment
│   ├── agents/                 # risk_gate, var, position_sizing, drawdown, correlation, liquidity
│   ├── flows/                  # Risk flow orchestration
│   └── pipeline/               # Risk pipeline coordinator
├── portfolio_construction/     # Portfolio optimization
│   ├── agents/                 # optimizer, allocation, rebalancing, factor, cost_estimator
│   └── pipeline/               # Portfolio pipeline coordinator
├── execution/                  # Order management
│   ├── agents/                 # order_generation, smart_order_router, execution_monitor, post_trade, emergency
│   ├── brokers/                # Alpaca broker integration
│   └── pipeline/               # Execution pipeline coordinator
├── compliance/                 # Regulatory compliance
│   ├── agents/                 # pre_trade, position_limit, wash_sale_pdt, audit_logger, report_generator
│   └── reports/                # Generated compliance reports
├── monitoring/                 # Dashboard & alerting
│   ├── dashboard/
│   │   ├── dashboard_api.py    # Main FastAPI app (port 8001)
│   │   └── routers/            # strategy_comparison, realtime, pipeline_trigger, etc.
│   ├── agents/                 # health_monitor agent
│   ├── alerts/                 # Alert manager
│   └── feedback/               # User feedback loop
├── trading-frontend/           # React SPA
│   └── src/
│       ├── pages/              # CommandCenter, StrategyComparison, Signals, Portfolio, Risk, Audit
│       ├── components/charts/  # TradingViewChart, StrategyComparisonChart
│       ├── components/layout/  # Shell (sidebar + topbar)
│       └── hooks/              # useSSE custom hook
├── scripts/                    # Utility & migration scripts
├── tests/                      # Integration tests
├── docker-compose.yml          # Infrastructure (TimescaleDB, PostgreSQL, Redis, Prefect)
├── requirements.txt            # Python dependencies
└── .env                        # Environment variables (not committed)
```

---

## 📄 License

This project is for educational and research purposes.

---

*Built for autonomous market intelligence. 🚀*
