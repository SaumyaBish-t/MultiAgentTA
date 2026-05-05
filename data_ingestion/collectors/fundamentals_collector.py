"""
Trading System — Fundamentals Collector
========================================

Collects company profiles, financial statements, and earnings
calendar from **Financial Modeling Prep** (FMP).

Schedule: once daily after market close (4:30 PM ET / 20:30 UTC).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from loguru import logger
from tenacity import (
    retry, retry_if_exception_type,
    stop_after_attempt, wait_exponential,
)

from config.settings import PROJECT_ROOT, settings

FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"
STAGING_DIR: Path = PROJECT_ROOT / "data_ingestion" / "storage" / "staging"

COMPANY_COLUMNS: list[str] = [
    "ticker", "name", "sector", "industry", "exchange",
    "market_cap", "shares_outstanding", "currency", "updated_at",
]
INCOME_COLUMNS: list[str] = [
    "ticker", "period_type", "fiscal_date", "revenue",
    "gross_profit", "operating_income", "net_income",
    "eps", "ebitda", "source",
]
BALANCE_COLUMNS: list[str] = [
    "ticker", "fiscal_date", "period_type", "total_assets",
    "total_liabilities", "equity", "cash", "total_debt", "source",
]


@dataclass
class FundamentalsMetrics:
    success: dict[str, int] = field(default_factory=dict)
    failure: dict[str, int] = field(default_factory=dict)
    total_rows: dict[str, int] = field(default_factory=dict)

    def record_success(self, ticker: str, rows: int) -> None:
        self.success[ticker] = self.success.get(ticker, 0) + 1
        self.total_rows[ticker] = self.total_rows.get(ticker, 0) + rows

    def record_failure(self, ticker: str) -> None:
        self.failure[ticker] = self.failure.get(ticker, 0) + 1

    def summary(self) -> dict[str, Any]:
        return {"success": dict(self.success), "failure": dict(self.failure),
                "total_rows": dict(self.total_rows)}


def _parse_fiscal_date(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _safe_int(val: Any) -> int | None:
    if val is None: return None
    try: return int(float(val))
    except (ValueError, TypeError): return None


def _safe_float(val: Any) -> float | None:
    if val is None: return None
    try: return float(val)
    except (ValueError, TypeError): return None


def _stage_parquet(df: pd.DataFrame, category: str, tag: str) -> Path | None:
    if df.empty: return None
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    out_dir = STAGING_DIR / f"type={category}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{tag}_{ts}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.debug("Staged {} rows -> {}", len(df), path.name)
    return path


class FundamentalsCollector:
    """
    Company fundamentals collector via FMP REST API.

    Provides: company profiles, income statements, balance sheets,
    earnings calendar, and TTM calculations.
    """

    def __init__(
        self,
        fmp_api_key: str | None = None,
        retry_attempts: int | None = None,
        timeout: int | None = None,
    ) -> None:
        self._api_key = fmp_api_key or settings.fmp_api_key.get_secret_value()
        self._retry_attempts = retry_attempts or settings.collector_retry_attempts
        self._timeout = timeout or settings.collector_timeout
        self._backoff = settings.collector_retry_backoff
        self.metrics = FundamentalsMetrics()
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("FundamentalsCollector initialised  retries={}  timeout={}s",
                     self._retry_attempts, self._timeout)

    async def _fmp_get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """GET request to FMP with retry."""
        url = f"{FMP_BASE_URL}/{endpoint}"
        query: dict[str, Any] = {"apikey": self._api_key}
        if params: query.update(params)

        @retry(
            stop=stop_after_attempt(self._retry_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=60, exp_base=self._backoff),
            retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
            reraise=True,
        )
        async def _do() -> Any:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.get(url, params=query)
                resp.raise_for_status()
                return resp.json()

        return await _do()

    # ── Company Profile ─────────────────────────────────────────

    async def fetch_company_profile(self, ticker: str) -> pd.DataFrame:
        """Fetch company profile matching ``Company`` model columns."""
        logger.info("fetch_company_profile  ticker={}", ticker)
        try:
            data = await self._fmp_get(f"profile/{ticker}")
            if not data or not isinstance(data, list):
                self.metrics.record_failure(ticker)
                return pd.DataFrame(columns=COMPANY_COLUMNS)
            p = data[0]
            row = {
                "ticker": ticker, "name": p.get("companyName", ""),
                "sector": p.get("sector"), "industry": p.get("industry"),
                "exchange": p.get("exchangeShortName"),
                "market_cap": _safe_int(p.get("mktCap")),
                "shares_outstanding": _safe_int(p.get("sharesOutstanding")),
                "currency": (p.get("currency") or "USD").upper(),
                "updated_at": datetime.now(tz=timezone.utc),
            }
            df = pd.DataFrame([row], columns=COMPANY_COLUMNS)
            self.metrics.record_success(ticker, 1)
            _stage_parquet(df, "company_profile", ticker)
            logger.info("Profile OK  ticker={}  name={}", ticker, row["name"])
            return df
        except Exception as exc:
            logger.error("Profile FAILED  ticker={}  err={}", ticker, exc)
            self.metrics.record_failure(ticker)
            return pd.DataFrame(columns=COMPANY_COLUMNS)

    # ── Income Statements ───────────────────────────────────────

    async def fetch_income_statement(
        self, ticker: str, period: str = "quarter", limit: int = 8,
    ) -> pd.DataFrame:
        """Fetch income statements matching ``IncomeStatement`` model."""
        logger.info("fetch_income_statement  ticker={}  period={}  limit={}", ticker, period, limit)
        try:
            data = await self._fmp_get(f"income-statement/{ticker}", {"period": period, "limit": limit})
            if not data:
                self.metrics.record_failure(ticker)
                return pd.DataFrame(columns=INCOME_COLUMNS)
            rows = []
            for item in data:
                fd = _parse_fiscal_date(item.get("date"))
                if fd is None: continue
                rows.append({
                    "ticker": ticker,
                    "period_type": "quarterly" if period == "quarter" else "annual",
                    "fiscal_date": fd,
                    "revenue": _safe_int(item.get("revenue")),
                    "gross_profit": _safe_int(item.get("grossProfit")),
                    "operating_income": _safe_int(item.get("operatingIncome")),
                    "net_income": _safe_int(item.get("netIncome")),
                    "eps": _safe_float(item.get("eps")),
                    "ebitda": _safe_int(item.get("ebitda")),
                    "source": "fmp",
                })
            df = pd.DataFrame(rows, columns=INCOME_COLUMNS)
            self.metrics.record_success(ticker, len(df))
            _stage_parquet(df, "income_statement", f"{ticker}_{period}")
            logger.info("Income OK  ticker={}  rows={}", ticker, len(df))
            return df
        except Exception as exc:
            logger.error("Income FAILED  ticker={}  err={}", ticker, exc)
            self.metrics.record_failure(ticker)
            return pd.DataFrame(columns=INCOME_COLUMNS)

    # ── Balance Sheets ──────────────────────────────────────────

    async def fetch_balance_sheet(
        self, ticker: str, period: str = "quarter", limit: int = 8,
    ) -> pd.DataFrame:
        """Fetch balance sheets matching ``BalanceSheet`` model."""
        logger.info("fetch_balance_sheet  ticker={}  period={}  limit={}", ticker, period, limit)
        try:
            data = await self._fmp_get(f"balance-sheet-statement/{ticker}", {"period": period, "limit": limit})
            if not data:
                self.metrics.record_failure(ticker)
                return pd.DataFrame(columns=BALANCE_COLUMNS)
            rows = []
            for item in data:
                fd = _parse_fiscal_date(item.get("date"))
                if fd is None: continue
                rows.append({
                    "ticker": ticker, "fiscal_date": fd,
                    "period_type": "quarterly" if period == "quarter" else "annual",
                    "total_assets": _safe_int(item.get("totalAssets")),
                    "total_liabilities": _safe_int(item.get("totalLiabilities")),
                    "equity": _safe_int(item.get("totalStockholdersEquity")),
                    "cash": _safe_int(item.get("cashAndCashEquivalents") or item.get("cashAndShortTermInvestments")),
                    "total_debt": _safe_int(item.get("totalDebt")),
                    "source": "fmp",
                })
            df = pd.DataFrame(rows, columns=BALANCE_COLUMNS)
            self.metrics.record_success(ticker, len(df))
            _stage_parquet(df, "balance_sheet", f"{ticker}_{period}")
            logger.info("Balance OK  ticker={}  rows={}", ticker, len(df))
            return df
        except Exception as exc:
            logger.error("Balance FAILED  ticker={}  err={}", ticker, exc)
            self.metrics.record_failure(ticker)
            return pd.DataFrame(columns=BALANCE_COLUMNS)

    # ── Earnings Calendar ───────────────────────────────────────

    async def fetch_earnings_calendar(
        self, start_date: str | date | None = None, end_date: str | date | None = None,
    ) -> pd.DataFrame:
        """Fetch upcoming earnings for tracked tickers (default: next 30 days)."""
        start_date = start_date or date.today()
        end_date = end_date or (date.today() + timedelta(days=30))
        logger.info("fetch_earnings_calendar  range={}/{}", start_date, end_date)
        try:
            data = await self._fmp_get("earning_calendar", {"from": str(start_date), "to": str(end_date)})
            if not data: return pd.DataFrame()
            tracked = set(settings.tickers)
            rows = [
                {"ticker": i.get("symbol"), "date": i.get("date"),
                 "eps_estimated": _safe_float(i.get("epsEstimated")),
                 "eps_actual": _safe_float(i.get("eps")),
                 "revenue_estimated": _safe_int(i.get("revenueEstimated")),
                 "fiscal_date_ending": i.get("fiscalDateEnding")}
                for i in data if i.get("symbol") in tracked
            ]
            df = pd.DataFrame(rows)
            _stage_parquet(df, "earnings_calendar", "upcoming")
            logger.info("Earnings calendar OK  tracked={}", len(df))
            return df
        except Exception as exc:
            logger.error("Earnings calendar FAILED  err={}", exc)
            return pd.DataFrame()

    # ── TTM Calculation ─────────────────────────────────────────

    @staticmethod
    def calculate_ttm(quarterly_df: pd.DataFrame) -> dict[str, Any]:
        """Sum last 4 quarters for summable fields; latest for per-share."""
        if len(quarterly_df) < 4:
            return {}
        last_4 = quarterly_df.head(4)
        summable = ["revenue", "gross_profit", "operating_income", "net_income", "ebitda"]
        ttm: dict[str, Any] = {"period_type": "ttm"}
        for col in summable:
            if col in last_4.columns:
                vals = pd.to_numeric(last_4[col], errors="coerce")
                ttm[col] = int(vals.sum()) if vals.notna().all() else None
        if "eps" in last_4.columns:
            eps = pd.to_numeric(last_4["eps"], errors="coerce")
            ttm["eps"] = round(float(eps.sum()), 4) if eps.notna().all() else None
        ttm["fiscal_date"] = quarterly_df["fiscal_date"].iloc[0]
        return ttm

    # ── Bulk Fetch ──────────────────────────────────────────────

    async def fetch_all_tickers_fundamentals(self, max_concurrency: int = 3) -> dict[str, dict[str, Any]]:
        """Fetch profiles, income, balance for all configured tickers."""
        tickers = settings.tickers
        sem = asyncio.Semaphore(max_concurrency)

        async def _one(t: str) -> tuple[str, dict[str, Any]]:
            async with sem:
                profile = await self.fetch_company_profile(t)
                income = await self.fetch_income_statement(t, "quarter", 8)
                balance = await self.fetch_balance_sheet(t, "quarter", 8)
                ttm = self.calculate_ttm(income)
                return t, {"profile": profile, "income": income, "balance": balance, "ttm": ttm}

        logger.info("fetch_all_tickers_fundamentals  tickers={}  concurrency={}", len(tickers), max_concurrency)
        results = await asyncio.gather(*[_one(t) for t in tickers])
        result_dict = dict(results)
        ok = sum(1 for d in result_dict.values() if not d["profile"].empty)
        logger.info("fetch_all_tickers_fundamentals DONE  ok={}/{}", ok, len(tickers))
        return result_dict

    def print_metrics(self) -> None:
        logger.info("FundamentalsCollector Metrics:\n{}", json.dumps(self.metrics.summary(), indent=2))


fundamentals_collector = FundamentalsCollector()


async def _main() -> None:
    profile = await fundamentals_collector.fetch_company_profile("AAPL")
    print(profile.to_string())
    income = await fundamentals_collector.fetch_income_statement("AAPL", "quarter", 4)
    print(income.to_string())
    fundamentals_collector.print_metrics()

if __name__ == "__main__":
    asyncio.run(_main())
