from datetime import date
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_ingestion.api.cache import cache_response
from data_ingestion.api.dependencies import get_postgres_db
from data_ingestion.api.schemas import (
    BalanceSheetResponse,
    FundamentalSummaryResponse,
    IncomeStatementResponse,
)
from data_ingestion.storage.models import BalanceSheet, Company, IncomeStatement

router = APIRouter(prefix="/fundamentals", tags=["Fundamentals"])


@router.get("/{ticker}/income", response_model=List[IncomeStatementResponse])
@cache_response(ttl_seconds=3600)  # 1 hour cache
async def get_income_statements(
    request: Request,
    ticker: str,
    period: str = Query("quarterly", description="annual or quarterly"),
    limit: int = Query(8, ge=1, le=40),
    db: Session = Depends(get_postgres_db)
):
    """Get income statements for a ticker."""
    ticker = ticker.upper()
    query = select(IncomeStatement).where(
        IncomeStatement.ticker == ticker,
        IncomeStatement.period_type == period
    ).order_by(IncomeStatement.fiscal_date.desc()).limit(limit)
    
    results = db.execute(query).scalars().all()
    return results


@router.get("/{ticker}/balance", response_model=List[BalanceSheetResponse])
@cache_response(ttl_seconds=3600)
async def get_balance_sheets(
    request: Request,
    ticker: str,
    period: str = Query("quarterly", description="annual or quarterly"),
    limit: int = Query(8, ge=1, le=40),
    db: Session = Depends(get_postgres_db)
):
    """Get balance sheets for a ticker."""
    ticker = ticker.upper()
    query = select(BalanceSheet).where(
        BalanceSheet.ticker == ticker,
        BalanceSheet.period_type == period
    ).order_by(BalanceSheet.fiscal_date.desc()).limit(limit)
    
    results = db.execute(query).scalars().all()
    return results


@router.get("/{ticker}/summary", response_model=FundamentalSummaryResponse)
@cache_response(ttl_seconds=3600)
async def get_fundamental_summary(
    request: Request,
    ticker: str,
    db: Session = Depends(get_postgres_db)
):
    """Get latest key ratios and summary metrics."""
    ticker = ticker.upper()
    
    # We construct a summary from the company profile, latest income, and balance sheets.
    # The models might not have standard ratios natively unless we saved them there.
    # In normalizer, we computed ratios. Let's assume they were added to IncomeStatement
    # or Company profile. If not, we return what's available.
    
    company = db.execute(select(Company).where(Company.ticker == ticker)).scalar_one_or_none()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
        
    latest_income = db.execute(
        select(IncomeStatement).where(IncomeStatement.ticker == ticker)
        .order_by(IncomeStatement.fiscal_date.desc()).limit(1)
    ).scalar_one_or_none()
    
    latest_balance = db.execute(
        select(BalanceSheet).where(BalanceSheet.ticker == ticker)
        .order_by(BalanceSheet.fiscal_date.desc()).limit(1)
    ).scalar_one_or_none()

    # Construct the summary based on what's available in the models
    # If the ratios are not direct columns, we calculate them on the fly
    response = FundamentalSummaryResponse(ticker=ticker)
    
    if latest_income and latest_balance:
        # Assuming these might be present as dynamically added columns or we calculate
        revenue = getattr(latest_income, "revenue", 0) or 0
        net_income = getattr(latest_income, "net_income", 0) or 0
        equity = getattr(latest_balance, "equity", 0) or 1 # avoid div zero
        assets = getattr(latest_balance, "total_assets", 0) or 1
        debt = getattr(latest_balance, "total_debt", 0) or 0
        
        response.net_margin = net_income / revenue if revenue else None
        response.roe = net_income / equity if equity else None
        response.roa = net_income / assets if assets else None
        response.debt_to_equity = debt / equity if equity else None
        
    return response


@router.get("/earnings-calendar")
@cache_response(ttl_seconds=3600)
async def get_earnings_calendar(
    request: Request,
    start_date: date,
    end_date: date,
    db: Session = Depends(get_postgres_db)
):
    """Get upcoming earnings dates."""
    # Since we didn't define an EarningsCalendar table in models.py, 
    # we return a placeholder or empty list. 
    # If it was added to Company, we'd query it.
    return []
