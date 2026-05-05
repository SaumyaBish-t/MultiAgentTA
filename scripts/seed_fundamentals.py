import asyncio
import sys
from datetime import datetime, timezone
import pandas as pd
import yfinance as yf
from loguru import logger
from data_ingestion.storage.storage_manager import StorageManager
from config.settings import settings

def _safe_int(val):
    try:
        return int(float(val)) if val is not None and not pd.isna(val) else None
    except:
        return None

def _safe_float(val):
    try:
        return float(val) if val is not None and not pd.isna(val) else None
    except:
        return None

async def run(tickers_to_seed):
    storage = StorageManager()
    
    logger.info(f"Starting fundamental data seeding via yfinance for: {tickers_to_seed}")
    
    for ticker_sym in tickers_to_seed:
        ticker_sym = ticker_sym.upper()
        logger.info(f"--- Seeding {ticker_sym} ---")
        
        try:
            yticker = yf.Ticker(ticker_sym)
            info = yticker.info
            
            # 1. Company Profile
            profile_data = {
                "ticker": ticker_sym,
                "name": info.get("longName", info.get("shortName", "")),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "exchange": info.get("exchange"),
                "market_cap": _safe_int(info.get("marketCap")),
                "shares_outstanding": _safe_int(info.get("sharesOutstanding")),
                "currency": info.get("currency", "USD").upper(),
                "updated_at": datetime.now(tz=timezone.utc),
            }
            profile_df = pd.DataFrame([profile_data])
            res = storage.write_fundamentals(profile_df, "companies")
            logger.info(f"✅ Company profile: {res.inserted} inserted")

            # 2. Income Statements (Quarterly)
            income_stmt = yticker.quarterly_income_stmt
            if not income_stmt.empty:
                # Transpose and rename columns
                income_stmt = income_stmt.T.reset_index()
                income_stmt.columns = [c.lower().replace(" ", "_") for c in income_stmt.columns]
                
                rows = []
                for _, row in income_stmt.iterrows():
                    rows.append({
                        "ticker": ticker_sym,
                        "period_type": "quarterly",
                        "fiscal_date": row["index"].to_pydatetime().replace(tzinfo=timezone.utc),
                        "revenue": _safe_int(row.get("total_revenue", row.get("revenue"))),
                        "gross_profit": _safe_int(row.get("gross_profit")),
                        "operating_income": _safe_int(row.get("operating_income")),
                        "net_income": _safe_int(row.get("net_income_common_stockholders", row.get("net_income"))),
                        "eps": _safe_float(row.get("basic_eps")),
                        "ebitda": _safe_int(row.get("ebitda")),
                        "source": "yfinance",
                    })
                
                income_df = pd.DataFrame(rows)
                res = storage.write_fundamentals(income_df, "income_statements")
                logger.info(f"✅ Income statements: {res.inserted} inserted")
            else:
                logger.warning(f"⚠️ No income statement data for {ticker_sym}")

            # 3. Balance Sheets (Quarterly)
            balance_sheet = yticker.quarterly_balance_sheet
            if not balance_sheet.empty:
                balance_sheet = balance_sheet.T.reset_index()
                balance_sheet.columns = [c.lower().replace(" ", "_") for c in balance_sheet.columns]
                
                rows = []
                for _, row in balance_sheet.iterrows():
                    rows.append({
                        "ticker": ticker_sym,
                        "fiscal_date": row["index"].to_pydatetime().replace(tzinfo=timezone.utc),
                        "period_type": "quarterly",
                        "total_assets": _safe_int(row.get("total_assets")),
                        "total_liabilities": _safe_int(row.get("total_liabilities_net_minority_interest", row.get("total_liabilities"))),
                        "equity": _safe_int(row.get("stockholders_equity")),
                        "cash": _safe_int(row.get("cash_cash_equivalents_&_short_term_investments", row.get("cash_and_cash_equivalents"))),
                        "total_debt": _safe_int(row.get("total_debt")),
                        "source": "yfinance",
                    })
                
                balance_df = pd.DataFrame(rows)
                res = storage.write_fundamentals(balance_df, "balance_sheets")
                logger.info(f"✅ Balance sheets: {res.inserted} inserted")
            else:
                logger.warning(f"⚠️ No balance sheet data for {ticker_sym}")

        except Exception as e:
            logger.error(f"❌ Failed to seed fundamentals for {ticker_sym}: {e}")

    logger.info("Done seeding fundamentals via yfinance.")

if __name__ == "__main__":
    target_tickers = sys.argv[1:] if len(sys.argv) > 1 else settings.tickers
    if not target_tickers:
        logger.error("No tickers specified.")
        sys.exit(1)
        
    asyncio.run(run(target_tickers))
