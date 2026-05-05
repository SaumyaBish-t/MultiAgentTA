import asyncio
from loguru import logger
from data_ingestion.collectors.macro_collector import macro_collector
from data_ingestion.storage.storage_manager import StorageManager

async def run():
    storage = StorageManager()
    
    # Tracked series in settings are usually: 
    # ['FEDFUNDS', 'CPIAUCSNS', 'UNRATE', 'GDP', 'T10Y2Y', 'VIXCLS', 'SP500']
    series_ids = ['FEDFUNDS', 'CPIAUCSNS', 'UNRATE', 'GDP', 'T10Y2Y']
    
    logger.info(f"Starting macro data seeding for: {series_ids}")
    
    for sid in series_ids:
        try:
            logger.info(f"Fetching {sid}...")
            df = await macro_collector.fetch_series(sid)
            if not df.empty:
                res = storage.write_macro(df)
                logger.info(f"✅ {sid}: {res.inserted} observations inserted")
            else:
                logger.warning(f"⚠️ No data for {sid}")
        except Exception as e:
            logger.error(f"❌ Failed to seed {sid}: {e}")

    logger.info("Done seeding macro data.")

if __name__ == "__main__":
    asyncio.run(run())
