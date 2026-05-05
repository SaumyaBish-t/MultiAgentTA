import asyncio
from data_ingestion.collectors import collector as market_data_collector
from data_ingestion.storage.storage_manager import StorageManager
from data_ingestion.normalizers import DataNormalizer

async def run():
    storage = StorageManager()
    norm = DataNormalizer()
    print('Fetching AAPL data...')
    df = await market_data_collector.fetch_historical('AAPL', '2024-01-01', '2026-04-26', '1d')
    if not df.empty:
        df = norm.normalize_timestamps(df)
        storage.write_price_bars(df)
        print(f'Done fetching {len(df)} AAPL bars.')
    else:
        print('No data returned from API.')

if __name__ == '__main__':
    asyncio.run(run())
