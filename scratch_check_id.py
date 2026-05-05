from data_ingestion.storage.init_db import get_db_manager
from sqlalchemy import text
db = get_db_manager()
with db.timescale_session() as session:
    res = session.execute(text("SELECT column_default FROM information_schema.columns WHERE table_name = 'ohlcv_bars' AND column_name = 'id'"))
    for row in res:
        print(f"ID Default: {row[0]}")
