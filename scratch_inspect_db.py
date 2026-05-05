from data_ingestion.storage.init_db import get_db_manager
from sqlalchemy import text
db = get_db_manager()
with db.timescale_session() as session:
    print("--- Columns ---")
    res = session.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ohlcv_bars'"))
    for row in res:
        print(row)
    print("--- Constraints ---")
    res = session.execute(text("SELECT conname, contype FROM pg_constraint c JOIN pg_class t ON c.conrelid = t.oid WHERE t.relname = 'ohlcv_bars'"))
    for row in res:
        print(row)
