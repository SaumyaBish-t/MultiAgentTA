import psycopg2

conn = psycopg2.connect(host='127.0.0.1', port=5435, dbname='market_data', user='trader', password='password')
cur = conn.cursor()
try:
    cur.execute("SELECT show_chunks('ohlcv_bars');")
    chunks = cur.fetchall()
    for chunk in chunks:
        cur.execute(f"SELECT decompress_chunk('{chunk[0]}');")
    
    cur.execute('ALTER TABLE ohlcv_bars ALTER COLUMN ticker TYPE VARCHAR(20);')
    conn.commit()
    print('Migrated ohlcv_bars successfully')
except Exception as e:
    print('Error:', e)
