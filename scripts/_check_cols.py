import psycopg2
from config.settings import settings
conn = psycopg2.connect(settings.postgres_url)
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'execution_performance' ORDER BY ordinal_position")
for r in cur.fetchall():
    print(r[0])
conn.close()
