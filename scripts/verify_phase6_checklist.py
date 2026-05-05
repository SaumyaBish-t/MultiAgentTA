import psycopg2
from config.settings import settings

def check_db_state():
    conn = psycopg2.connect(settings.postgres_url)
    cur = conn.cursor()

    # 1. Broker Connections
    cur.execute("SELECT broker_name, account_type, cash_balance FROM broker_connections WHERE broker_name = 'alpaca'")
    row = cur.fetchone()
    if row:
        print(f"Broker Connection: {row[0]} | Paper: {row[1]} | Cash: ${row[2]:,.2f}")
    else:
        print("Broker connection not found.")

    # 2. Portfolio Positions Status
    cur.execute("SELECT ticker, status, updated_at FROM portfolio_positions ORDER BY updated_at DESC LIMIT 5")
    rows = cur.fetchall()
    print("Latest Position Statuses:")
    for r in rows:
        print(f"  - {r[0]}: {r[1]} (Updated: {r[2]})")

    # 3. Orders and Batches
    cur.execute("SELECT COUNT(*) FROM orders")
    order_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM order_batches")
    batch_count = cur.fetchone()[0]
    print(f"✅ DB Storage: {order_count} orders, {batch_count} batches")

    conn.close()

if __name__ == "__main__":
    check_db_state()
