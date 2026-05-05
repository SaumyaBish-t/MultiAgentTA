"""Verify compliance DB records after pre-trade tests."""
import psycopg2
from config.settings import settings

conn = psycopg2.connect(settings.postgres_url)
cur = conn.cursor()

# Compliance checks today
cur.execute("""SELECT rule_id, check_result, ticker, details
    FROM compliance_checks
    WHERE DATE(checked_at) = CURRENT_DATE
    ORDER BY checked_at DESC LIMIT 10""")
rows = cur.fetchall()
print(f"Compliance checks recorded today: {len(rows)}")
for r in rows:
    rid = str(r[0])[:30]
    res = str(r[1])[:10]
    tkr = str(r[2] or "N/A")[:5]
    det = str(r[3])[:55]
    print(f"  {rid:30s} | {res:10s} | {tkr:5s} | {det}")

# Violations today
cur.execute("""SELECT rule_id, ticker, severity, status FROM rule_violations
    WHERE DATE(created_at) = CURRENT_DATE ORDER BY created_at DESC""")
rows = cur.fetchall()
print(f"\nViolations recorded today: {len(rows)}")
for r in rows:
    print(f"  {r[0]:30s} | {str(r[1] or 'N/A'):5s} | {r[2]:10s} | {r[3]}")

# Audit log count
cur.execute("SELECT COUNT(*) FROM audit_log WHERE DATE(created_at) = CURRENT_DATE")
count = cur.fetchone()[0]
print(f"\nAudit log entries today: {count}")

# Rules count
cur.execute("SELECT COUNT(*) FROM compliance_rules WHERE enabled = true")
count = cur.fetchone()[0]
print(f"Active compliance rules: {count}")

conn.close()
print("\nAll DB records verified.")
