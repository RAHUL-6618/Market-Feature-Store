import subprocess
import psycopg2
import os
from dotenv import load_dotenv
import ingest

load_dotenv()

conn = psycopg2.connect(os.getenv("DB"))
cur = conn.cursor()

tickers = ["AAPL", "MSFT", "GOOGL"]
start   = "2024-01-01"
end     = "2024-12-31"

print("=== market-feature-store ===\n")

print("[1/3] ingesting live quotes...")
for t in tickers:
    row = ingest.grab_quote(t)
    print(f"  {t}  price={row[1]}")

print("\n[2/3] ingesting ohlcv history...")
for t in tickers:
    df = ingest.grab_ohlcv(t, start, end)
    print(f"  {t}  rows={len(df)}")

print("\n[3/3] refreshing feature views...")
subprocess.run(["psql", "mfs", "-f", "features.sql"], capture_output=True)

print("\n[done] querying gold.features sample...")
cur.execute("SELECT ticker, dt, close, rsi_14, vol_20d FROM gold.features ORDER BY dt DESC LIMIT 6")
rows = cur.fetchall()
for r in rows:
    print(f"  {r[0]}  {r[1]}  close={r[2]}  rsi={r[3]}  vol={r[4]}")

conn.close()