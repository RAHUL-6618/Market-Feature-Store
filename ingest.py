import finnhub
import yfinance as yf
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

fc = finnhub.Client(api_key=os.getenv("FINNHUB_KEY"))
conn = psycopg2.connect(os.getenv("DB"))
cur = conn.cursor()

def grab_quote(ticker):
    q = fc.quote(ticker)
    row = (ticker, q['c'], q.get('v', 0), datetime.now(), 'finnhub')
    cur.execute(
        "INSERT INTO bronze.quotes (ticker,price,volume,ts,source) VALUES (%s,%s,%s,%s,%s)",
        row
    )
    conn.commit()
    return row

def grab_ohlcv(ticker, start, end):
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
    df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.reset_index(inplace=True)
    for _, r in df.iterrows():
        cur.execute(
            """INSERT INTO bronze.ohlcv (ticker,open,high,low,close,volume,dt)
               VALUES (%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (ticker,dt) DO NOTHING""",
            (ticker, float(r['Open']), float(r['High']),
             float(r['Low']), float(r['Close']), int(r['Volume']),
             r['Date'].date())
        )
    conn.commit()
    return df.head()

if __name__ == "__main__":
    tickers = ["AAPL", "MSFT", "GOOGL"]

    print("--- quotes ---")
    for t in tickers:
        row = grab_quote(t)
        print(t, row[1], row[2])

    print("\n--- ohlcv ---")
    for t in tickers:
        df = grab_ohlcv(t, "2024-01-01", "2024-12-31")
        print(t, df.shape)