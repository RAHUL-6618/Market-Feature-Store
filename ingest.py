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
    from rich.console import Console
    from rich.table import Table
    from rich import box

    console = Console()
    tickers = ["AAPL", "MSFT", "GOOGL"]

    console.print("\n[bold cyan]Quotes[/]")
    q_table = Table(box=box.SIMPLE, header_style="bold cyan")
    q_table.add_column("Ticker"); q_table.add_column("Price", justify="right"); q_table.add_column("Volume", justify="right")
    for t in tickers:
        row = grab_quote(t)
        q_table.add_row(row[0], f"${row[1]:,.2f}", f"{row[2]:,}")
    console.print(q_table)

    console.print("[bold magenta]OHLCV[/]")
    o_table = Table(box=box.SIMPLE, header_style="bold magenta")
    o_table.add_column("Ticker"); o_table.add_column("Rows", justify="right")
    for t in tickers:
        df = grab_ohlcv(t, "2024-01-01", "2024-12-31")
        o_table.add_row(t, str(df.shape[0]))
    console.print(o_table)