import subprocess
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich import box
from rich.text import Text
from rich.columns import Columns
from rich.rule import Rule

import ingest

load_dotenv()

console = Console()

TICKERS = ["AAPL", "MSFT", "GOOGL"]
START   = "2024-01-01"
END     = "2024-12-31"


def print_header():
    title = Text("Market Feature Store", style="bold white")
    subtitle = Text("Bronze → Silver → Gold  |  Finnhub · yfinance · PostgreSQL", style="dim cyan")
    console.print()
    console.print(Panel.fit(
        f"[bold white]Market Feature Store[/]\n[dim cyan]Bronze → Silver → Gold  |  Finnhub · yfinance · PostgreSQL[/]",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()


def ingest_quotes(conn, cur):
    console.print(Rule("[bold cyan]Step 1 / 3 — Live Quotes[/]", style="cyan"))
    console.print()

    quote_rows = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TaskProgressColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Fetching quotes...", total=len(TICKERS))
        for t in TICKERS:
            progress.update(task, description=f"[cyan]Fetching[/] [bold]{t}[/]...")
            row = ingest.grab_quote(t)
            quote_rows.append(row)
            progress.advance(task)

    table = Table(box=box.ROUNDED, border_style="cyan", show_header=True, header_style="bold cyan")
    table.add_column("Ticker", style="bold white", justify="center")
    table.add_column("Price", justify="right", style="green")
    table.add_column("Volume", justify="right", style="yellow")
    table.add_column("Source", justify="center", style="dim")

    for row in quote_rows:
        table.add_row(row[0], f"${row[1]:,.2f}", f"{row[2]:,}", row[4])

    console.print(table)
    console.print()


def ingest_ohlcv(conn, cur):
    console.print(Rule("[bold magenta]Step 2 / 3 — OHLCV History[/]", style="magenta"))
    console.print()

    ohlcv_rows = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TaskProgressColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("Downloading OHLCV...", total=len(TICKERS))
        for t in TICKERS:
            progress.update(task, description=f"[magenta]Downloading[/] [bold]{t}[/]  ({START} → {END})")
            df = ingest.grab_ohlcv(t, START, END)
            ohlcv_rows.append((t, df))
            progress.advance(task)

    table = Table(box=box.ROUNDED, border_style="magenta", show_header=True, header_style="bold magenta")
    table.add_column("Ticker", style="bold white", justify="center")
    table.add_column("Period", justify="center", style="dim")
    table.add_column("Rows", justify="right", style="green")
    table.add_column("Open", justify="right", style="cyan")
    table.add_column("Close", justify="right", style="cyan")

    for t, df in ohlcv_rows:
        first_open  = f"${float(df['Open'].iloc[0]):,.2f}"  if not df.empty else "—"
        last_close  = f"${float(df['Close'].iloc[-1]):,.2f}" if not df.empty else "—"
        table.add_row(t, f"{START} → {END}", str(len(df)), first_open, last_close)

    console.print(table)
    console.print()


def refresh_features():
    console.print(Rule("[bold yellow]Step 3 / 3 — Refreshing Feature Views[/]", style="yellow"))
    console.print()
    with console.status("[yellow]Running features.sql in PostgreSQL...[/]", spinner="dots"):
        result = subprocess.run(["psql", "mfs", "-f", "features.sql"], capture_output=True)
    if result.returncode == 0:
        console.print("  [bold green]✓[/] Feature views refreshed successfully")
    else:
        console.print("  [bold red]✗[/] psql exited with errors")
        console.print(result.stderr.decode(), style="red dim")
    console.print()


def show_gold_sample(cur):
    console.print(Rule("[bold green]Gold Layer Sample — gold.features[/]", style="green"))
    console.print()

    cur.execute("""
        SELECT ticker, dt, close, ret_1d, vol_20d, rsi_14, vol_zscore
        FROM gold.features
        ORDER BY dt DESC, ticker
        LIMIT 9
    """)
    rows = cur.fetchall()

    table = Table(
        box=box.ROUNDED, border_style="green",
        show_header=True, header_style="bold green",
        title="[dim]Latest rows from gold.features[/]",
    )
    table.add_column("Ticker", style="bold white", justify="center")
    table.add_column("Date",   justify="center", style="dim")
    table.add_column("Close",  justify="right",  style="cyan")
    table.add_column("Ret 1D", justify="right")
    table.add_column("Vol 20D", justify="right", style="yellow")
    table.add_column("RSI 14", justify="right")
    table.add_column("Vol Z", justify="right", style="magenta")

    for r in rows:
        ticker, dt, close, ret_1d, vol_20d, rsi_14, vol_zscore = r

        ret_str = f"{float(ret_1d)*100:+.2f}%" if ret_1d is not None else "—"
        ret_color = "green" if ret_1d and ret_1d > 0 else "red"

        rsi_val = float(rsi_14) if rsi_14 is not None else None
        rsi_str = f"{rsi_val:.1f}" if rsi_val is not None else "—"
        rsi_color = "red" if rsi_val and rsi_val > 70 else ("green" if rsi_val and rsi_val < 30 else "white")

        vol_z_val = float(vol_zscore) if vol_zscore is not None else None
        vol_z_str = f"{vol_z_val:+.2f}" if vol_z_val is not None else "—"
        vol_z_color = "red bold" if vol_z_val and abs(vol_z_val) > 2 else "white"

        table.add_row(
            ticker,
            str(dt),
            f"${float(close):,.2f}",
            f"[{ret_color}]{ret_str}[/]",
            f"{float(vol_20d):.4f}" if vol_20d is not None else "—",
            f"[{rsi_color}]{rsi_str}[/]",
            f"[{vol_z_color}]{vol_z_str}[/]",
        )

    console.print(table)
    console.print()


def main():
    print_header()

    conn = psycopg2.connect(os.getenv("DB"))
    cur  = conn.cursor()

    start_time = datetime.now()

    ingest_quotes(conn, cur)
    ingest_ohlcv(conn, cur)
    refresh_features()
    show_gold_sample(cur)

    elapsed = (datetime.now() - start_time).total_seconds()
    console.print(Panel(
        f"[bold green]Pipeline complete[/]  ·  [dim]{elapsed:.1f}s[/]",
        border_style="green",
        padding=(0, 2),
    ))
    console.print()

    conn.close()


if __name__ == "__main__":
    main()
