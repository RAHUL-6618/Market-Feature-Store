# Market Feature Store

End-to-end market data pipeline built on PostgreSQL. Ingests live quotes from Finnhub and historical OHLCV from yfinance, engineers quant features entirely in SQL across a Bronze / Silver / Gold architecture. Data contracts defined using Goldman Sachs's open-source Finos Legend.

---

## Architecture
```
Finnhub API          yfinance
(live quotes)        (historical OHLCV)
     │                    │
     └────────┬───────────┘
              │
         ingest.py
              │
     ┌────────▼────────┐
     │  bronze schema  │  raw insert, no transformation
     │  quotes, ohlcv  │
     └────────┬────────┘
              │
     ┌────────▼────────┐
     │  silver schema  │  cleaned, type-validated views
     └────────┬────────┘
              │
     ┌────────▼────────┐
     │   gold schema   │  feature views — all SQL
     │   gold.features │
     └─────────────────┘
```

---

## Features Engineered in SQL

All feature engineering lives in `features.sql` using CTEs and window functions.

| Feature | Description |
|---|---|
| ret_1d / ret_5d / ret_20d | Price returns over 1, 5, 20 days |
| vol_20d | 20-day rolling volatility (std of daily returns) |
| ma_50 / ma_200 | 50 and 200-day moving averages |
| vwap_10 | 10-day volume-weighted average price |
| vol_zscore | Volume z-score over 20-day window |
| rsi_14 | 14-day Relative Strength Index |

---

## Data Contracts — Finos Legend

Data models for `Quote` and `OHLCVBar` are defined in `model/market.pure` using Goldman Sachs's open-source [Finos Legend](https://legend.finos.org) schema language. The Postgres schema in `schema.sql` is derived directly from these model definitions.

---

## Stack

- PostgreSQL 15
- Python 3.12
- Finnhub API
- yfinance
- Finos Legend (Pure)
- psycopg2, python-dotenv

---

## Setup

### 1. Install dependencies
```bash
pip install finnhub-python yfinance psycopg2-binary python-dotenv
```

### 2. Create `.env`
```
FINNHUB_KEY=your_finnhub_api_key
DB=postgresql://your_user@localhost/mfs
```

### 3. Create database and schema
```bash
psql postgres -c "CREATE DATABASE mfs;"
psql mfs -f schema.sql
```

### 4. Run the pipeline
```bash
python run.py
```

---

## Query the Gold Layer
```sql
-- latest features for all tickers
SELECT * FROM gold.features
ORDER BY dt DESC, ticker
LIMIT 20;

-- RSI overbought signals
SELECT ticker, dt, close, rsi_14
FROM gold.features
WHERE rsi_14 > 70
ORDER BY dt DESC;

-- high volume anomalies
SELECT ticker, dt, close, volume, vol_zscore
FROM gold.features
WHERE vol_zscore > 2
ORDER BY vol_zscore DESC;
```

---

## Project Structure
```
market-feature-store/
├── model/
│   └── market.pure      # Finos Legend data contracts
├── schema.sql            # Postgres DDL — bronze/silver/gold schemas
├── ingest.py             # Finnhub + yfinance ingestion
├── features.sql          # Silver + Gold views (all feature engineering)
├── run.py                # Pipeline orchestrator
└── README.md
```

---

## Resume Line

> **Market Feature Store** — PostgreSQL pipeline with Bronze/Silver/Gold schemas; engineered quant features (rolling volatility, RSI, VWAP, momentum) entirely in SQL using CTEs and window functions; ingests live Finnhub quotes and yfinance OHLCV history; data contracts defined using Goldman Sachs's Finos Legend
> *PostgreSQL · SQL · Python · Finnhub · yfinance · Finos Legend*
