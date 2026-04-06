CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE bronze.quotes (
    ticker    VARCHAR(10)    NOT NULL,
    price     DECIMAL(12,4)  NOT NULL,
    volume    INT            NOT NULL,
    ts        TIMESTAMP      NOT NULL,
    source    VARCHAR(20)    NOT NULL,
    loaded_at TIMESTAMP      DEFAULT NOW()
);

CREATE TABLE bronze.ohlcv (
    ticker    VARCHAR(10)    NOT NULL,
    open      DECIMAL(12,4)  NOT NULL,
    high      DECIMAL(12,4)  NOT NULL,
    low       DECIMAL(12,4)  NOT NULL,
    close     DECIMAL(12,4)  NOT NULL,
    volume    INT            NOT NULL,
    dt        DATE           NOT NULL,
    loaded_at TIMESTAMP      DEFAULT NOW(),
    UNIQUE(ticker, dt)
);