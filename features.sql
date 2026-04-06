CREATE OR REPLACE VIEW silver.ohlcv AS
SELECT
    ticker,
    dt,
    open,
    high,
    low,
    close,
    volume
FROM bronze.ohlcv
WHERE close > 0
  AND volume >= 0
  AND open > 0;


CREATE OR REPLACE VIEW gold.features AS
WITH base AS (
    SELECT * FROM silver.ohlcv
),

returns AS (
    SELECT
        ticker,
        dt,
        close,
        volume,
        (close - LAG(close,1)  OVER (PARTITION BY ticker ORDER BY dt)) / LAG(close,1)  OVER (PARTITION BY ticker ORDER BY dt) AS r1,
        (close - LAG(close,5)  OVER (PARTITION BY ticker ORDER BY dt)) / LAG(close,5)  OVER (PARTITION BY ticker ORDER BY dt) AS r5,
        (close - LAG(close,20) OVER (PARTITION BY ticker ORDER BY dt)) / LAG(close,20) OVER (PARTITION BY ticker ORDER BY dt) AS r20
    FROM base
),

vol AS (
    SELECT
        ticker,
        dt,
        STDDEV(r1) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS rolling_vol_20
    FROM returns
),

ma AS (
    SELECT
        ticker,
        dt,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS ma50,
        AVG(close) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS ma200
    FROM base
),

vwap AS (
    SELECT
        ticker,
        dt,
        SUM(close * volume) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) / NULLIF(SUM(volume) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ), 0) AS vwap_10
    FROM base
),

vol_zscore AS (
    SELECT
        ticker,
        dt,
        (volume - AVG(volume) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) / NULLIF(STDDEV(volume) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) AS volume_zscore
    FROM base
),

rsi_calc AS (
    SELECT
        ticker,
        dt,
        r1,
        AVG(CASE WHEN r1 > 0 THEN r1 ELSE 0 END) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        AVG(CASE WHEN r1 < 0 THEN ABS(r1) ELSE 0 END) OVER (
            PARTITION BY ticker ORDER BY dt
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss
    FROM returns
),

rsi AS (
    SELECT
        ticker,
        dt,
        100 - (100 / (1 + NULLIF(avg_gain, 0) / NULLIF(avg_loss, 0))) AS rsi_14
    FROM rsi_calc
)

SELECT
    b.ticker,
    b.dt,
    b.close,
    b.volume,
    r.r1                                    AS ret_1d,
    r.r5                                    AS ret_5d,
    r.r20                                   AS ret_20d,
    ROUND(v.rolling_vol_20::numeric, 6)     AS vol_20d,
    ROUND(m.ma50::numeric, 4)               AS ma_50,
    ROUND(m.ma200::numeric, 4)              AS ma_200,
    ROUND(vw.vwap_10::numeric, 4)           AS vwap_10,
    ROUND(vz.volume_zscore::numeric, 4)     AS vol_zscore,
    ROUND(rs.rsi_14::numeric, 2)            AS rsi_14
FROM base b
JOIN returns    r  ON b.ticker = r.ticker  AND b.dt = r.dt
JOIN vol        v  ON b.ticker = v.ticker  AND b.dt = v.dt
JOIN ma         m  ON b.ticker = m.ticker  AND b.dt = m.dt
JOIN vwap       vw ON b.ticker = vw.ticker AND b.dt = vw.dt
JOIN vol_zscore vz ON b.ticker = vz.ticker AND b.dt = vz.dt
JOIN rsi        rs ON b.ticker = rs.ticker AND b.dt = rs.dt
ORDER BY b.ticker, b.dt;