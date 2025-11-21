{{ config(
    materialized='table',
    unique_key=['symbol', 'trade_date'],
    tags=['daily', 'analytics']
) }}

WITH base AS (
    SELECT
        trade_date,
        symbol,
        close,
        close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) AS price_change
    FROM {{ source('raw', 'stock_prices') }}
    WHERE close IS NOT NULL
),

gains_losses AS (
    SELECT
        *,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM base
),

ma AS (
    SELECT
        *,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma_20,
        close - LAG(close, 10) OVER (PARTITION BY symbol ORDER BY trade_date) AS price_momentum
    FROM base
),

rsi_base AS (
    SELECT
        trade_date,
        symbol,
        close,
        AVG(gain) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        AVG(loss) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss
    FROM gains_losses
    WHERE trade_date IS NOT NULL
),

final_rsi AS (
    SELECT
        trade_date,
        symbol,
        close,
        CASE
            WHEN avg_loss = 0 THEN 100.0
            ELSE 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))
        END AS rsi_14
    FROM rsi_base
)

SELECT
    t1.trade_date,
    t1.symbol,
    t1.close,
    t2.ma_20,
    t2.price_momentum,
    t1.rsi_14,
    CURRENT_TIMESTAMP() AS load_ts
FROM final_rsi t1
INNER JOIN ma t2 ON t1.symbol = t2.symbol AND t1.trade_date = t2.trade_date
ORDER BY symbol, trade_date
