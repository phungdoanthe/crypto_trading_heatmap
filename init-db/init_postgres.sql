DROP TABLE IF EXISTS trade_agg_1min;

CREATE TABLE trade_agg_1min (
    window_start    TIMESTAMP,
    symbol          TEXT,

    open_price      DOUBLE PRECISION,
    high_price      DOUBLE PRECISION,
    low_price       DOUBLE PRECISION,
    close_price     DOUBLE PRECISION,

    total_qty       DOUBLE PRECISION,
    vwap            DOUBLE PRECISION,
    buy_qty         DOUBLE PRECISION,
    sell_qty        DOUBLE PRECISION,

    PRIMARY KEY (window_start, symbol)
);

DROP TABLE IF EXISTS ob_agg_1min;

CREATE TABLE IF NOT EXISTS ob_agg_1min (
    window_start    TIMESTAMP,
    symbol          TEXT,
    bid_liquidity   DOUBLE PRECISION,
    ask_liquidity   DOUBLE PRECISION,
    total_liquidity DOUBLE PRECISION,
    imbalance       DOUBLE PRECISION,
    vwap            DOUBLE PRECISION,
    PRIMARY KEY (window_start, symbol)
);