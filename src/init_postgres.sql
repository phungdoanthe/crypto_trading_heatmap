CREATE TABLE IF NOT EXISTS trade_agg_1min (
    window_start TIMESTAMP,
    symbol TEXT,
    total_qty DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    buy_qty DOUBLE PRECISION,
    sell_qty DOUBLE PRECISION,
    PRIMARY KEY (window_start, symbol)
);

CREATE TABLE IF NOT EXISTS trade_events (
    symbol TEXT,
    price DOUBLE PRECISION,
    qty DOUBLE PRECISION,
    side STRING,
    PRIMARY KEY (symbol)
);

CREATE TABLE IF NOT EXISTS ob_agg_1min (
    window_start TIMESTAMP,
    symbol TEXT,
    order_type TEXT,
    price DOUBLE PRECISION,
    total_qty DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    PRIMARY KEY (window_start, symbol)
);

CREATE TABLE IF NOT EXISTS ob_events (
    symbol TEXT,
    price DOUBLE PRECISION,
    qty DOUBLE PRECISION,
    order_type STRING,
    timestamp BIGINT,
    PRIMARY KEY (symbol)
);