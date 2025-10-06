-- Gold Layer: Enriched and aggregated data
-- This layer stores business-ready data with derived metrics and aggregations

-- Enriched market data table (Gold)
CREATE TABLE IF NOT EXISTS carbon_ingestion.gold_enriched_market_data (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    market_id String,
    enriched_payload String,
    taxonomy_tags Array(String),
    semantic_tags Array(String),
    geospatial_data String,
    metadata_tags Map(String, String),
    enrichment_score Float64,
    enrichment_timestamp DateTime DEFAULT now(),
    enrichment_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY enrichment_date
ORDER BY (market, occurred_at, event_id)
TTL enrichment_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Enriched trade data table (Gold)
CREATE TABLE IF NOT EXISTS carbon_ingestion.gold_enriched_trades (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    market_id String,
    trade_id String,
    delivery_location String,
    delivery_date Date,
    delivery_hour UInt8,
    delivery_datetime DateTime,
    price Float64,
    quantity Float64,
    bid_price Float64,
    offer_price Float64,
    clearing_price Float64,
    congestion_price Float64,
    loss_price Float64,
    price_currency String,
    quantity_unit String,
    taxonomy_tags Array(String),
    semantic_tags Array(String),
    geospatial_data String,
    metadata_tags Map(String, String),
    enrichment_score Float64,
    enrichment_timestamp DateTime DEFAULT now(),
    enrichment_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY enrichment_date
ORDER BY (market, delivery_date, delivery_hour, trade_id)
TTL enrichment_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- OHLC bars table (Gold)
CREATE TABLE IF NOT EXISTS carbon_ingestion.gold_ohlc_bars (
    market String,
    delivery_location String,
    delivery_date Date,
    bar_type String,
    bar_size String,
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    vwap Float64,
    trade_count UInt64,
    price_currency String,
    quantity_unit String,
    aggregation_timestamp DateTime DEFAULT now(),
    aggregation_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY aggregation_date
ORDER BY (market, delivery_location, delivery_date, bar_type, bar_size)
TTL aggregation_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Rolling metrics table (Gold)
CREATE TABLE IF NOT EXISTS carbon_ingestion.gold_rolling_metrics (
    market String,
    delivery_location String,
    delivery_date Date,
    metric_type String,
    window_size String,
    metric_value Float64,
    metric_unit String,
    sample_count UInt64,
    calculation_timestamp DateTime DEFAULT now(),
    calculation_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY calculation_date
ORDER BY (market, delivery_location, delivery_date, metric_type, window_size)
TTL calculation_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Curve pre-stage table (Gold)
CREATE TABLE IF NOT EXISTS carbon_ingestion.gold_curve_prestage (
    market String,
    curve_type String,
    delivery_date Date,
    delivery_hour UInt8,
    price Float64,
    quantity Float64,
    price_currency String,
    quantity_unit String,
    curve_metadata Map(String, String),
    aggregation_timestamp DateTime DEFAULT now(),
    aggregation_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY aggregation_date
ORDER BY (market, curve_type, delivery_date, delivery_hour)
TTL aggregation_date + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- Materialized view for daily OHLC bars
CREATE MATERIALIZED VIEW IF NOT EXISTS carbon_ingestion.mv_daily_ohlc_bars
TO carbon_ingestion.gold_ohlc_bars
AS SELECT
    market,
    delivery_location,
    delivery_date,
    'daily' as bar_type,
    '1D' as bar_size,
    min(price) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    max(price) as close_price,
    sum(quantity) as volume,
    sum(price * quantity) / sum(quantity) as vwap,
    count() as trade_count,
    price_currency,
    quantity_unit,
    now() as aggregation_timestamp,
    toDate(now()) as aggregation_date
FROM carbon_ingestion.silver_normalized_trades
WHERE validation_status = 'valid'
GROUP BY market, delivery_location, delivery_date, price_currency, quantity_unit;

-- Materialized view for hourly OHLC bars
CREATE MATERIALIZED VIEW IF NOT EXISTS carbon_ingestion.mv_hourly_ohlc_bars
TO carbon_ingestion.gold_ohlc_bars
AS SELECT
    market,
    delivery_location,
    delivery_date,
    'hourly' as bar_type,
    '1H' as bar_size,
    min(price) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    max(price) as close_price,
    sum(quantity) as volume,
    sum(price * quantity) / sum(quantity) as vwap,
    count() as trade_count,
    price_currency,
    quantity_unit,
    now() as aggregation_timestamp,
    toDate(now()) as aggregation_date
FROM carbon_ingestion.silver_normalized_trades
WHERE validation_status = 'valid'
GROUP BY market, delivery_location, delivery_date, delivery_hour, price_currency, quantity_unit;
