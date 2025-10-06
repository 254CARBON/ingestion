-- Bronze Layer: Raw ingested data with minimal processing
-- This layer stores raw data as received from connectors with loose schema

-- Raw market data table (Bronze)
CREATE TABLE IF NOT EXISTS carbon_ingestion.bronze_raw_market_data (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    raw_payload String,
    ingestion_timestamp DateTime DEFAULT now(),
    ingestion_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY ingestion_date
ORDER BY (market, occurred_at, event_id)
TTL ingestion_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Raw trade data table (Bronze)
CREATE TABLE IF NOT EXISTS carbon_ingestion.bronze_raw_trades (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    trade_id String,
    delivery_location String,
    delivery_date Date,
    delivery_hour UInt8,
    price Float64,
    quantity Float64,
    bid_price Float64,
    offer_price Float64,
    clearing_price Float64,
    congestion_price Float64,
    loss_price Float64,
    raw_data String,
    ingestion_timestamp DateTime DEFAULT now(),
    ingestion_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY ingestion_date
ORDER BY (market, delivery_date, delivery_hour, trade_id)
TTL ingestion_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Raw curve data table (Bronze)
CREATE TABLE IF NOT EXISTS carbon_ingestion.bronze_raw_curves (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    curve_type String,
    delivery_date Date,
    delivery_hour UInt8,
    price Float64,
    quantity Float64,
    raw_data String,
    ingestion_timestamp DateTime DEFAULT now(),
    ingestion_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY ingestion_date
ORDER BY (market, curve_type, delivery_date, delivery_hour)
TTL ingestion_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Data quality metrics table (Bronze)
CREATE TABLE IF NOT EXISTS carbon_ingestion.bronze_data_quality (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    quality_metric String,
    metric_value Float64,
    threshold Float64,
    status String,
    details String,
    ingestion_timestamp DateTime DEFAULT now(),
    ingestion_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY ingestion_date
ORDER BY (market, quality_metric, occurred_at)
TTL ingestion_date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
