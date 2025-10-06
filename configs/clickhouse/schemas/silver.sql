-- Silver Layer: Normalized and validated data
-- This layer stores cleaned, typed, and canonical data

-- Normalized market data table (Silver)
CREATE TABLE IF NOT EXISTS carbon_ingestion.silver_normalized_market_data (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    market_id String,
    timezone String,
    currency String,
    unit String,
    price_unit String,
    normalized_payload String,
    validation_status String,
    validation_errors Array(String),
    normalization_timestamp DateTime DEFAULT now(),
    normalization_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY normalization_date
ORDER BY (market, occurred_at, event_id)
TTL normalization_date + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

-- Normalized trade data table (Silver)
CREATE TABLE IF NOT EXISTS carbon_ingestion.silver_normalized_trades (
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
    validation_status String,
    validation_errors Array(String),
    normalization_timestamp DateTime DEFAULT now(),
    normalization_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY normalization_date
ORDER BY (market, delivery_date, delivery_hour, trade_id)
TTL normalization_date + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

-- Normalized curve data table (Silver)
CREATE TABLE IF NOT EXISTS carbon_ingestion.silver_normalized_curves (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    market_id String,
    curve_type String,
    delivery_date Date,
    delivery_hour UInt8,
    delivery_datetime DateTime,
    price Float64,
    quantity Float64,
    price_currency String,
    quantity_unit String,
    validation_status String,
    validation_errors Array(String),
    normalization_timestamp DateTime DEFAULT now(),
    normalization_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY normalization_date
ORDER BY (market, curve_type, delivery_date, delivery_hour)
TTL normalization_date + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

-- Data quality summary table (Silver)
CREATE TABLE IF NOT EXISTS carbon_ingestion.silver_data_quality_summary (
    event_id String,
    trace_id String,
    occurred_at UInt64,
    tenant_id String,
    schema_version String,
    producer String,
    market String,
    quality_score Float64,
    completeness_score Float64,
    accuracy_score Float64,
    consistency_score Float64,
    timeliness_score Float64,
    validity_score Float64,
    total_records UInt64,
    valid_records UInt64,
    invalid_records UInt64,
    quality_details String,
    normalization_timestamp DateTime DEFAULT now(),
    normalization_date Date DEFAULT toDate(now())
) ENGINE = MergeTree()
PARTITION BY normalization_date
ORDER BY (market, occurred_at)
TTL normalization_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
