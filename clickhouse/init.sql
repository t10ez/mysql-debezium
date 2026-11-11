-- Target for mysql1 CDC. ReplacingMergeTree keeps the latest version per id;
-- rows with is_deleted = 1 are hidden by FINAL queries.
--   _version   = source.ts_us (commit time on mysql1, microseconds)
--   is_deleted = 1 when Debezium rewrote a DELETE (__deleted = 'true')
-- Query with: SELECT ... FROM inventory.customers FINAL

CREATE DATABASE IF NOT EXISTS inventory;

CREATE TABLE inventory.customers
(
    id             Int32,
    first_name     String,
    last_name      String,
    email          String,
    created_at     Nullable(String),
    __op           LowCardinality(String),
    __source_ts_us Int64,
    __deleted      LowCardinality(String),
    _version       UInt64 MATERIALIZED toUInt64(__source_ts_us),
    is_deleted     UInt8  MATERIALIZED __deleted = 'true',
    synced_at      DateTime64(3) MATERIALIZED now64(3)
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY id;

CREATE TABLE inventory.orders
(
    id             Int32,
    customer_id    Int32,
    amount         Decimal(10, 2),
    status         String,
    __op           LowCardinality(String),
    __source_ts_us Int64,
    __deleted      LowCardinality(String),
    _version       UInt64 MATERIALIZED toUInt64(__source_ts_us),
    is_deleted     UInt8  MATERIALIZED __deleted = 'true',
    synced_at      DateTime64(3) MATERIALIZED now64(3)
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY id;
