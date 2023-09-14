CREATE DATABASE IF NOT EXISTS rateshouse ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS rateshouse.prices_and_volumes (
	`symbol_name` String,
	`timeseries_name` String, 
	`timestamp` DateTime64(3, 'US/Eastern'),
    `open` Decimal64(4),
    `high` Decimal64(4),
    `low` Decimal64(4),
    `close` Decimal64(4),
    `volume` UInt32
)
ENGINE = MergeTree()
PRIMARY KEY tuple()
ORDER BY (`symbol_name`, `timestamp`, `timeseries_name`) 
PARTITION BY toYYYYMM("timestamp");