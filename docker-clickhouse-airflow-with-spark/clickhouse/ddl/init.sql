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
PARTITION BY toYYYYMMDD("timestamp");

CREATE TABLE IF NOT EXISTS rateshouse.last_day_rates_data_mart (
	`symbol_name` String,
	`daily_volume` UInt32,
	`daily_open` Decimal64(4),
	`daily_close` Decimal64(4),
	`daily_volatility` Decimal64(2),
	`timestamp_with_max_volume` DateTime64(3, 'US/Eastern'),
	`timestamp_with_max_high` DateTime64(3, 'US/Eastern'),
	`timestamp_with_min_low` DateTime64(3, 'US/Eastern'),
)
ENGINE = MergeTree()
PRIMARY KEY tuple()