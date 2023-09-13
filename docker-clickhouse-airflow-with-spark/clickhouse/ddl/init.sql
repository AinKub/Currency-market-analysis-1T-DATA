CREATE DATABASE IF NOT EXISTS rateshouse ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS rateshouse.symbols (
	id UUID,
	"name" String
)
ENGINE = MergeTree()
ORDER BY id;


CREATE TABLE IF NOT EXISTS rateshouse.timeseries (
	id UUID,
	"name" String,
)
ENGINE = MergeTree()
ORDER BY id;


CREATE TABLE IF NOT EXISTS rateshouse.prices_and_volumes (
	symbol_uuid UUID,
	timeseries_uuid UUID, 
	"timestamp" DateTime64(3, 'Europe/Moscow'),
    "open" Decimal64(4),
    high Decimal64(4),
    low Decimal64(4),
    "close" Decimal64(4),
    volume UInt32
)
ENGINE = MergeTree()
PRIMARY KEY tuple()
ORDER BY "timestamp"
PARTITION BY toYYYYMMDD("timestamp");