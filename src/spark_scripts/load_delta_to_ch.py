import sys

import pendulum
from modules.read_csv import read_rates_csv
from modules.writing_to_ch import write_to_ch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (FloatType, IntegerType, StructField, StructType,
                               TimestampType)

CLICKHOUSE_JDBC = sys.argv[-2]
CLICKHOUSE_DRIVER = sys.argv[1]


def main():
    """
    Читает данные за 1 минуту и день, объединяет их и пишет в clickhouse
    """
    spark = (
        SparkSession.builder
        .appName('load_delta_to_ch')
        .getOrCreate()
    )
    
    schema = StructType(
        [
            StructField('timestamp', TimestampType()),
            StructField('open', FloatType()),
            StructField('high', FloatType()),
            StructField('low', FloatType()),
            StructField('close', FloatType()),
            StructField('volume', IntegerType())
        ]
    )

    one_min_df = read_rates_csv(spark, '1min', schema)
    daily_df = read_rates_csv(spark, 'daily', schema)
    result_df = one_min_df.union(daily_df).filter(
        to_date(col('timestamp')) == pendulum.now('US/Eastern').subtract(days=1).date()
    )
    write_to_ch(result_df, CLICKHOUSE_JDBC, 'rateshouse.prices_and_volumes')


if __name__ == '__main__':
    main()