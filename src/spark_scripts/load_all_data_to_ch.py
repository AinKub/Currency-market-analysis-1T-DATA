import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (TimestampType, FloatType, IntegerType, StructField,
                               StructType)
from pyspark.sql.functions import to_date, col, lit

from modules.read_csv import read_rates_csv
from modules.writing_to_ch import write_to_ch

CLICKHOUSE_JDBC = sys.argv[-2]
CLICKHOUSE_DRIVER = sys.argv[1]


def main():
    """
    Читает данные за 1 минуту и день, объединяет их и пишет в clickhouse
    """
    spark = (
        SparkSession.builder
        .appName('load_daily_data_to_hdfs')
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
    min_timestamp_row = (
        one_min_df.select('timestamp')
                  .sort(one_min_df.timestamp.asc())
                  .head()
    )
    min_date = min_timestamp_row.timestamp.date()

    daily_df = read_rates_csv(spark, 'daily', schema)
    filtered_daily_df_by_date = daily_df.filter(to_date(col('timestamp')) >= lit(min_date))

    result_df = one_min_df.union(filtered_daily_df_by_date)
    # result_df = result_df.sort(result_df.timestamp.desc(), result_df.symbol_name.asc())
    write_to_ch(result_df, CLICKHOUSE_JDBC, 'rateshouse.prices_and_volumes')

if __name__ == '__main__':
    main()