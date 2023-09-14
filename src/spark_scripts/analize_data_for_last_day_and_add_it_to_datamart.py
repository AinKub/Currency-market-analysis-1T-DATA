import sys

import pendulum
from modules.writing_to_ch import read_from_ch, write_to_ch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, to_date

CLICKHOUSE_JDBC = sys.argv[-2]
CLICKHOUSE_DRIVER = sys.argv[1]


def main():
    spark = (
        SparkSession.builder
        .appName('analize_data_for_last_day_and_add_it_to_datamart')
        .getOrCreate()
    )
    df_prices_and_volumes = read_from_ch(spark, 
                                         CLICKHOUSE_JDBC, 
                                         'rateshouse.prices_and_volumes')
    last_day_df = (
        df_prices_and_volumes
        .filter(
            # Хоть в бд время приведено в часовом поясе US/Eastern, оно конвертируется в, 
            # поэтому идёт сравнение именно с этой временной зоной
            to_date(df_prices_and_volumes.timestamp) == pendulum.now('UTC').subtract(days=1).date()
            )
    )
    daily_df = last_day_df.filter(last_day_df.timeseries_name == 'daily')

    result_df = (
        daily_df
        .select(
            col('symbol_name'), 
            col('volume').alias('daily_volume'), 
            col('open').alias('daily_open'), 
            col('close').alias('daily_close')
        )
        .withColumn('daily_volatility', (100 - (col('daily_open') * 100 / col('daily_close')) ) )
    )

    intraday_df = last_day_df.filter(last_day_df.timeseries_name == '1min')
    max_volumes_df = (
        intraday_df
        .groupBy(col('symbol_name'))
        .agg(
            max(
                col('volume')
                )
            .alias('volume')
            )
    )

    max_volume_timestamp_df = (
        intraday_df.alias('in1')
        .join(
            max_volumes_df, 
            col('in1.volume') == max_volumes_df.volume, 'right'
            )
        .groupBy(col('in1.symbol_name'))
        .agg(min(col('in1.timestamp')).alias('timestamp_with_max_volume'))
    )
    max_high_timestamp_df = (
        intraday_df.alias('in2')
        .join(
            daily_df.alias('day1'), 
            col('in2.high') == col('day1.high'), 'right'
            )
        .groupBy(col('in2.symbol_name'))
        .agg(min(col('in2.timestamp')).alias('timestamp_with_max_high'))
    )
    min_low_timestamp_df = (
        intraday_df.alias('in3')
        .join(
            daily_df.alias('day2'), 
            col('in3.low') == col('day2.low'), 'right'
            )
        .groupBy(col('in3.symbol_name'))
        .agg(min(col('in3.timestamp')).alias('timestamp_with_min_low'))
    )
    
    result_df = (
        result_df.alias('res')
        .join(
            max_volume_timestamp_df.alias('maxv'), 
            col('res.symbol_name') == col('maxv.symbol_name')
        )
        .select(
            col('res.*'), 
            col('maxv.timestamp_with_max_volume')
        )
        .alias('res2')
        .join(
            max_high_timestamp_df.alias('maxh'), 
            col('res2.symbol_name') == col('maxh.symbol_name')
        )
        .select(
            col('res2.*'), 
            col('maxh.timestamp_with_max_high')
        )
        .alias('res3')
        .join(
            min_low_timestamp_df.alias('minl'), 
            col('res3.symbol_name') == col('minl.symbol_name')
        )
        .select(
            col('res3.*'), 
            col('minl.timestamp_with_min_low')
        )
    )
    write_to_ch(result_df, 
                CLICKHOUSE_JDBC, 
                'rateshouse.last_day_rates_data_mart',
                mode='overwrite',
                createTableOptions="engine=MergeTree() ORDER BY tuple()")
    

if __name__ == '__main__':
    main()