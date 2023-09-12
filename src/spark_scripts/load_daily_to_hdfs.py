from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, FloatType, IntegerType, StructField,
                               StructType)

from modules.read_csv import read_rates_csv


def main():
    spark = (
        SparkSession.builder
        .appName('load_daily_data_to_hdfs')
        .getOrCreate()
    )
    
    schema = StructType(
        [
            StructField('date', DateType()),
            StructField('open', FloatType()),
            StructField('high', FloatType()),
            StructField('low', FloatType()),
            StructField('close', FloatType()),
            StructField('volume', IntegerType())
        ]
    )

    read_rates_csv(spark, 'daily', schema)


if __name__ == '__main__':
    main()