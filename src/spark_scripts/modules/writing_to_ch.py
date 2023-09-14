from typing import Literal
from pyspark.sql import DataFrame, SparkSession


def write_to_ch(df: DataFrame, 
                jdbs: str, 
                table: str, 
                mode: Literal['append', 'overwrite'] = 'append',
                **options):
    
    df_writer = (
        df.write
        .format('jdbc')
        .option('url', jdbs)
        .option('dbtable', table)
        .mode(mode)
    )
    if options:
        for key, value in options.items():
            df_writer = df_writer.option(key, value)

    df_writer.save()


def read_from_ch(spark: SparkSession, jdbs: str, table: str):
    return (
        spark.read
        .format('jdbc')
        .option('url', jdbs)
        .option('dbtable', table)
        .load()
    )