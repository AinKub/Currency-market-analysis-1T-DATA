from pyspark.sql import DataFrame


def write_to_ch(df: DataFrame, jdbs: str, table: str):
    (
        df.write
        .format('jdbc')
        .option('url', jdbs)
        .option('dbtable', table)
        .mode('append')
        .save()
    )