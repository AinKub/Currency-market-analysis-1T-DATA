from pathlib import Path
from typing import List
import uuid
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName('load_symbols_and_timeseries_to_ch')
        .getOrCreate()
    )
    
    raw_data_layer = Path(Path.cwd().root) / 'raw_layer_data'
    symbol_folder_paths = [folder for folder in raw_data_layer.iterdir()]
    timeseries = list()
    # Добавим все timeseries, которые есть в папках symbols, в список
    for symbol_folder_path in symbol_folder_paths:
        timeseries.extend([timeseries_folder.name for timeseries_folder in symbol_folder_path.iterdir()])
    
    symbols = [symbol_folder.name for symbol_folder in symbol_folder_paths]
    timeseries = list(set(timeseries))  # уберём дубликаты таймфреймов

    symbol_tuples_with_uuid: List[tuple] = []
    for symbol in symbols:
        symbol_tuples_with_uuid.append((str(uuid.uuid4()), symbol))

    symbols_df = spark.createDataFrame(symbol_tuples_with_uuid, schema=['id', 'name'])

    symbols_df.show()
    # here write to clickhouse

    timeseries_tuples_with_uuid: List[tuple] = []
    for timeseries_name in timeseries:
        timeseries_tuples_with_uuid.append((str(uuid.uuid4()), timeseries_name))

    timeseries_df = spark.createDataFrame(timeseries_tuples_with_uuid, schema=['id', 'name'])

    timeseries_df.show()
    # here write to clickhouse


if __name__ == "__main__":
    main()