from typing import Literal, Union
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType


def read_rates_csv(spark: SparkSession,
                   timeseries_to_read: Literal['daily', '1min'],
                   schema: Union[StructType, None] = None
                   ) -> DataFrame:
      
    is_read_first_df = False
    raw_data_layer = Path(Path.cwd().root) / 'raw_layer_data'
    now_folder = raw_data_layer / str(datetime.now().date())

    for symbol_folder in now_folder.iterdir():
        symbol_name = symbol_folder.name

        if is_read_first_df:
             df = df.union(
                 return_df_with_symbol_and_timeseries_name(
                     spark,
                     symbol_folder / timeseries_to_read,
                     symbol_name,
                     timeseries_to_read,
                     schema
                 )
             )
        else:
            df = return_df_with_symbol_and_timeseries_name(
                spark,
                symbol_folder / timeseries_to_read,
                symbol_name,
                timeseries_to_read,
                schema
            )

            is_read_first_df = True

    return df


def return_df_with_symbol_and_timeseries_name(spark: SparkSession,
                                              file_path: Union[Path, str],
                                              symbol_name,
                                              timeseries_name,
                                              schema: Union[StructType, None] = None
                                              ) -> DataFrame:  
    df = (
        spark.read.csv(
            f'file://{file_path}',
            schema=schema,
            header=True
            )
            .withColumns(
                {'symbol_name': lit(symbol_name),
                 'timeseries_name': lit(timeseries_name)}
                 )
        )
    
    return df