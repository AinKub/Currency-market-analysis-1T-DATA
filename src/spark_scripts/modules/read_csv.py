from typing import Literal, Union
import uuid
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType


def read_rates_csv(spark: SparkSession,
                   timeseries_to_read: Literal['daily', '1min'],
                   schema: Union[StructType, None] = None):
      
    is_read_first_df = False
    raw_data_layer = Path(Path.cwd().root) / 'raw_layer_data'

    for symbol_folder in raw_data_layer.iterdir():
        symbol_name = symbol_folder.name

        if is_read_first_df:
             df = df.union(
                 return_df_with_name_timeseriese_and_uuids(
                     spark,
                     symbol_folder / timeseries_to_read,
                     symbol_name,
                     timeseries_to_read,
                     schema
                 )
             )
        else:
            df = return_df_with_name_timeseriese_and_uuids(
                spark,
                symbol_folder / timeseries_to_read,
                symbol_name,
                timeseries_to_read,
                schema
            )

            is_read_first_df = True

    df.show()
    df.printSchema()


def return_df_with_name_timeseriese_and_uuids(spark: SparkSession,
                                              file_path: Union[Path, str],
                                              symbol_name,
                                              timeseries_name,
                                              schema: Union[StructType, None] = None
                                              ) -> DataFrame:
    symbol_uuid = str(uuid.uuid4())
    timeseries_uuid = str(uuid.uuid4())
    prices_and_volumes_uuid = str(uuid.uuid4())
    
    df = (
        spark.read.csv(
            f'file://{file_path}',
            schema=schema,
            header=True
            )
            .withColumns(
                {'prices_and_volumes_uuid': lit(prices_and_volumes_uuid),
                 'symbol_name': lit(symbol_name),
                 'symbol_uuid': lit(symbol_uuid),
                 'timeseries_name': lit(timeseries_name),
                 'timeseries_uuid': lit(timeseries_uuid)}
                 )
        ).limit(5)
    
    return df