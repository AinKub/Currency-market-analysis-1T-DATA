import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow_scripts.stock_data_loader import extract_timeseries_to_temp_data_folder


CLICKHOUSE_DRIVER_JAR = '/opt/airflow/spark_scripts/jars/clickhouse-jdbc-0.3.1.jar'
CLICKHOUSE_JDBC = 'jdbc:clickhouse://clickhouse:8123/rateshouse'
CLICKHOUSE_DRIVER = 'com.clickhouse.jdbc.ClickHouseDriver'


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 29)
}


def _init_raw_layer_folders(*folders):
    raw_layer_folder = Path('/raw_layer_data')
    destination_path = raw_layer_folder
    for folder in folders:
        destination_path = destination_path / folder

        if not destination_path.exists():
            destination_path.mkdir()

    return destination_path


def load_last_daily_data():
    """
    Загружает общие данные по торгам по каждому дню за 100 дней
    """
    symbols_to_follow = Variable.get('symbols_to_follow')
    alpha_vantage_api_keys = Variable.get('alpha_vantage_api_keys')
    alpha_vantage_query_url = Variable.get('alpha_vantage_query_url')

    alpha_vantage_api_keys = json.loads(alpha_vantage_api_keys)
    symbols_to_follow = json.loads(symbols_to_follow)
    for symbol in symbols_to_follow:
        
        destination_path = _init_raw_layer_folders(str(datetime.now().date()), symbol, 'daily')
        extract_timeseries_to_temp_data_folder(alpha_vantage_api_keys,
                                               alpha_vantage_query_url,
                                               destination_path,
                                               'daily',
                                               symbol,
                                               daily_outputsize='compact',
                                               datatype='csv')


def load_last_intraday_data():
    """
    Загружает данные по торгам внутри дня (прошедшего)
    """
    symbols_to_follow = Variable.get('symbols_to_follow')
    alpha_vantage_api_keys = Variable.get('alpha_vantage_api_keys')
    alpha_vantage_query_url = Variable.get('alpha_vantage_query_url')

    alpha_vantage_api_keys = json.loads(alpha_vantage_api_keys)
    symbols_to_follow = json.loads(symbols_to_follow)
    for symbol in symbols_to_follow:
        destination_path = _init_raw_layer_folders(str(datetime.now().date()), symbol, '1min')
        extract_timeseries_to_temp_data_folder(alpha_vantage_api_keys,
                                               alpha_vantage_query_url,
                                               destination_path,
                                               'intraday',
                                               symbol,
                                               '1min',
                                               intraday_outputsize='full',
                                               datatype='csv')
        

with DAG(
    'load_delta',
    catchup=False,
    schedule='0 0 * * *',
    default_args=args
):
    task1_1 = PythonOperator(
        task_id='load_last_daily_data',
        python_callable=load_last_daily_data
    )

    task1_2 = PythonOperator(
        task_id='load_last_intraday_data',
        python_callable=load_last_intraday_data
    )

    task2 = SparkSubmitOperator(
        task_id='load_delta_to_ch',
        application=str(Path.cwd() / 'spark_scripts' / 'load_delta_to_ch.py'),
        conn_id='spark_default',
        application_args=[CLICKHOUSE_JDBC, CLICKHOUSE_DRIVER],
        jars=CLICKHOUSE_DRIVER_JAR,
        driver_class_path=CLICKHOUSE_DRIVER_JAR
    )

    task3 = SparkSubmitOperator(
        task_id='analize_data_for_last_day_and_add_it_to_datamart',
        application=str(Path.cwd() / 'spark_scripts' / 'analize_data_for_last_day_and_add_it_to_datamart.py'),
        conn_id='spark_default',
        application_args=[CLICKHOUSE_JDBC, CLICKHOUSE_DRIVER],
        jars=CLICKHOUSE_DRIVER_JAR,
        driver_class_path=CLICKHOUSE_DRIVER_JAR
    )

    [task1_1, task1_2] >> task2# >> task3