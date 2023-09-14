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
    raw_layer_folder = Path('/raw_layer_temp_data')
    destination_path = raw_layer_folder
    for folder in folders:
        destination_path = destination_path / folder

        if not destination_path.exists():
            destination_path.mkdir()

    return destination_path


def load_daily_data_first_time():
    """
    Загружает общие данные по торгам по каждому дню за всё время
    """
    init_symbols = Variable.get('init_symbols')
    alpha_vantage_api_keys = Variable.get('alpha_vantage_api_keys')
    alpha_vantage_query_url = Variable.get('alpha_vantage_query_url')

    alpha_vantage_api_keys = json.loads(alpha_vantage_api_keys)
    init_symbols = json.loads(init_symbols)
    for symbol in init_symbols:
        
        destination_path = _init_raw_layer_folders(symbol, 'daily')
        extract_timeseries_to_temp_data_folder(alpha_vantage_api_keys,
                                               alpha_vantage_query_url,
                                               destination_path,
                                               'daily',
                                               symbol,
                                               daily_outputsize='full',
                                               datatype='csv')
        

def load_all_intraday_data_first_time():
    """
    Загружает данные по торгам внутри дня по каждому дню на протяжении последних
    6 месяцев
    """
    init_symbols = Variable.get('init_symbols')
    alpha_vantage_api_keys = Variable.get('alpha_vantage_api_keys')
    alpha_vantage_query_url = Variable.get('alpha_vantage_query_url')

    alpha_vantage_api_keys = json.loads(alpha_vantage_api_keys)
    init_symbols = json.loads(init_symbols)
    now = datetime.now()
    for symbol in init_symbols:
        destination_path = _init_raw_layer_folders(symbol, '1min')

        try:
            year = now.year
            month_num = now.month
            for i in range(6):

                month = '-'.join([str(year), f'{month_num}'.rjust(2, '0')])
                extract_timeseries_to_temp_data_folder(alpha_vantage_api_keys,
                                                       alpha_vantage_query_url,
                                                       destination_path,
                                                       'intraday',
                                                       symbol,
                                                       '1min',
                                                       intraday_outputsize='full',
                                                       month=month,
                                                       datatype='csv')
                month_num -= 1
                if month_num == 0:
                    year -= 1
                    month_num = 12
                
        except FileNotFoundError:
            continue


with DAG(
    'initial_data_load',
    catchup=False,
    schedule='@once',
    default_args=args
):
    task1_1 = PythonOperator(
        task_id='load_all_daily_data_for_init_symbols',
        python_callable=load_daily_data_first_time
    )

    task1_2 = PythonOperator(
        task_id='load_all_intradays_data_for_init_symbols',
        python_callable=load_all_intraday_data_first_time
    )

    task2= SparkSubmitOperator(
        task_id='load_all_raw_data_to_ch',
        application=str(Path.cwd() / 'spark_scripts' / 'load_all_data_to_ch.py'),
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

    [task1_1, task1_2] >> task2 >> task3