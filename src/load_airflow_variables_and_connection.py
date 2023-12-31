import json

import requests
from requests.auth import HTTPBasicAuth

from config import (AIRFLOW_PASSWORD, AIRFLOW_USERNAME, ALPHA_VANTAGE_API_KEYS,
                    ALPHA_VANTAGE_QUERY_URL, INIT_SYMBOLS)


AIRFLOW_API_URL = 'http://localhost:8080/api/v1/'


def load_variables_and_connections():

    datas = []

    init_symbols_data = dict(
        key='init_symbols',
        value=json.dumps(INIT_SYMBOLS),
        description='Symbols that will be loaded in initialization mode'
    )
    datas.append((init_symbols_data, 'variables'))

    alpha_vantage_api_keys_data = dict(
        key='alpha_vantage_api_keys',
        description='API keys for authorization in Alpha Vantage',
        value=json.dumps(ALPHA_VANTAGE_API_KEYS)
    )
    datas.append((alpha_vantage_api_keys_data, 'variables'))

    alpha_vantage_query_url_data = dict(
        key='alpha_vantage_query_url',
        description='Alpha Vantage url for sending queries',
        value=ALPHA_VANTAGE_QUERY_URL
    )
    datas.append((alpha_vantage_query_url_data, 'variables'))

    spark_connection_data = {
        "connection_id": "spark_default",
        "conn_type": "spark",
        "host": "local[4]",
        "extra": json.dumps({"queue": "root.default"})
    }
    datas.append((spark_connection_data, 'connections'))

    auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)

    with requests.Session() as session:
        for data, type in datas:
            responce = session.post(AIRFLOW_API_URL + type, 
                                    json=data,
                                    auth=auth)
            data_name = data['key'] if type == 'variables' else data['connection_id']
            if responce.status_code == 200:
                print(f'{data_name} added succesfully!')
            else:
                print(f'Error adding {data_name}: {responce.status_code}\n{responce.json()}')


if __name__ == '__main__':
    load_variables_and_connections()