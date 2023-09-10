import json

import requests
from requests.auth import HTTPBasicAuth

from config import (AIRFLOW_PASSWORD, AIRFLOW_USERNAME, ALPHA_VANTAGE_API_KEY,
                    ALPHA_VANTAGE_QUERY_URL, INIT_SYMBOLS)


AIRFLOW_API_URL = 'http://localhost:8079/api/v1/'


def load_variables_and_connections():

    datas = []

    init_symbols_data = dict(
        key='init_symbols',
        value=json.dumps(INIT_SYMBOLS),
        description='Symbols that will be loaded in initialization mode'
    )
    datas.append((init_symbols_data, 'variables'))

    alpha_vantage_api_key_data = dict(
        key='alpha_vantage_api_key',
        description='API key for authorization in Alpha Vantage',
        value=ALPHA_VANTAGE_API_KEY
    )
    datas.append((alpha_vantage_api_key_data, 'variables'))

    alpha_vantage_query_url_data = dict(
        key='alpha_vantage_query_url',
        description='Alpha Vantage url for sending queries',
        value=ALPHA_VANTAGE_QUERY_URL
    )
    datas.append((alpha_vantage_query_url_data, 'variables'))

    spark_connection_data = {
        "connection_id": "spark_default",
        "conn_type": "spark",
        "host": "localhost",
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
    print('Hello!')
    # load_variables_and_connections()