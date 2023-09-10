from environs import Env
from pathlib import Path

env = Env()
env.read_env()

ALPHA_VANTAGE_API_KEY = env.str('ALPHA_VANTAGE_API_KEY')

ALPHA_VANTAGE_QUERY_URL = 'https://www.alphavantage.co/query'

AIRFLOW_USERNAME = env.str('AIRFLOW_USERNAME')
AIRFLOW_PASSWORD = env.str('AIRFLOW_PASSWORD')

INIT_SYMBOLS = ['TSLA', 'AAPL', 'NVDA', 'AMD', 'GOOG']