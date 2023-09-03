from environs import Env
from pathlib import Path

env = Env()
env.read_env()

ALPHA_VANTAGE_API_KEY = env.str('ALPHA_VANTAGE_API_KEY')

ALPHA_VANTAGE_QUERY_URL = 'https://www.alphavantage.co/query'