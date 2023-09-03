from pathlib import Path
from typing import Literal, Union
from datetime import datetime

import requests

from config import ALPHA_VANTAGE_API_KEY, ALPHA_VANTAGE_QUERY_URL


def get_timeseries(temporal_resolutions: Literal['daily', 'intraday'],
                   symbol: str,
                   interval: Union[Literal['1min', '5min', '15min', '30min', '60min'], None] = None,
                   month: Union[str, None] = None,
                   datatype: Literal['json', 'csv'] = 'csv') -> str:
    """
    Получает данные через запрос к API. Возвращает полученный файл - content

    Parameters
    ----------
    :param temporal_resoluions: `daily` если нужно выбрать общие данные по дням, `intraday`
    если нужно получить данные в определенные таймфреймы, например в 19.50, 19.45, 19.40 и т.д.

    :param symbol: Имя тикера. Всегда пишется в uppercase

    :param interval: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    Указывает, за какие промежутки времени нужно получить данные

    :param month: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    При его наличии данные собираются только для указанного месяца. Необходимо указывать в формате `YYYY-MM`

    :param datatype: Тип получаемых данных. По умолчанию это csv файл. Обработка json в проекте не предусмотрена

    Return
    ------
    :return: Возвращает content, полученный в результате запроса.

    Raise
    -----
    :raise: FileNotFoundError если неправильно указаны параметры для запроса и вместо контента вернулась ошибка
    """

    data = dict(
        function=f'TIME_SERIES_{temporal_resolutions.upper()}',
        symbol=symbol,
        datatype=datatype,
        apikey=ALPHA_VANTAGE_API_KEY
    )
    if temporal_resolutions == 'intraday':
        data['outputsize'] = 'full'   # full возвращает данные за последние 30 дней.
                                      # Если указан month, то возвращает все данные за этот месяц

        data['interval'] = interval
        if month:
            data['month'] = month

    else:
        data['outputsize'] = 'compact'   # В случае получения общих данных за день full вернёт
                                         # абсолютно все даты, когда торговался данный тикер.
                                         # Compact ограничивает вывод до 100 записей.

    with requests.Session() as session:
        timeseries_response = session.get(ALPHA_VANTAGE_QUERY_URL, params=data)
        decoded_content = timeseries_response.content.decode('utf-8')
        if 'Error Message' in decoded_content:
            raise FileNotFoundError
    
    return decoded_content
        

def write_timeseries(timeseries_content: str,
                     temporal_resolutions: Literal['daily', 'intraday'],
                     symbol: str,
                     interval: Union[Literal['1min', '5min', '15min', '30min', '60min'], None] = None,
                     month: Union[str, None] = None,
                     datatype: Literal['json', 'csv'] = 'csv') -> None:
    """
    Записывает полученный контент в формат, указанный в `datatype`. По умолчанию записывает в .csv.

    Parameters
    ----------
    :param temporal_resoluions: Добавляет к названию файла тип данных - общие за прошедшие дни или 
    в определенные таймфреймы, например в 19.50, 19.45, 19.40 и т.д.

    :param symbol: Имя тикера. Всегда пишется в uppercase

    :param interval: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    Добавляется к имени файла.

    :param month: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    Добавляется к имени файла. Необходимо указывать в формате `YYYY-MM`

    :param datatype: Расширение итогового файла. По умолчанию это csv файл. Обработка json в проекте не предусмотрена

    Raise
    -----
    :raise: ValueError если указан `datatype` = json
    """

    if datatype == 'json':
        print('Не реализовано для json')
        raise ValueError
    
    destination_folder = Path.cwd() / 'temp_data'
    if not destination_folder.exists():
        destination_folder.mkdir()

    filename = '{month}{temporal_resolutions}{interval}{symbol}.{datatype}'.format(
        month=f'{month}_' if month else '',
        temporal_resolutions=temporal_resolutions,
        interval=f'_{interval}_' if interval else '_',
        symbol=symbol,
        datatype=datatype
    )
    destination_file = destination_folder / filename
    with open(destination_file, mode='w', encoding='utf-8', newline='') as csv_file:
        csv_file.write(timeseries_content)
    

def extract_timeseries_to_temp_data_folder(
        temporal_resolutions: Literal['daily', 'intraday'],
        symbol: str,
        interval: Union[Literal['1min', '5min', '15min', '30min', '60min'], None] = None,
        month: Union[str, None] = None,
        datatype: Literal['json', 'csv'] = 'csv'
    ) -> None:
    """
    Получает данные через запрос к API и записывает полученный контент в формат, 
    указанный в `datatype`. По умолчанию записывает в .csv.

    Parameters
    ----------
    :param temporal_resoluions: `daily` если нужно выбрать и сохранить общие данные по дням, `intraday`
    если нужно получить данные и сохранить в определенные таймфреймы, например в 19.50, 19.45, 19.40 и т.д.

    :param symbol: Имя тикера. Всегда пишется в uppercase

    :param interval: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    Указывает, за какие промежутки времени нужно получить данные. Также добавляется к имени выходного файла

    :param month: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    При его наличии данные собираются только для указанного месяца. Необходимо указывать в формате `YYYY-MM`.
    Также добавляется к имени выходного файла

    :param datatype: Тип получаемых данных а также расширение выходного файла. По умолчанию это csv файл. 
    Обработка json в проекте не предусмотрена

    Raise
    -----
    :raise: FileNotFoundError если неправильно указаны параметры для запроса и вместо контента вернулась ошибка
    :raise: ValueError если указан `datatype` = json
    """
    timeseries_content = get_timeseries(temporal_resolutions, symbol, interval, month, datatype)
    write_timeseries(timeseries_content, temporal_resolutions, symbol, interval, month, datatype)


now = datetime.now()

for month_num in range(now.month, now.month-4, -1):
    month = '-'.join([str(now.year), f'{month_num}'.rjust(2, '0')])
    extract_timeseries_to_temp_data_folder('intraday', 'TSLA', '5min', month=month)

extract_timeseries_to_temp_data_folder('daily', 'TSLA')
