from pathlib import Path
from time import sleep
from typing import List, Literal, Union

import requests


def get_timeseries(api_keys: List[str],
                   query_url: str,
                   temporal_resolutions: Literal['daily', 'intraday'],
                   symbol: str,
                   interval: Union[Literal['1min', '5min', '15min', '30min', '60min'], None] = None,
                   intraday_outputsize: Literal['compact', 'full'] = 'full',
                   daily_outputsize: Literal['compact', 'full'] = 'full',
                   month: Union[str, None] = None,
                   datatype: Literal['json', 'csv'] = 'csv',
                   attemps: int = 0) -> str:
    """
    Получает данные через запрос к API. Возвращает полученный файл - content

    Parameters
    ----------
    :param api_keys: API ключи для авторизации по адресу `query_url`

    :param query_url: URL для запросов

    :param temporal_resoluions: `daily` если нужно выбрать общие данные по дням, `intraday`
    если нужно получить данные в определенные таймфреймы, например в 19.50, 19.45, 19.40 и т.д.

    :param symbol: Имя тикера. Всегда пишется в uppercase

    :param interval: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    Указывает, за какие промежутки времени нужно получить данные

    :param intraday_outputsize: `full` возвращает данные за последние 30 дней. Если указан month, то 
    возвращает все данные за этот месяц. `compact` возвращает 100 первых записей

    :param daily_outputsize: В случае получения общих данных за день `full` вернёт абсолютно все даты, 
    когда торговался данный тикер. `compact` ограничивает вывод до 100 записей.

    :param month: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    При его наличии данные собираются только для указанного месяца. Необходимо указывать в формате `YYYY-MM`

    :param datatype: Тип получаемых данных. По умолчанию это csv файл. Обработка json в проекте не предусмотрена

    :param attemps: Количество попыток получить данные с `query_url` используя ключи из списка `api_keys`

    Return
    ------
    :return: Возвращает content, полученный в результате запроса.

    Raise
    -----
    :raise: FileNotFoundError если неправильно указаны параметры для запроса и вместо контента вернулась ошибка

    :raise: PermissionError если превышено количество попыток получить данные (На бесплатном тарифе доступно 
    5 запросов в минуту и 100 запросов в день на один api ключ)
    """

    api_key = api_keys.pop(0)
    api_keys.append(api_key)
    print(f'Use api token: {api_key[:3]+"*"*8}') 

    data = dict(
        function=f'TIME_SERIES_{temporal_resolutions.upper()}',
        symbol=symbol,
        datatype=datatype,
        apikey=api_key
    )
    if temporal_resolutions == 'intraday':
        data['outputsize'] = intraday_outputsize

        data['interval'] = interval
        if month:
            data['month'] = month

    else:
        data['outputsize'] = daily_outputsize   

    with requests.Session() as session:
        timeseries_response = session.get(query_url, params=data)
        decoded_content = timeseries_response.content.decode('utf-8')
        if 'Error Message' in decoded_content:
            raise FileNotFoundError
        elif 'Thank you for using Alpha Vantage!' in decoded_content:
            attemps += 1
            print(f'Attemp {attemps}. Note in decoded_content. Api key: {api_key[:3]+"*"*8}')

            if attemps == len(api_keys):
                print(f'Attemp {attemps} == {len(api_keys)}. Sleep 60 sec. Api key: {api_key[:3]+"*"*8}')
                sleep(90)
            
            elif attemps > len(api_keys):
                print(f'PermissionError: Attemp {attemps} > {len(api_keys)}. Api key: {api_key[:3]+"*"*8}')
                raise PermissionError
            
            return get_timeseries(api_keys,
                                  query_url,
                                  temporal_resolutions,
                                  symbol,
                                  interval,
                                  intraday_outputsize,
                                  daily_outputsize,
                                  month,
                                  datatype,
                                  attemps)
    
    return decoded_content
        

def write_timeseries(timeseries_content: str,
                     destination_folder: Union[Path, str],
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
    
    destination_folder = Path(destination_folder) if not isinstance(destination_folder, Path) else destination_folder
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
        api_keys: List[str],
        query_url: str,
        destination_folder: Union[Path, str],
        temporal_resolutions: Literal['daily', 'intraday'],
        symbol: str,
        interval: Union[Literal['1min', '5min', '15min', '30min', '60min'], None] = None,
        intraday_outputsize: Literal['compact', 'full'] = 'full',
        daily_outputsize: Literal['compact', 'full'] = 'full',
        month: Union[str, None] = None,
        datatype: Literal['json', 'csv'] = 'csv'
    ) -> None:
    """
    Получает данные через запрос к API и записывает полученный контент в формат, 
    указанный в `datatype`. По умолчанию записывает в .csv.

    Parameters
    ----------
    :param api_key: API ключи для авторизации по адресу `query_url`

    :param query_url: URL для запросов

    :param temporal_resoluions: `daily` если нужно выбрать и сохранить общие данные по дням, `intraday`
    если нужно получить данные и сохранить в определенные таймфреймы, например в 19.50, 19.45, 19.40 и т.д.

    :param symbol: Имя тикера. Всегда пишется в uppercase

    :param interval: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    Указывает, за какие промежутки времени нужно получить данные. Также добавляется к имени выходного файла

    :param intraday_outputsize: `full` возвращает данные за последние 30 дней. Если указан month, то 
    возвращает все данные за этот месяц. `compact` возвращает 100 первых записей

    :param daily_outputsize: В случае получения общих данных за день `full` вернёт абсолютно все даты, 
    когда торговался данный тикер. `compact` ограничивает вывод до 100 записей.

    :param month: Опциональный параметр. Используется при указании `temporal_resoluions` = 'intraday'.
    При его наличии данные собираются только для указанного месяца. Необходимо указывать в формате `YYYY-MM`.
    Также добавляется к имени выходного файла

    :param datatype: Тип получаемых данных а также расширение выходного файла. По умолчанию это csv файл. 
    Обработка json в проекте не предусмотрена

    Raise
    -----
    :raise: FileNotFoundError если неправильно указаны параметры для запроса и вместо контента вернулась ошибка
    :raise: ValueError если указан `datatype` = json
    :raise: PermissionError если превышено количество попыток получить данные (На бесплатном тарифе доступно 
    5 запросов в минуту и 100 запросов в день на один api ключ)
    """
    timeseries_content = get_timeseries(api_keys, 
                                        query_url, 
                                        temporal_resolutions, 
                                        symbol, 
                                        interval,
                                        intraday_outputsize,
                                        daily_outputsize, 
                                        month, 
                                        datatype)
    
    write_timeseries(timeseries_content, destination_folder, 
                     temporal_resolutions, symbol, interval, 
                     month, datatype)


# now = datetime.now()

# for month_num in range(now.month, now.month-4, -1):
#     month = '-'.join([str(now.year), f'{month_num}'.rjust(2, '0')])
#     extract_timeseries_to_temp_data_folder('intraday', 'TSLA', '5min', month=month)

# extract_timeseries_to_temp_data_folder('daily', 'TSLA')
