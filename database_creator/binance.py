import asyncio
import hashlib
import hmac
import functools
import time
import urllib.parse
from datetime import datetime

import aiohttp
import requests

from constans import INTERVAL_1M, INTERVALS_MILLISECONDS, MAX_CANDLES_CONNECTIONS, CANDLES_DELAY
from exchange import Exchange


class Binance:
    '''
    Класс для работы с Binance API.

    Он предоставляет методы для получения данных о тикетах, свечах и временных метках.
    '''

    SEMAPHORE = asyncio.Semaphore(MAX_CANDLES_CONNECTIONS)

    def __init__(self, api_key, api_secret):
        '''
        Инициализация класса `Binance`.

        :param api_key: Ключ API Binance.
        :param api_secret: Секретный ключ API Binance.
        '''
        self.__API_KEY = api_key
        self.__API_SECRET = api_secret
    
    def delay(func):
        '''
        Декоратор, добавляющий задержку между вызовами функции.

        :param func: Функция, которую нужно обернуть.
        :return: Обернутая функция с задержкой.
        '''
        @functools.wraps(func)
        async def inner(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            end_time = time.time()
            
            time_sleep = CANDLES_DELAY - (end_time - start_time)
            if time_sleep > 0:
                await asyncio.sleep(time_sleep)
            return result
        return inner
    
    @staticmethod
    def error_handler(response):
        '''
        Обработчик ошибок.

        :param response: Ответ от API Binance.
        '''
        print('Error: ', response)
    
    @classmethod
    def get(cls, url):
        '''
        Отправляет GET-запрос к указанному URL и возвращает ответ в формате JSON.

        :param url: URL для GET-запроса.
        :return: Ответ в формате JSON.
        '''
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()

        cls.error_handler(response.json())

    @classmethod
    def get_all_tickets(cls, url):
        '''
        Возвращает список всех тикетов.

        :param url: URL для получения списка тикетов.
        :return: Список тикетов.
        '''
        return [coin['symbol'].lower() for coin in cls.get(url)['symbols']][:5]
    
    @classmethod
    @delay
    async def async_get(cls, url, **params):
        '''
        Асинхронно отправляет GET-запрос к указанному URL с параметрами и возвращает ответ в формате JSON.

        :param url: URL для GET-запроса.
        :param params: Параметры запроса.
        :return: Ответ в формате JSON.
        '''
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                res = await response.json()
                if response.status == 200:
                    return res
                
                cls.error_handler(res)
    
    @classmethod
    async def __get_candles_task(cls, url, **params):
        '''
        Асинхронная задача для получения свечей (candles) для указанного URL и параметров.

        :param url: URL для запроса свечей.
        :param params: Параметры запроса.
        :return: Свечи (candles).
        '''
        async with cls.SEMAPHORE:
            return await cls.async_get(url, **params)

    @classmethod
    async def get_candles(cls, url, **params):
        '''
        Получает свечи (candles) для указанного URL и параметров.

        :param url: URL для запроса свечей.
        :param params: Параметры запроса.
        :return: Свечи (candles).
        '''
        endTime = int(time.time() * 1000) if 'endTime' not in params else params['endTime']
        params_list = []

        while True:
            interval_time = INTERVALS_MILLISECONDS[params['interval']]
            candles_len = int((endTime - params['startTime']) / interval_time)
            params['limit'] = 1000 if candles_len > 1000 else candles_len
            params['symbol'] = params['symbol'].upper()

            if params['limit'] == 0:
                break

            params_list.append(params.copy())

            params['startTime'] += interval_time * 1000

            if params['limit'] != 1000:
                break
        

        tasks = [asyncio.create_task(cls.__get_candles_task(url, **params)) for params in params_list]

        return [
            (
                datetime.fromtimestamp(candl[0] / 1000),
                *(map(float, candl[1:6])),
                datetime.fromtimestamp(candl[6] / 1000)
            )
            for candles in await asyncio.gather(*tasks)
            for candl in candles
        ]

    @classmethod
    async def times(cls, url, tickets):
        '''
        Получает временные метки с начала IPO для указанных тикетов.
        Не больше 83 годам и 4 месяцам назад.

        :param url: URL для запроса временных меток.
        :param tickets: Список тикетов.
        :return: Список временных меток.
        '''
        tasks = [
            asyncio.create_task(cls.__get_candles_task(url, symbol=ticket.upper(), interval=INTERVAL_1M, limit=1000))
            for ticket in tickets
        ]

        return [element[0][0] for element in await asyncio.gather(*tasks)]

    def __use_keys(self, params, headers):
        '''
        Добавляет ключи API и подпись к параметрам и заголовкам запроса.

        :param params: Параметры запроса.
        :param headers: Заголовки запроса.
        '''
        timestamp = int(time.time() * 1000)
        params['timestamp'] = timestamp

        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(self.__API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

        params['signature'] = signature
        headers['X-MBX-APIKEY'] = self.__API_KEY
