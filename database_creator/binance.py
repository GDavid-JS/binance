import asyncio
import hashlib
import hmac
import functools
import time
import math
import urllib.parse
from datetime import datetime

import aiohttp
import requests
from multipledispatch import dispatch

from constans import INTERVAL_1M, INTERVALS_MILLISECONDS, MAX_CANDLES_CONNECTIONS, CANDLES_DELAY
from exchange import Exchange

def delay(candles_delay):
    def decorator(func):
        '''
        Декоратор, добавляющий задержку между вызовами функции.

        :param candles_delay: Задержка между вызовами функции в секундах.
        :return: Обернутая функция с задержкой.
        '''
        @functools.wraps(func)
        async def inner(*args, **kwargs):
            start_time = time.time()
            result = await func(*args, **kwargs)
            end_time = time.time()
            
            time_sleep = candles_delay - (end_time - start_time)
            if time_sleep > 0:
                await asyncio.sleep(time_sleep)
            return result
        return inner
    return decorator

class Binance(Exchange):
    '''
    Класс для работы с Binance API.

    Он предоставляет методы для получения данных о тикетах, свечах и временных метках.
    '''

    CONNECTIONS = MAX_CANDLES_CONNECTIONS

    def __init__(self, api_key, api_secret, connections=CONNECTIONS):
        '''
        Инициализация класса `Binance`.

        :param api_key: Ключ API Binance.
        :param api_secret: Секретный ключ API Binance.
        '''
        self.__API_KEY = api_key
        self.__API_SECRET = api_secret
        self.__connections = connections
        self.__semaphore = asyncio.Semaphore(self.__connections)
    

    def __del__(self):
        self.CONNECTIONS += self.__connections
    
    @property
    def connections(self):
        '''
        Количество соединений для интерфейса.

        :return: Максимальное количество соединений.
        '''
        return self.__connections
    
    @connections.setter
    def connections(self, value):
        if value < self.COCNNECTIONS:
            self.__connections = value
            self.COCNNECTIONS - value
        else:
            raise "Превышено максимальное количетсво подключений"
    
    @staticmethod
    def error_handler(response):
        '''
        Обработчик ошибок.

        :param response: Ответ от API Binance.
        '''
        print('Error: ', response)

    @classmethod
    def get_all_tickets(cls, url):
        '''
        Возвращает список всех тикетов.

        :param url: URL для получения списка тикетов.
        :return: Список тикетов.
        '''

        return [coin['symbol'].lower() for coin in requests.get(url).json()['symbols'] if coin['isSpotTradingAllowed']][:10]

    async def get(self, url, **params):
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
                
                self.error_handler(res)
    
    @delay(CANDLES_DELAY)
    async def session(self, url, session, **params):
        async with session.get(url, params=params) as response:
            res = await response.json()
            if response.status == 200:
                return res
            
            self.error_handler(res)

    async def __get_candles_task(self, url, session, **params):
        '''
        Асинхронная задача для получения свечей (candles) для указанного URL и параметров.

        :param url: URL для запроса свечей.
        :param params: Параметры запроса.
        :return: Свечи (candles).
        '''
        async with self.__semaphore:
            return await self.session(url, session, **params)

    async def get_candles(self, url, *args):
        '''
        Получает свечи (candles) для указанного URL и параметров.

        :param url: URL для запроса свечей.
        :param params: Параметры запроса.
        :return: Свечи (candles).
        '''

        params_list = self._generate_candle_params(*args)

        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(self.__get_candles_task(url, session, **params)) for params in params_list]

            for candles in asyncio.as_completed(tasks):
                result = [
                    (
                        datetime.fromtimestamp(candl[0] / 1000),
                        *(map(float, candl[1:6])),
                        datetime.fromtimestamp(candl[6] / 1000)
                    )
                    for candl in await candles
                ]

                yield result
    
    def get_candle_interval(self, interval):
        return INTERVALS_MILLISECONDS[interval]
    
    def __get_required_params(self, symbol, interval):
        return {
            'symbol': symbol.upper(),
            'interval': interval,
        }
    
    @dispatch(str, str, int)
    def _generate_candle_params(self, symbol, interval, limit):
        params_list = []
        params = self.__get_required_params(symbol, interval)
        params['limit'] = limit
        params_list.append(params)
        return params_list
    
    @dispatch(str, str, datetime)
    def _generate_candle_params(self, symbol, interval, startTime):
        params_list = []
        params = self.__get_required_params(symbol, interval)
        params['startTime'] =  int(startTime.timestamp()*1000)
        params_list.append(params)
        return params_list
    
    @dispatch(str, str, datetime, int)
    def _generate_candle_params(self, symbol, interval, startTime, limit):
        params_list = []
        params = self.__get_required_params(symbol, interval)
        params['limit'] = limit
        params['startTime'] =  int(startTime.timestamp()*1000)
        params_list.append(params)
        return params_list
    
    @dispatch(str, str, datetime, datetime)
    def _generate_candle_params(self, symbol, interval, startTime, endTime):
        return self._generate_candle_params(symbol, interval, startTime, endTime, 1000)

    @dispatch(str, str, datetime, datetime, int)
    def _generate_candle_params(self, symbol, interval, startTime, endTime, limit):
        params_list = []
        params = self.__get_required_params(symbol, interval)
        params['limit'] = 1000

        startTime = int(startTime.timestamp() * 1000)
        endTime = int(endTime.timestamp() * 1000)
        candle_interval = self.get_candle_interval(interval)
        time_diff = limit * candle_interval

        while endTime > startTime:
            params_copy = params.copy()
            params_copy['startTime'] = startTime
            params_copy['endTime'] = min(startTime + time_diff, endTime)
            params_list.append(params_copy)
            startTime = params_copy['endTime']

        return params_list


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