import asyncio
from datetime import datetime

from multipledispatch import dispatch

from constans import INTERVALS_UNIQUE, INTERVALS, MAX_CANDLES_CONNECTIONS, INTERVALS_MILLISECONDS
from binance import Binance
from exchange import Exchange

class TemplateBinanceInterface(Binance, Exchange):
    '''
    Класс для создания интерфейса Binance, представляющего шаблон для других типов интерфейсов.

    Он предоставляет базовые методы и свойства для работы с данными Binance.
    '''

    def __init__(self, *args, **kwargs):
        '''
        Инициализация класса `TemplateBinanceInterface`.

        :param args: Позиционные аргументы для базовых классов.
        :param kwargs: Именованные аргументы для базовых классов.
        '''
        super().__init__(*args, **kwargs)
        self._tickets = Binance.get_all_tickets(f'{self._MAIN_URL}exchangeInfo')
        self._schemas = [f'{self._TYPE}_{ticket}' for ticket in self._tickets]
        self._tables = [f'interval_{interval}' for interval in INTERVALS_UNIQUE]

    @property
    def max_connections(self):
        '''
        Максимальное количество соединений для всех интерфейсов.

        :return: Максимальное количество соединений.
        '''
        return MAX_CANDLES_CONNECTIONS

    @property
    def tickets(self):
        '''
        Список тикетов.

        :return: Список тикетов.
        '''
        return self._tickets
    
    @property
    def schemas(self):
        '''
        Список схем.

        :return: Список схем.
        '''
        return self._schemas
    
    @property
    def tables(self):
        '''
        Список таблиц.

        :return: Список таблиц.
        '''
        return self._tables
    
    @property
    def intervals(self):
        '''
        Список интервалов.

        :return: Список интервалов.
        '''
        return INTERVALS
    
    async def get_candles(self, *args):
        '''
        Получает свечи (candles) для указанного тикета, интервала и временной метки.

        :param ticket: Тикет.
        :param interval: Интервал.
        :param time: Временная метка.
        :return: Свечи (candles).
        '''
        async for candles in super().get_candles(f'{self._MAIN_URL}klines', *args):
            yield candles

        # return await super().get_candles(f'{self._MAIN_URL}klines', *args)


    async def get_first_candle_time(self, ticket, interval):
        async for candles in super().get_candles(f'{self._MAIN_URL}klines', ticket, interval, datetime.fromtimestamp(0), 1):
            return candles[0][0]
    
    def __iter__(self):
        return zip(self.tickets, self.schemas)


class Spot(TemplateBinanceInterface):
    '''
    Класс для создания интерфейса Spot Binance.

    Интерфейс предоставляет методы и свойства для работы с данными Spot Binance.
    '''

    _TYPE = 'spot'
    _API = 'api'
    _VERSION = 'v3'
    _BASE_URL = f'https://{_API}.binance.com/'
    _MAIN_URL = f'{_BASE_URL}{_API}/{_VERSION}/'

class Future(TemplateBinanceInterface):
    '''
    Класс для создания интерфейса Future Binance.

    Интерфейс предоставляет методы и свойства для работы с данными Future Binance.
    '''

    _TYPE = 'future'
    _API = 'fapi'
    _VERSION = 'v1'
    _BASE_URL = f'https://{_API}.binance.com/'
    _MAIN_URL = f'{_BASE_URL}{_API}/{_VERSION}/'
