from constans import INTERVALS_UNIQUE, INTERVALS, MAX_CANDLES_CONNECTIONS
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
        self.common_data = [
            {
                'ticket': ticket,
                'shema': f'{self._TYPE}_{ticket}',
                'time': None
            }
            for ticket in Binance.get_all_tickets(f'{self._MAIN_URL}exchangeInfo')
        ]

    @property
    def max_connections(self):
        '''
        Максимальное количество соединений для интерфейса.

        :return: Максимальное количество соединений.
        '''
        return MAX_CANDLES_CONNECTIONS
    
    @property
    def tickets(self):
        '''
        Список тикетов.

        :return: Список тикетов.
        '''
        return [data['ticket'] for data in self.common_data]
    
    @property
    def schemas(self):
        '''
        Список схем.

        :return: Список схем.
        '''
        return [data['shema'] for data in self.common_data]
    
    @property
    def times(self):
        '''
        Список временных меток с момента когда надо заполнить базу данных.

        :return: Список временных меток.
        '''
        return [data['time'] for data in self.common_data]
    
    @property
    def tables(self):
        '''
        Список таблиц.

        :return: Список таблиц.
        '''
        return [f'interval_{interval}' for interval in INTERVALS_UNIQUE]
    
    @property
    def intervals(self):
        '''
        Список интервалов.

        :return: Список интервалов.
        '''
        return INTERVALS
    
    def set_time(self, ticket, time):
        '''
        Устанавливает временную метку для тикета.

        :param ticket: Тикет.
        :param time: Временная метка.
        '''
        for data in self.common_data:
            if data['ticket'] == ticket:
                data['time'] = time
    
    @classmethod
    async def get_candles(cls, ticket, interval, time):
        '''
        Получает свечи (candles) для указанного тикета, интервала и временной метки.

        :param ticket: Тикет.
        :param interval: Интервал.
        :param time: Временная метка.
        :return: Свечи (candles).
        '''
        return await super().get_candles(f'{cls._MAIN_URL}klines', symbol=ticket.upper(), interval=interval, startTime=time)
    
    async def get_times(self):
        '''
        Получает временные метки для тикетов, у которых временная метка еще не установлена.
        '''
        tickets = [data['ticket'] for data in self.common_data if data['time'] is None]
        times = await super().times(f'{self._MAIN_URL}klines', tickets)

        i=0
        for data in self.common_data:
            if data['time'] is None:
                data['time'] = times[i]
                i+=1
    
    def __iter__(self):
        '''
        Возвращает итератор, который перебирает тикеты, схемы и временные метки.

        :return: Итератор, содержащий тикеты, схемы и временные метки.
        '''
        return zip(self.tickets, self.schemas, self.times)


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
