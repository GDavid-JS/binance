from abc import ABC, abstractmethod

class Exchange(ABC):
    '''
    Абстрактный базовый класс для представления интерфейса биржи.

    Определяет общие методы и свойства, которые должны быть реализованы в дочерних классах.
    '''

    @abstractmethod
    def __init__(self, *args, **kwargs):
        '''
        Инициализация класса Exchange.

        :param args: Позиционные аргументы.
        :param kwargs: Именованные аргументы.
        '''
        pass

    @property
    def connections(self):
        pass

    @connections.setter
    def connections(self, value):
        pass

    @property
    @abstractmethod
    def max_connections(self):
        '''
        Максимальное количество соединений для интерфейса.

        :return: Максимальное количество соединений.
        '''
        pass

    @property
    @abstractmethod
    def tickets(self):
        '''
        Список тикетов.

        :return: Список тикетов.
        '''
        pass
    
    @property
    @abstractmethod
    def schemas(self):
        '''
        Список схем.

        :return: Список схем.
        '''
        pass

    @property
    @abstractmethod
    def tables(self):
        '''
        Список таблиц.

        :return: Список таблиц.
        '''
        pass

    @property
    @abstractmethod
    def intervals(self):
        '''
        Список интервалов.

        :return: Список интервалов.
        '''
        pass

    @abstractmethod
    async def get_candles(self, ticket, interval, time):
        '''
        Получает свечи для указанного тикета, интервала и временной метки.

        :param ticket: Тикет.
        :param interval: Интервал.
        :param time: Временная метка.
        :return: Список свечей.
        '''
        pass

    @abstractmethod
    async def get_first_candle_time(self, ticket, interval):
        pass

    @abstractmethod
    def __iter__(self):
        '''
        Возвращает итератор, который перебирает тикеты, схемы и временные метки.

        :return: Итератор, содержащий тикеты, схемы и временные метки.
        '''
        pass
