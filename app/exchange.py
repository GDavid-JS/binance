from abc import ABC, abstractmethod

class Exchange(ABC):
    @property
    @abstractmethod
    def max_connections(self):
        pass

    @property
    @abstractmethod
    def intervals(self):
        pass

    @abstractmethod
    def get_candle_interval(self, interval):
        pass
    
    @abstractmethod
    def get_all_tickets(self):
        pass

    @abstractmethod
    async def get_candles(self, ticket, interval, time):
        pass

    @abstractmethod
    async def get_first_candle_time(self, ticket, interval):
        pass
