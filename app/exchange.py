from abc import ABC, abstractmethod

class Exchange(ABC):
    @abstractmethod
    def get_all_tickets(self):
        pass

    @abstractmethod
    async def get_candles(self, tasks):
        pass

    @abstractmethod
    async def get_first_candle_time(self, ticket, interval):
        pass