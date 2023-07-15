from abc import ABC, abstractmethod

class Exchange(ABC):
    @abstractmethod
    def get_tickets(self):
        pass

    @abstractmethod
    def get_candles(self):
        pass

    @abstractmethod
    def get_start_time(self):
        pass