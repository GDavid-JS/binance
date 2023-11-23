from datetime import datetime

from .binance import Binance
from .exchange import Exchange
from .interval import TimeInterval

class TemplateBinanceInterface(Binance, Exchange):
    def get_all_tickets(self):
        return super().get_all_tickets(f'{self._MAIN_URL}exchangeInfo')

    async def get_candles(self, symbol, interval, start_time, end_time):
        if type(interval) == TimeInterval and type(start_time) == datetime and type(end_time) == datetime:
            async for candles in super().get_candles(f'{self._MAIN_URL}klines', symbol, interval, start_time, end_time):
                yield candles

    async def get_first_candle_time(self, ticket, interval):
        if type(interval) == TimeInterval:
            async for candles in super().get_candles(f'{self._MAIN_URL}klines', ticket, interval, datetime.fromtimestamp(0), 1):
                return list(candles)[0][0]
    

class Spot(TemplateBinanceInterface):
    _API = 'api'
    _VERSION = 'v3'
    _BASE_URL = f'https://{_API}.binance.com/'
    _MAIN_URL = f'{_BASE_URL}{_API}/{_VERSION}/'

class Future(TemplateBinanceInterface):
    _API = 'fapi'
    _VERSION = 'v1'
    _BASE_URL = f'https://{_API}.binance.com/'
    _MAIN_URL = f'{_BASE_URL}{_API}/{_VERSION}/'
