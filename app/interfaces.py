from datetime import datetime

from constans import MAX_CANDLES_CONNECTIONS
from binance import Binance
from exchange import Exchange

class TemplateBinanceInterface(Binance, Exchange):
    def get_all_tickets(self):
        return super().get_all_tickets(f'{self._MAIN_URL}exchangeInfo')

    async def get_candles(self, *args):
        async for candles in super().get_candles(f'{self._MAIN_URL}klines', *args):
            yield candles

    async def get_first_candle_time(self, ticket, interval):
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