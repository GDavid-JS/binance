import asyncio
import hashlib
import hmac
import functools
import time
import urllib.parse
from datetime import datetime, timedelta

import aiohttp
import requests
from multipledispatch import dispatch

def delay(candles_delay):
    def decorator(func):
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

candles_delay = delay(1.2)

class Binance:
    _shared_state = {
        '_semaphore': None
    }

    def __new__(cls):
        if cls._shared_state.get('_semaphore') is None:
            cls._shared_state['_semaphore'] = asyncio.Semaphore(20)

        obj = super(Binance, cls).__new__(cls)
        obj.__dict__ = cls._shared_state

        return obj

    @classmethod
    def get_all_tickets(cls, url):
        return [coin['symbol'].lower() for coin in requests.get(url).json()['symbols']]

    @candles_delay
    async def session(self, url, session, **params):
        async with session.get(url, params=params) as response:
            return await response.json()

    async def __get_candles_task(self, url, session, **params):
        async with self._semaphore:
            return await self.session(url, session, **params)

    async def get_candles(self, url, symbol, interval, start_time, end_time):
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(self.__get_candles_task(url, session, **binance_task)) for binance_task in self.__get_binance_tasks(symbol, interval, start_time, end_time)]

            for candles in asyncio.as_completed(tasks):
                yield (
                    (
                        datetime.fromtimestamp(candl[0] / 1000),
                        *(map(float, candl[1:6])),
                        datetime.fromtimestamp(candl[6] / 1000)
                    )
                    for candl in await candles
                )

    def __get_binance_tasks(self, symbol, interval, start_time, end_time):
        params_list = []
        params = {
            'symbol': symbol.upper(),
            'interval': interval.value,
        }

        params['limit'] = 1000

        start_time = int(start_time.timestamp() * 1000)
        end_time = int(end_time.timestamp() * 1000)
        candle_interval = interval.to_milliseconds()
        time_diff = params['limit'] * candle_interval

        while end_time > start_time:
            params_copy = params.copy()
            params_copy['startTime'] = start_time
            params_copy['endTime'] = min(start_time + time_diff, end_time)
            params_list.append(params_copy)
            start_time = params_copy['endTime']

        return params_list