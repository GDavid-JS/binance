import asyncio
import hashlib
import hmac
import functools
import time
import urllib.parse
from datetime import datetime

import aiohttp
import requests
from multipledispatch import dispatch

from constans import MAX_CANDLES_CONNECTIONS, CANDLES_DELAY, INTERVALS_MILLISECONDS

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

class Binance:
    candles_delay = delay(CANDLES_DELAY)

    def __init__(self):
        self.__semaphore = asyncio.Semaphore(MAX_CANDLES_CONNECTIONS)

    @classmethod
    def get_all_tickets(cls, url):
        return [coin['symbol'].lower() for coin in requests.get(url).json()['symbols']]

    @candles_delay
    async def session(self, url, session, **params):
        async with session.get(url, params=params) as response:
            return await response.json()

    async def __get_candles_task(self, url, session, **params):
        async with self.__semaphore:
            return await self.session(url, session, **params)

    async def get_candles(self, url, *args):
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(self.__get_candles_task(url, session, **binance_task)) for binance_task in self.__get_binance_tasks(*args)]

            for candles in asyncio.as_completed(tasks):
                yield (
                    (
                        datetime.fromtimestamp(candl[0] / 1000),
                        *(map(float, candl[1:6])),
                        datetime.fromtimestamp(candl[6] / 1000)
                    )
                    for candl in await candles
                )

    def __get_binance_tasks(self, symbol, interval, startTime, endTime):
        params_list = []
        params = {
            'symbol': symbol.upper(),
            'interval': interval,
        }

        params['limit'] = 1000

        startTime = int(startTime.timestamp() * 1000)
        endTime = int(endTime.timestamp() * 1000)
        candle_interval = INTERVALS_MILLISECONDS[interval]
        time_diff = params['limit'] * candle_interval

        while endTime > startTime:
            params_copy = params.copy()
            params_copy['startTime'] = startTime
            params_copy['endTime'] = min(startTime + time_diff, endTime)
            params_list.append(params_copy)
            startTime = params_copy['endTime']

        return params_list