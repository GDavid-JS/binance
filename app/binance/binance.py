import asyncio
import functools
import time
from datetime import datetime
from abc import ABC, abstractmethod

import aiohttp

from .interval import TimeInterval

def delay(func):
    @functools.wraps(func)
    async def inner(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        
        time_sleep = 1.2 - (end_time - start_time)
        if time_sleep > 0:
            await asyncio.sleep(time_sleep)
        return result
    return inner

class Exchange(ABC):
    @abstractmethod
    async def get_candles(self, symbol, interval, start_time, end_time):
        pass


class Binance(Exchange):
    __semaphore = None

    def __new__(cls):
        if cls.__semaphore is None:
            cls.__semaphore = asyncio.Semaphore(20)

        return super().__new__(cls)
    
    def __init__(self, url):
        self.url = url

    @delay
    async def __session(self, url, session, **params):
        async with session.get(url, params=params) as response:
            return await response.json()

    async def __get_candles_task(self, url, session, **params):
        async with self.__semaphore:
            return await self.__session(url, session, **params)

    async def get_candles(self, symbol, interval, start_time, end_time):
        if isinstance(interval, TimeInterval) and isinstance(start_time, datetime) and isinstance(end_time, datetime):
            async with aiohttp.ClientSession() as session:
                tasks = [asyncio.create_task(self.__get_candles_task(self.url, session, **binance_task)) for binance_task in self.__get_binance_tasks(symbol, interval, start_time, end_time)]

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
    

class Spot(Binance):
    def __init__(self):
        super().__init__('https://api.binance.com/api/v3')

class Future(Binance):
    def __init__(self):
        super().__init__('https://fapi.binance.com/api/v1')
