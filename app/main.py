import asyncio
import os
import functools
import time
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass

import aiohttp
from sqlalchemy import MetaData, Table, Column, TIMESTAMP, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

metadata = MetaData()

Base = declarative_base(metadata=metadata)

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

class TimeInterval(Enum):
    INTERVAL_1MIN = '1m'
    INTERVAL_3MIN = '3m'
    INTERVAL_5MIN = '5m'
    INTERVAL_15MIN = '15m'
    INTERVAL_30MIN = '30m'
    INTERVAL_1H = '1h'
    INTERVAL_2H = '2h'
    INTERVAL_4H = '4h'
    INTERVAL_6H = '6h'
    INTERVAL_8H = '8h'
    INTERVAL_12H = '12h'
    INTERVAL_1D = '1d'
    INTERVAL_3D = '3d'
    INTERVAL_1W = '1w'
    INTERVAL_1M = '1M'

    def to_milliseconds(self):
        if 'm' in self.value:
            return int(self.value[:-1]) * 60 * 1000
        elif 'h' in self.value:
            return int(self.value[:-1]) * 60 * 60 * 1000
        elif 'd' in self.value:
            return int(self.value[:-1]) * 24 * 60 * 60 * 1000
        elif 'w' in self.value:
            return int(self.value[:-1]) * 7 * 24 * 60 * 60 * 1000
        elif 'M' in self.value:
            return int(self.value[:-1]) * 30 * 24 * 60 * 60 * 1000
        raise ValueError("Invalid interval format")

class CandlesABC(ABC):
    @abstractmethod
    async def get_candles(self, symbol, interval, start_time, end_time):
        pass

class GetCandles(CandlesABC):
    __semaphore = None

    def __new__(cls):
        if cls.__semaphore is None:
            cls.__semaphore = asyncio.Semaphore(20)

        return super().__new__(cls)
    
    @delay
    async def __session(self, url, session, **params):
        async with session.get(url, params=params) as response:
            return await response.json()

    async def __get_candles_task(self, url, session, **params):
        if self.__semaphore is not None:
            async with self.__semaphore:
                return await self.__session(url, session, **params)

    async def _get_candles(self, url, symbol, interval, start_time, end_time):
        if isinstance(interval, TimeInterval) and isinstance(start_time, datetime) and isinstance(end_time, datetime):
            async with aiohttp.ClientSession() as session:
                tasks = [self.__get_candles_task(url, session, **binance_task) for binance_task in self.__get_tasks(symbol, interval, start_time, end_time)]

                for candles in asyncio.as_completed(tasks):
                    yield (
                        (
                            datetime.fromtimestamp(candl[0] / 1000),
                            *(map(float, candl[1:6])),
                            datetime.fromtimestamp(candl[6] / 1000)
                        )
                        for candl in await candles
                    )

    def __get_tasks(self, symbol, interval, start_time, end_time):
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

class Spot(GetCandles):
    __url = 'https://api.binance.com/api/v3/klines'
    async def get_candles(self, symbol, interval, start_time, end_time):
        async for candles in self._get_candles(self.__url, symbol, interval, start_time, end_time):
            yield candles

class Future(GetCandles):
    __url = 'https://fapi.binance.com/api/v1/klines'
    async def get_candles(self, symbol, interval, start_time, end_time):
        async for candles in self._get_candles(self.__url, symbol, interval, start_time, end_time):
            yield candles

@dataclass
class Task:
    __slots__ = ('ticket', 'interval', 'start_time', 'end_time')

    ticket: str
    interval: str
    start_time: datetime
    end_time: datetime

async def create_tables(engine, tables):
    async with engine.begin() as conn:
        for table in tables:
            await conn.run_sync(table.create, checkfirst=True)

async def insert_ticket(engine, interface, task, table_name):
    async with AsyncSession(engine) as session:
        if isinstance(task, Task) and isinstance(interface, CandlesABC):
            async for candles in interface.get_candles(task.ticket, task.interval, task.start_time, task.end_time):
                await session.execute(Table(table_name, metadata).insert().values(tuple(candles)))
                await session.commit()

async def main():
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    host = os.environ.get('HOST')
    port = os.environ.get('POSTGRES_PORT')
    database = os.environ.get('NAME')

    database_url = f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}'

    engine = create_async_engine(database_url, pool_size=20)

    tasks = [
        Task('btcusdt', TimeInterval.INTERVAL_1D, datetime.now() - timedelta(days=10), datetime.now())
    ]

    spot = Spot()

    insert_tasks = []
    tables = []

    for task in tasks:
        table_name = f'{task.ticket}_{task.interval.value}'
        tables.append(Table(
            table_name, metadata,
            Column('time_open', TIMESTAMP, nullable=False),
            Column('open', Float, nullable=False),
            Column('high', Float, nullable=False),
            Column('low', Float, nullable=False),
            Column('close', Float, nullable=False),
            Column('volume', Float, nullable=False),
            Column('time_close', TIMESTAMP(3), nullable=False)
        ))

        insert_tasks.append(insert_ticket(engine, spot, task, table_name))

    await create_tables(engine, tables)
    await asyncio.gather(*insert_tasks)

if __name__ == '__main__':
    asyncio.run(main())
