import asyncio
import os
import functools
import time
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from enum import Enum

import asyncpg
import aiohttp

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
        else:
            raise ValueError("Invalid interval format")

class CandlesABC(ABC):
    @abstractmethod
    async def get_candles(self, symbol, interval, start_time, end_time):
        pass

class Candles(CandlesABC):
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
                tasks = [asyncio.create_task(self.__get_candles_task(url, session, **binance_task)) for binance_task in self.__get_tasks(symbol, interval, start_time, end_time)]

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

class Spot(Candles):
    __url = 'https://api.binance.com/api/v3/klines'
    async def get_candles(self, symbol, interval, start_time, end_time):
        async for candles in self._get_candles(self.__url, symbol, interval, start_time, end_time)
            yield candles

class Future(Candles):
    __url = 'https://fapi.binance.com/api/v1/klines'
    async def get_candles(self, symbol, interval, start_time, end_time):
        async for candles in self._get_candles(self.__url, symbol, interval, start_time, end_time)
            yield candles

class Task:
    __slots__ = ('ticket', 'interval', 'start_time', 'end_time')

    def __init__(self, ticket, interval, start_time, end_time):
        self.ticket = ticket
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time

class DatabaseConnector:
    async def init_connection(self, user, password, host, port, database, max_size):
        self.pool = await asyncpg.create_pool(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            max_size=max_size
        )

class TicketSqlABC(ABC):
    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def insert_many(self):
        pass


class TicketSql(TicketSqlABC):
    def __init__(self, schema, table):
        self.schema = schema
        self.table = table

    def create(self):
        return f'''
        CREATE SCHEMA IF NOT EXISTS "{self.schema}";
        CREATE TABLE IF NOT EXISTS "{self.schema}"."{self.table}" (
            time_open TIMESTAMP NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            time_close TIMESTAMP(3) PRIMARY KEY
        );'''

    def insert_many(self):
        return f'''
        INSERT INTO "{self.schema}"."{self.table}"
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (time_close)
        DO NOTHING;
        '''

class TicketData:
    def __init__(self, connector):
        self.connector = connector

    async def create_ticket(self, ticket_sql):
        if isinstance(ticket_sql, TicketSqlABC):
            async with self.connector.pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute(ticket_sql.create())

    async def insert_ticket(self, interface, ticket_sql, task):
        if isinstance(task, Task) and isinstance(interface, CandlesABC) and isinstance(ticket_sql, TicketSqlABC):
            async with self.connector.pool.acquire() as connection:
                async for candles in interface.get_candles(task.ticket, task.interval, task.start_time, task.end_time):
                    print(candles)
                    # async with connection.transaction():
                    #     await connection.executemany(
                    #         ticket_sql.insert(),
                    #         candles
                    # )

async def main():
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    host = os.environ.get('HOST')
    port = os.environ.get('POSTGRES_PORT')
    database = os.environ.get('NAME')

    tasks = [
        Task('btcusdt', TimeInterval.INTERVAL_1D, datetime.now() - timedelta(days=10), datetime.now())
    ]

    insert_tasks = []

    spot = Spot()
    connector = DatabaseConnector()
    ticket_data = TicketData(connector)

    await connector.init_connection(user, password, host, port, database, 20)
    for task in tasks:
        ticket_sql = TicketSql(task.ticket, task.interval)

        await ticket_data.create_ticket(ticket_sql)
        insert_tasks.append(asyncio.create_task(ticket_data.insert_ticket(spot, ticket_sql, task)))
    
    await asyncio.gather(*insert_tasks)

if __name__ == '__main__':
    asyncio.run(main())
