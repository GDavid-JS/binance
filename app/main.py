import asyncio
import aiohttp
import asyncpg
import requests
import psycopg2
import time
from accessify import private, protected
from abc import ABC, abstractmethod

class Binance:
    INTERVALS = ['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d']

    @staticmethod
    @abstractmethod
    async def get_tickets(url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                return [coin['symbol'] for coin in data['symbols']]
    
    @staticmethod
    @abstractmethod
    async def get_candles(url, params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                return await response.json()

class Spot(Binance):
    TYPE = 'spot'

    __API = 'api'
    __V = 'v3'
    __BASE_URL = f'https://{__API}.binance.com/'
    __MAIN_URL = f'{__BASE_URL}{__API}/{__V}/'
    # https://api.binance.com/api/v3/

    @classmethod
    async def get_tickets(cls):
        return await super().get_tickets(f'{cls.__MAIN_URL}exchangeInfo')

    @classmethod
    async def get_candles(cls, **params):
        return await super().get_candles(f'{cls.__MAIN_URL}klines', params)

class Future(Binance):
    TYPE = 'future'

    __API = 'fapi'
    __V = 'v1'
    __BASE_URL = f'https://{__API}.binance.com/'
    __MAIN_URL = f'{__BASE_URL}{__API}/{__V}/'

    @classmethod
    async def get_tickets(cls):
        return await super().get_tickets(f'{cls.__MAIN_URL}exchangeInfo')

    @classmethod
    async def get_candles(cls, **params):
        return await super().get_candles(f'{cls.__MAIN_URL}klines', params)


class CreateBinance:
    def __init__(self, **kwargs):
        self.loop = asyncio.get_event_loop()
        self.auth = kwargs

    async def connect(self):
        return await asyncpg.connect(**self.auth);


    def __call__(self):
        manager = Spot()
        start_time = time.time()
        self.loop.run_until_complete(self.create_tickets(manager))
        end_time = time.time()

        print(end_time - start_time)
        # asyncio.run(manager.get_candles(symbol="APTUSDT", interval="1h", limit=10))

    async def create_tickets(self, manager):
        tickets = await manager.get_tickets()

        tickets_tasks = []

        for ticket in tickets:
            tickets_tasks.append(asyncio.create_task(self.create_ticket(f'{manager.TYPE}_{ticket}')))
        
        await asyncio.gather(*tickets_tasks)
            
    
    async def create_ticket(self, schema_name):
        conn = await self.connect()
        await conn.execute(f'''CREATE SCHEMA IF NOT EXISTS {schema_name};''')
        await conn.execute(f'''SET search_path TO {schema_name};''')

        for interval in Binance.INTERVALS:
            await conn.execute(f'''CREATE TABLE IF NOT EXISTS interval_{interval} (
                                    id SERIAL PRIMARY KEY,
                                    open DOUBLE PRECISION NOT NULL,
                                    close DOUBLE PRECISION NOT NULL,
                                    high DOUBLE PRECISION NOT NULL,
                                    low DOUBLE PRECISION NOT NULL,
                                    volume INTEGER NOT NULL,
                                    time TIMESTAMP(3)
            )''')
        
        await conn.close()

    def __del__(self):
        self.loop.close()    



def main():
    binance = CreateBinance(user='root', password='root', host='db', port=5432, database='binance')
    # binance()

if __name__ == "__main__":
    main()

