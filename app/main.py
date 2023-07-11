import asyncio
import aiohttp
import asyncpg
import requests
import psycopg2
import time
import json
import hmac
import hashlib
from accessify import private, protected
from abc import ABC, abstractmethod

class Binance:
    INTERVALS = ['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d']

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_key = api_secret

    @staticmethod
    @abstractmethod
    def get_tickets(url):
        return [coin['symbol'] for coin in requests.get(url).json()['symbols']][:2]
    
    @private
    def use_key(self, func):
        async def wrapper(self, url, params, headers={}):
            timestamp = int(time.time() * 1000)
            params['timestamp'] = timestamp

            query_string = '?' + [f'{key}={value}&' for key, value in params.items()]
            signature = hmac.new(self.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

            params['signature'] = signature

            headers['X-MBX-APIKEY'] = self.api_key
            return await func(url, params, headers)
        
        return wrapper

    # @staticmethod
    @use_key
    @abstractmethod
    async def get_candles(self, url, params, headers):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as response:
                return await response.json()

class Spot(Binance):
    TYPE = 'spot'

    __API = 'api'
    __V = 'v3'
    __BASE_URL = f'https://{__API}.binance.com/'
    __MAIN_URL = f'{__BASE_URL}{__API}/{__V}/'
    # https://api.binance.com/api/v3/

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def get_tickets(cls):
        return super().get_tickets(f'{cls.__MAIN_URL}exchangeInfo')

    @classmethod
    async def get_candles(cls, params, headers = {}):
        return await super().get_candles(f'{cls.__MAIN_URL}klines', params, headers) 

class Future(Binance):
    TYPE = 'future'

    __API = 'fapi'
    __V = 'v1'
    __BASE_URL = f'https://{__API}.binance.com/'
    __MAIN_URL = f'{__BASE_URL}{__API}/{__V}/'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    async def get_tickets(cls):
        return await super().get_tickets(f'{cls.__MAIN_URL}exchangeInfo')

    @classmethod
    async def get_candles(cls, params, headers = {}):
        return await super().get_candles(f'{cls.__MAIN_URL}klines', params, headers) 

class CreateBinance:
    def __init__(self, **connection_params):
        self.loop = asyncio.get_event_loop()
        self.semaphore = asyncio.Semaphore(50)
        self.connection_params = connection_params
        self.conn = psycopg2.connect(**connection_params)
        self.cursor = self.conn.cursor()

    def __call__(self):
        manager = Spot()
        tickets = manager.get_tickets()
        start_time = time.time()
        self.create_tickets(manager, tickets)
        end_time = time.time()

        print(end_time - start_time)
        start_time = time.time()
        self.loop.run_until_complete(self.start_insert(manager, tickets))
        end_time = time.time()
        print(end_time - start_time)
    
    async def start_insert(self, manager, tickets):
        tasks = []
        for ticket in tickets:
            for interval in Binance.INTERVALS:
                tasks.append(asyncio.create_task(self.insert(manager, ticket, interval)))

        await asyncio.gather(*tasks)

    async def insert(self, manager, ticket, interval):
        async with self.semaphore:
            conn = psycopg2.connect(**self.connection_params)
            cursor = conn.cursor()

            candles = ','.join([f'({",".join(candl[1:6])}, to_timestamp({candl[6]}))' for candl in await manager.get_candles({'symbol':ticket, 'interval':interval, 'limit':5000})])
            
            cursor.execute(f'''SET search_path TO {manager.TYPE}_{ticket};''')
            cursor.execute(f'''INSERT INTO interval_{interval}(open, close, high, low, volume, time) VALUES {candles}''')

            conn.commit()
            conn.close()

    def create_tickets(self, manager, tickets):
        for ticket in tickets:
            self.create_ticket(manager, ticket)
    
    def create_ticket(self, manager, ticket):
        self.cursor.execute(f'''CREATE SCHEMA IF NOT EXISTS {manager.TYPE}_{ticket};''')
        self.cursor.execute(f'''SET search_path TO {manager.TYPE}_{ticket};''')

        for interval in Binance.INTERVALS:
            self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS interval_{interval} (
                                    id SERIAL PRIMARY KEY,
                                    open DOUBLE PRECISION NOT NULL,
                                    close DOUBLE PRECISION NOT NULL,
                                    high DOUBLE PRECISION NOT NULL,
                                    low DOUBLE PRECISION NOT NULL,
                                    volume DOUBLE PRECISION NOT NULL,
                                    time TIMESTAMP(3)
            )''')

        self.conn.commit()

    def close(self):
        self.conn.close()  
        self.cursor.close()  
        self.loop.close()

    def __del__(self):
        self.close()


async def asyncStart():
    manager = Spot()
    candles = await manager.get_candles(symbol='BTCUSDT', interval='1m', limit=5000)

    print(len(candles))


def main():
    # asyncio.run(asyncStart())
    binance = CreateBinance(user='root', password='root', host='db', port=5432, database='binance')
    binance()


if __name__ == "__main__":
    main()

