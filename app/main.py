import asyncio
import aiohttp
import asyncpg
import requests
import psycopg2
import time
import json
import hmac
import hashlib
import urllib.parse
import random
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from secret import API_KEY, API_SECRET

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


class Binance:
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

    __BASE_INTERVALS = {
        INTERVAL_1MIN: 60 * 1000,
        INTERVAL_1H: 60 * 60 * 1000,
        INTERVAL_1D: 24 * 60 * 60 * 1000,
    }

    INTERVALS_MILLSECONDS = {
        INTERVAL_1MIN: __BASE_INTERVALS[INTERVAL_1MIN],
        INTERVAL_3MIN: 3 * __BASE_INTERVALS[INTERVAL_1MIN],
        INTERVAL_5MIN: 5 * __BASE_INTERVALS[INTERVAL_1MIN],
        INTERVAL_15MIN: 15 * __BASE_INTERVALS[INTERVAL_1MIN],
        INTERVAL_30MIN: 30 * __BASE_INTERVALS[INTERVAL_1MIN],
        INTERVAL_1H: __BASE_INTERVALS[INTERVAL_1H],
        INTERVAL_2H: 2 * __BASE_INTERVALS[INTERVAL_1H],
        INTERVAL_4H: 4 * __BASE_INTERVALS[INTERVAL_1H],
        INTERVAL_6H: 6 * __BASE_INTERVALS[INTERVAL_1H],
        INTERVAL_8H: 8 * __BASE_INTERVALS[INTERVAL_1H],
        INTERVAL_12H: 12 * __BASE_INTERVALS[INTERVAL_1H],
        INTERVAL_1D: __BASE_INTERVALS[INTERVAL_1D],
        INTERVAL_3D: 3 * __BASE_INTERVALS[INTERVAL_1D],
        INTERVAL_1W: 7 * __BASE_INTERVALS[INTERVAL_1D],
        INTERVAL_1M: 30 * __BASE_INTERVALS[INTERVAL_1D],
    }

    INTERVALS_NAME = list(INTERVALS_MILLSECONDS.keys())

    INTERVALS_DATABASE = list(INTERVALS_MILLSECONDS.keys())[:-1]
    INTERVALS_DATABASE.append('1Mouth')

    def __init__(self, api_key, api_secret):
        self.__API_KEY = api_key
        self.__API_SECRET = api_secret

    @staticmethod
    def get_tickets(url):
        return [coin['symbol'] for coin in requests.get(url).json()['symbols']][:5]
    
    def __use_key(self, params, headers):
        timestamp = int(time.time() * 1000)
        params['timestamp'] = timestamp

        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(self.__API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

        params['signature'] = signature


        headers['X-MBX-APIKEY'] = self.__API_KEY

    @classmethod
    async def get(cls, url, **params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                res = await response.json()

                if isinstance(res, dict):
                    print(res)
                    # return
                
                return res

    @classmethod
    async def get_candles(cls, url, **params):
        endTime = int(time.time() * 1000) if 'endTime' not in params else params['endTime']
        params_list = []

        while True:
            interval_time = cls.INTERVALS_NAME[params['interval']]
            candles_len = int((endTime - params['startTime'])/interval_time)
            params['limit'] = 1000 if candles_len > 1000 else candles_len

            if params['limit'] == 0:
                break

            params_list.append(params.copy())

            params['startTime'] += interval_time*1000

            if params['limit'] != 1000:
                break
        

        results = []

        for params in params_list:
            start_time = time.time()
            result = await cls.get(url, **params);
            end_time = time.time()
            time_sleep = (1-(end_time-start_time))
            if time_sleep:
                await asyncio.sleep(time_sleep)
            
            results.extend(result)
        
        results = (
            (
                DatetimeManager.to_datetime(candl[0]),
                *candl[1:6],
                DatetimeManager.to_datetime(candl[6])
            )
            for candl in results
        )


        return results
            

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
        return Binance.get_tickets(f'{cls.__MAIN_URL}exchangeInfo')

    @classmethod
    async def get_candles(cls, ticket, interval, time):
        return await Binance.get_candles(f'{cls.__MAIN_URL}klines', symbol=ticket, interval=interval, startTime=time)
    
    @classmethod
    async def get_start_time(cls, ticket):
        return (await Binance.get(f'{cls.__MAIN_URL}klines', symbol=ticket, interval=Binance.INTERVAL_1M, limit=1000))[0][0]

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
        return await Binance.get_tickets(f'{cls.__MAIN_URL}exchangeInfo')

    @classmethod
    async def get_candles(cls, ticket, interval, time):
        return await Binance.get_candles(f'{cls.__MAIN_URL}klines', symbol=ticket, interval=interval, startTime=time)
    
    @classmethod
    async def get_start_time(cls, ticket):
        return (await Binance.get(f'{cls.__MAIN_URL}klines', symbol=ticket, interval=Binance.INTERVAL_1M, limit=1000))[0][0]


class CreateDatabase:
    def __init__(self, interfaces, **connection_params):
        self.loop = asyncio.get_event_loop()
        self.connection_params = connection_params
        self.conn = psycopg2.connect(**connection_params)
        self.cursor = self.conn.cursor()
        self.interfaces = interfaces

        self.MAX_CONNECTIONS = 4

    def __call__(self):
        for interface in self.interfaces:
            self.tickets = interface.get_tickets()

            self.create_tickets(interface)

            self.loop.run_until_complete(self.time(interface))
            # print(self.tickets_time)
            # self.loop.run_until_complete(self.insert(interface, tickets))

    async def time(self, interface):
        tickets_time = []
        tasks = []
        for ticket in self.tickets:
            self.cursor.execute(f'SET search_path TO {interface.TYPE}_{ticket};')
            for interval in interface.INTERVALS_NAME:
                self.cursor.execute(f'SELECT MAX(time_close) FROM {interface.TYPE}_{ticket}.interval_{interval};')
                time = self.cursor.fetchone()[0]


                if time is None:
                    tasks.append(asyncio.create_task(self.task_time(interface, ticket)))

                tickets_time.append({ticket: time})

        for i in range(0, len(tasks), self.MAX_CONNECTIONS):
            results = await asyncio.gather(*tasks[i:i+self.MAX_CONNECTIONS])
            # [item for sublist in results for item in sublist]
            # tickets_time.extend(results)
            print(results)

        self.tickets_time = tickets_time
    
    async def task_time(self, interface, ticket):
        return {ticket: await interface.get_start_time(ticket)}

    async def insert(self, interface):
        tasks = []
        for ticket in self.tickets:
            for interval in interface.INTERVALS_NAME:
                time_candles = DatetimeManager.from_datetime(datetime.now() - timedelta(days=1))
                tasks.append(asyncio.create_task(self.task_insert(interface, ticket, interval, time_candles)))
        
        for i in range(0, len(tasks), self.MAX_CONNECTIONS):
            await asyncio.gather(*tasks[i:i+self.MAX_CONNECTIONS])
    
    async def task_insert(self, interface, ticket, interval, time):
        conn = psycopg2.connect(**self.connection_params)
        cursor = conn.cursor()

        print(cursor.connection.get_backend_pid())
        
        candles = await interface.get_candles(ticket, interval, time)
        
        print(ticket, interval, time, len(candles))

        cursor.executemany(f'INSERT INTO {interface.TYPE}_{ticket}.interval_{interval} VALUES (%s, %s, %s, %s, %s, %s, %s)', candles)

        conn.commit()
        conn.close()

    def create_tickets(self, interface):
        schema_names = [f'{interface.TYPE}_{ticket}'.lower() for ticket in self.tickets]

        self.cursor.execute('''SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('public', 'information_schema', 'pg_catalog', 'pg_toast')''')
        existing_schemas = [row[0] for row in self.cursor.fetchall()]
        missing_schemas = [schema for schema in schema_names if not schema in existing_schemas]

        for schema in missing_schemas:
            self.cursor.execute(f'CREATE SCHEMA {schema};')

            for interval in interface.INTERVALS_DATABASE:
                self.cursor.execute(f'''CREATE TABLE {schema}.interval_{interval} (
                    time_open TIMESTAMP(3) NOT NULL,
                    open DOUBLE PRECISION NOT NULL,
                    close DOUBLE PRECISION NOT NULL,
                    high DOUBLE PRECISION NOT NULL,
                    low DOUBLE PRECISION NOT NULL,
                    volume DOUBLE PRECISION NOT NULL,
                    time_close TIMESTAMP(3) PRIMARY KEY
                )''')


            self.conn.commit()
    
    def close(self):
        self.conn.close()  
        self.cursor.close()  
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()

    def __del__(self):
        self.close()

class DatetimeManager:
    @staticmethod
    def to_datetime(time):
        return datetime.fromtimestamp(time / 1000) 
    
    @staticmethod
    def from_datetime(time):
        return int(time.timestamp() * 1000)

def main():
    interfaces = [Spot(API_KEY, API_SECRET)]

    binance = CreateDatabase(interfaces, user='root', password='root', host='db', port=5432, database='binance')
    binance()

if __name__ == "__main__":
    main()
