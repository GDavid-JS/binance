import asyncio
import aiohttp
import requests
import time
import urllib.parse
import hmac
import hashlib
import binance_intervals
from datetime_manager import DatetimeManager
from exchange import Exchange

class Binance(Exchange):
    INTERVALS = binance_intervals.INTERVALS
    INTERVALS_MILLSECONDS = binance_intervals.INTERVALS_MILLSECONDS
    INTERVALS_DATABASE = binance_intervals.INTERVALS_DATABASE
    MAX_CONNECTIONS = 20
    MIN_DELAY = 1

    def __init__(self, api_key, api_secret, delay = MIN_DELAY):
        self.__API_KEY = api_key
        self.__API_SECRET = api_secret
        self.__delay = delay
    
    @property
    def delay(self):
        return self.__delay
    
    @delay.setter
    def delay(self, delay):
        if delay >= self.MIN_DELAY:
            self.__delay = delay
        else:
            raise "Too much requests"

    def delay(self, func):
        async def inner(*args, **kwargs):
            start_time = time.time()
            result = func()
            end_time = time.time()
            time_sleep = self.delay-(end_time-start_time)
            if time_sleep > 0:
                await asyncio.sleep(time_sleep)
            
            return result
        return inner
    
    def __use_key(self, params, headers):
        timestamp = int(time.time() * 1000)
        params['timestamp'] = timestamp

        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(self.__API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

        params['signature'] = signature


        headers['X-MBX-APIKEY'] = self.__API_KEY
    
    def error_handler(response):
        print('Error', response)


    @classmethod
    def get(cls, url):
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()

        cls.error_handler(response.json())

    @classmethod
    async def async_get(cls, url, **params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                res = await response.json()
                if response.status == 200:
                    return res
                
                cls.error_handler(res)
    
    @classmethod
    def get_tickets(cls, url):
        return [coin['symbol'].lower() for coin in cls.get(url)['symbols']][:5]

    @classmethod
    async def get_candles(cls, url, **params):
        params_list = cls.__params(**params)
        
        results = []

        for params in params_list:
            results.extend(await cls.__get_candles_delay(url, **params))
        
        return (
            (
                DatetimeManager.to_datetime(candl[0]),
                *(map (float, candl[1:6])),
                DatetimeManager.to_datetime(candl[6])
            )
            for candl in results
        )

    @classmethod
    async def get_candles_max_connections(cls, url, **params):
        params_list = cls.__params(**params)

        semaphore = asyncio.Semaphore(cls.MAX_CONNECTIONS)

        tasks = [asyncio.create_task(cls.__get_candles_max_connections_task(url, semaphore, **params)) for params in params_list]
        
        await asyncio.gather(*tasks)
    
    @delay
    @classmethod
    async def __get_candles_max_connections_task(cls, url, semaphore, **params):
        async with semaphore:
            return await cls.async_get(url, **params);
    
    @delay
    @classmethod
    async def __get_candles_delay(cls, url, **params):
        return await cls.async_get(url, **params);

    @classmethod
    def __params(cls, **params):
        endTime = int(time.time() * 1000) if 'endTime' not in params else params['endTime']
        params_list = []

        while True:
            interval_time = cls.INTERVALS_MILLSECONDS[params['interval']]
            candles_len = int((endTime - params['startTime'])/interval_time)
            params['limit'] = 1000 if candles_len > 1000 else candles_len
            params['symbol'] = params['symbol'].upper()

            if params['limit'] == 0:
                break

            params_list.append(params.copy())

            params['startTime'] += interval_time*1000

            if params['limit'] != 1000:
                break
        
        return params_list

class Spot(Binance):
    TYPE = 'spot'

    __API = 'api'
    __VERSION = 'v3'
    __BASE_URL = f'https://{__API}.binance.com/'
    __MAIN_URL = f'{__BASE_URL}{__API}/{__VERSION}/'
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
        return (await Binance.async_get(f'{cls.__MAIN_URL}klines', symbol=ticket.upper(), interval=binance_intervals.INTERVAL_1M, limit=1000))[0][0]

class Future(Binance):
    TYPE = 'future'

    __API = 'fapi'
    __VERSION = 'v1'
    __BASE_URL = f'https://{__API}.binance.com/'
    __MAIN_URL = f'{__BASE_URL}{__API}/{__VERSION}/'

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
        return (await Binance.async_get(f'{cls.__MAIN_URL}klines', symbol=ticket.upper(), interval=Binance.INTERVAL_1M, limit=1000))[0][0]
