import asyncio
import time
from datetime import datetime, timedelta

import asyncpg

from binance import Spot
from datetime_manager import DatetimeManager
import secret


class CreateDatabase:
    def __init__(self, interfaces, **connection_params):
        self.loop = asyncio.get_event_loop()
        self.connection_params = connection_params
        self.interfaces = interfaces
        self.__tasks = []

        self.MAX_CONNECTIONS = 10

    def __call__(self):
        for interface in self.interfaces:
            self.tickets = interface.get_tickets()
            time.sleep(1)
            self.loop.run_until_complete(self.__init_connection())

            stopwatch_start_time = time.time()
            self.loop.run_until_complete(self.create_tickets(interface))
            stopwatch_end_time = time.time()
            print(stopwatch_end_time - stopwatch_start_time)

            stopwatch_start_time = time.time()
            self.loop.run_until_complete(self.get_tasks(interface))
            stopwatch_end_time = time.time()
            print(stopwatch_end_time - stopwatch_start_time)

            stopwatch_start_time = time.time()
            self.loop.run_until_complete(self.insert(interface))
            stopwatch_end_time = time.time()
            print(stopwatch_end_time - stopwatch_start_time)

    
    async def __init_connection(self):
        self.pool = await asyncpg.create_pool(
            **self.connection_params,
            max_size=self.MAX_CONNECTIONS
        )


    async def get_tasks(self, interface):
        async with self.pool.acquire() as connection:
            for ticket in self.tickets:
                for i in range(len(interface.INTERVALS)):
                    query = f'''
                        SELECT MAX(time_close)
                        FROM {interface.TYPE}_{ticket}.interval_{interface.INTERVALS_DATABASE[i]}
                    '''
                    start_time = await connection.fetchval(query)

                    self.__tasks.append({
                        'ticket': ticket,
                        'start_time': start_time,
                        'interval': interface.INTERVALS[i]
                    })
                
        tasks = [{'ticket': task['ticket'], 'start_time': task['start_time']}
                for task in self.__tasks if task['start_time'] is None]


        unique_tasks = []
        for d in tasks:
            if d not in unique_tasks:
                unique_tasks.append(d)
        
        semaphore = asyncio.Semaphore(self.MAX_CONNECTIONS)
        async_tasks = [asyncio.create_task(self.__task(interface, tasks['ticket'], semaphore)) for tasks in unique_tasks]

        results = await asyncio.gather(*async_tasks)

        for task in self.__tasks:
            if task['start_time'] is None:
                for ready_task in results:
                    if ready_task['ticket'] == task['ticket']:
                        task['start_time'] = DatetimeManager.to_datetime(ready_task['start_time'])


    async def __task(self, interface, ticket, semaphore):
        async with semaphore:
            stopwatch_start_time = time.time()
            start_time = await interface.get_start_time(ticket);
            time_sleep = 2 - (time.time() - stopwatch_start_time)
            
            if time_sleep > 0:
                await asyncio.sleep(time_sleep)

            return {
                'ticket': ticket,
                'start_time': start_time,
            }

    async def insert(self, interface):
        semaphore = asyncio.Semaphore(self.MAX_CONNECTIONS)

        async_tasks = []
        for task in self.__tasks:
            time_candles = DatetimeManager.from_datetime(datetime.now() - timedelta(days=1))
            async_tasks.append(asyncio.create_task(self.__insert_task(interface, task['ticket'], task['interval'], time_candles, semaphore)))
        
        await asyncio.gather(*async_tasks)
    
    async def __insert_task(self, interface, ticket, interval, start_time, semaphore):
        async with semaphore:
            stopwatch_start_time = time.time()
            candles = await interface.get_candles(ticket, interval, start_time)
            
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    await connection.executemany(
                        f'INSERT INTO {interface.TYPE}_{ticket}.interval_{interval} VALUES ($1, $2, $3, $4, $5, $6, $7)',
                        candles
                    )
            
            time_sleep = 2 - (time.time() - stopwatch_start_time)
            
            if time_sleep > 0:
                await asyncio.sleep(time_sleep)


    async def create_tickets(self, interface):
        schema_names = [f'{interface.TYPE}_{ticket}'.lower() for ticket in self.tickets]

        async with self.pool.acquire() as connection:
            existing_schemas = [
                row[0]
                for row in await connection.fetch(
                    '''
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN ('public', 'information_schema', 'pg_catalog', 'pg_toast')
                    '''
                )
            ]

            missing_schemas = [schema for schema in schema_names if schema not in existing_schemas]

            for schema in missing_schemas:
                async with connection.transaction():
                    await connection.execute(f'CREATE SCHEMA {schema};')

                    for interval in interface.INTERVALS_DATABASE:
                        await connection.execute(f'''CREATE TABLE {schema}.interval_{interval} (
                            time_open TIMESTAMP(3) NOT NULL,
                            open DOUBLE PRECISION NOT NULL,
                            close DOUBLE PRECISION NOT NULL,
                            high DOUBLE PRECISION NOT NULL,
                            low DOUBLE PRECISION NOT NULL,
                            volume DOUBLE PRECISION NOT NULL,
                            time_close TIMESTAMP(3) PRIMARY KEY
                        )''')

    
    async def __async_close(self):
        await self.pool.close()
    
    def close(self):
        self.loop.run_until_complete(self.__async_close())
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()

    def __del__(self):
        self.close()

def main():
    interfaces = [Spot(secret.API_KEY, secret.API_SECRET)]

    binance = CreateDatabase(
        interfaces,
        user=secret.USER,
        password=secret.PASSWORD,
        host=secret.HOST,
        port=secret.PORT,
        database=secret.DATABASE
    )

    binance()

if __name__ == "__main__":
    main()
