import os
import asyncio
import time
from datetime import datetime, timedelta

import asyncpg
from interfaces import Spot, Future


class DatabaseSchemaCreator:
    def __init__(self, **connection_params):
        self.connection_params = connection_params

    async def init_connection(self, interface):

        self.pool = await asyncpg.create_pool(
            **self.connection_params,
            max_size=interface.max_connections
        )

    async def get_tasks(self, tickets, intervals, interface):
        tasks = []
        params = []
        async with self.pool.acquire() as connection:
            for ticket in tickets:
                for interval in intervals:
                    query = f'''
                        SELECT MAX(time_close)
                        FROM "{ticket}"."{interval}"
                    '''
                    start_time = await connection.fetchval(query)

                    interval_time = interface.get_candle_interval(interval)

                    if start_time:
                        if start_time.timestamp() > datetime.now().timestamp()-interval_time:
                            tasks.append({
                                'ticket': ticket,
                                'interval': interval,
                                'startTime': start_time,
                            })
                    else:
                        params.append({
                            'ticket': ticket,
                            'interval': interval,
                        })

        param_tasks = [
            asyncio.create_task(interface.get_first_candle_time(
                param['ticket'],
                param['interval']
            ))
            for param in params
        ]

        result = [time for time in await asyncio.gather(*param_tasks)]



        for task in tasks:
            task['endTime'] =  datetime.now()

        for i, task_params in enumerate(params):
            tasks.append({**task_params, 'startTime': result[i], 'endTime': datetime.now()})
            # tasks.append({**task_params, 'startTime': datetime.now()-timedelta(10), 'endTime': datetime.now()})
        
        return tasks

    async def insert(self, interface, insert_tasks):
        tasks = [
            asyncio.create_task(
                self.__insert_task(
                    interface,
                    item['ticket'],
                    item['interval'],
                    item['startTime'],
                    item['endTime'],
                )
            )
            for item in insert_tasks
        ]

        await asyncio.gather(*tasks)

    async def __insert_task(self, interface, ticket, interval, start_time, end_time):
        async with self.pool.acquire() as connection:
            async for candles in interface.get_candles(ticket, interval, start_time, end_time):
                async with connection.transaction():
                    await connection.executemany(
                        f'''
                        INSERT INTO "{ticket}"."{interval}"
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (time_close)
                        DO NOTHING;
                        ''',
                        candles
                    )
                            
    async def create_tickets(self, schemas, tables):
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

            missing_schemas = [schema for schema in schemas if schema not in existing_schemas]

            for schema in missing_schemas:
                async with connection.transaction():
                    await connection.execute(f'CREATE SCHEMA "{schema}";')

                    for table in tables:
                        await connection.execute(f'''CREATE TABLE "{schema}"."{table}" (
                            time_open TIMESTAMP NOT NULL,
                            open DOUBLE PRECISION NOT NULL,
                            close DOUBLE PRECISION NOT NULL,
                            high DOUBLE PRECISION NOT NULL,
                            low DOUBLE PRECISION NOT NULL,
                            volume DOUBLE PRECISION NOT NULL,
                            time_close TIMESTAMP(3) PRIMARY KEY
                        );''')

    async def close(self):
        await self.pool.close()


async def main():
    USER = os.environ.get('POSTGRES_USER')
    PASSWORD = os.environ.get('POSTGRES_PASSWORD')
    HOST = os.environ.get('HOST')
    PORT = os.environ.get('POSTGRES_PORT')
    NAME = os.environ.get('NAME')

    interface = Spot()

    binance = DatabaseSchemaCreator(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=NAME
    )

    await binance.init_connection(interface)

    tickets = interface.get_all_tickets()[:2]

    print(tickets)

    # Создание схем и таблиц
    stopwatch_start_time = time.time()
    await binance.create_tickets(tickets, interface.intervals)
    stopwatch_end_time = time.time()
    print('Создание схем и таблиц: ', stopwatch_end_time - stopwatch_start_time)

    # # Получение задач и установка временных меток
    stopwatch_start_time = time.time()
    tasks = await binance.get_tasks(tickets, interface.intervals, interface)
    stopwatch_end_time = time.time()
    res = stopwatch_end_time - stopwatch_start_time
    print('Получение задач и установка временных меток: ', res)


    # # Вставка данных в базу данных
    # stopwatch_start_time = time.time()
    # await binance.insert(interface, tasks)
    # stopwatch_end_time = time.time()
    # print('Запросы и вставка данных в базу данных: ', stopwatch_end_time - stopwatch_start_time)

    await binance.close()

if __name__ == '__main__':
    asyncio.run(main())
