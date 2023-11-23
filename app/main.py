import asyncio
import os
import time
from datetime import datetime, timedelta

from interfaces import Future, Spot

import asyncpg

class Task:
    __slots__ = ('ticket', 'interval', 'start_time', 'end_time')

    def __init__(self, ticket, interval, start_time, end_time):
        self.ticket = ticket
        self.interval = interval
        self.start_time = start_time
        self.end_time = end_time

class DataProcessor:
    async def init_connection(self, user, password, host, port, database):
        self.pool = await asyncpg.create_pool(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database,
            max_size=10
        )
    
    async def create_ticket(self, task):
        if type(task) == Task:
            async with self.pool.acquire() as connection:
                schema = task.ticket
                table = task.interval

                async with connection.transaction():
                    await connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

                    await connection.execute(f'''CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                            time_open TIMESTAMP NOT NULL,
                            open DOUBLE PRECISION NOT NULL,
                            high DOUBLE PRECISION NOT NULL,
                            low DOUBLE PRECISION NOT NULL,
                            close DOUBLE PRECISION NOT NULL,
                            volume DOUBLE PRECISION NOT NULL,
                            time_close TIMESTAMP(3) PRIMARY KEY
                        );''')
    
    async def insert_ticket(self, interface, task):
        if type(task) == Task:
            async with self.pool.acquire() as connection:
                async for candles in interface.get_candles(task.ticket, task.interval, task.start_time, task.end_time):
                    async with connection.transaction():
                        await connection.executemany(
                            f'''
                            INSERT INTO "{task.ticket}"."{task.interval}"
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (time_close)
                            DO NOTHING;
                            ''',
                            candles
                    )

async def main():
    user = os.environ.get('POSTGRES_USER')
    password = os.environ.get('POSTGRES_PASSWORD')
    host = os.environ.get('HOST')
    port = os.environ.get('POSTGRES_PORT')
    database = os.environ.get('NAME')

    tasks = [
        Task('btcusdt', '1d', datetime.now() - timedelta(days=10), datetime.now())
    ]

    insert_tasks = []

    spot = Spot()
    data_processor = DataProcessor()
    
    await data_processor.init_connection(user, password, host, port, database)

    for task in tasks:
        await data_processor.create_ticket(task)
        insert_tasks.append(asyncio.create_task(data_processor.insert_ticket(spot, task)))
    
    await asyncio.gather(*insert_tasks)


if __name__ == '__main__':
    asyncio.run(main())
