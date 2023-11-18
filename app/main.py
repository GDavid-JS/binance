import asyncio
import os
import time
from datetime import datetime, timedelta

from interfaces import Future, Spot

import asyncpg

async def create_ticket(pool, interface, tasks):
    insert_tasks = []

    async with pool.acquire() as connection:
        for task in tasks:
            schema = task['ticket']
            table = task['interval']

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
            insert_tasks.append(asyncio.create_task(task(pool, interface, *task.values())))

    await asyncio.gather(*insert_tasks)


async def insert_ticket(pool, interface, ticket, interval, start_time, end_time):
    async with pool.acquire() as connection:
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

async def main():
    USER = os.environ.get('POSTGRES_USER')
    PASSWORD = os.environ.get('POSTGRES_PASSWORD')
    HOST = os.environ.get('HOST')
    PORT = os.environ.get('POSTGRES_PORT')
    NAME = os.environ.get('NAME')

    spot = Spot()

    pool = await asyncpg.create_pool(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        database=NAME,
        max_size=10
    )

    tasks = [
        {'ticket': 'btcusdt', 'interval': '1d', 'start_time': datetime.now() - timedelta(days=10), 'end_time': datetime.now()}
    ]

    await create_ticket(pool, spot, tasks)

if __name__ == '__main__':
    asyncio.run(main())
