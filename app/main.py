import os
import asyncio
import time
from datetime import datetime, timedelta

import asyncpg
from binance import Spot, Future
from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy

from routes import IndexView


# class DatabaseManager:
#     def __init__(self, interface, **connection_params):
#         self.connection_params = connection_params
#         self.interface = interface

#     async def init_connection(self):
#         self.pool = await asyncpg.create_pool(
#             **self.connection_params,
#             max_size=self.interface.max_connections
#         )

    # async def insert(self, tasks):
    #     insert_tasks = []

    #     async with self.pool.acquire() as connection:
    #         for task in tasks:
    #             schema = task['ticket']
    #             table = task['interval']

    #             async with connection.transaction():
    #                 await connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

    #                 await connection.execute(f'''CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
    #                         time_open TIMESTAMP NOT NULL,
    #                         open DOUBLE PRECISION NOT NULL,
    #                         high DOUBLE PRECISION NOT NULL,
    #                         low DOUBLE PRECISION NOT NULL,
    #                         close DOUBLE PRECISION NOT NULL,
    #                         volume DOUBLE PRECISION NOT NULL,
    #                         time_close TIMESTAMP(3) PRIMARY KEY
    #                     );''')
                    
    #             insert_tasks.append(asyncio.create_task(self.__insert_task(**task)))

    #     await asyncio.gather(*insert_tasks)

#     async def __insert_task(self, ticket, interval, start_time, end_time):
#         async with self.pool.acquire() as connection:
#             async for candles in self.interface.get_candles(ticket, interval, start_time, end_time):
#                 async with connection.transaction():
#                     await connection.executemany(
#                         f'''
#                         INSERT INTO "{ticket}"."{interval}"
#                         VALUES ($1, $2, $3, $4, $5, $6, $7)
#                         ON CONFLICT (time_close)
#                         DO NOTHING;
#                         ''',
#                         candles
#                     )

#     async def close(self):
#         await self.pool.close()

# binance = DatabaseManager(
#     interface=spot,
#     user=USER,
#     password=PASSWORD,
#     host=HOST,
#     port=PORT,
#     database=NAME
# )

app = Flask(__name__)

if __name__ == '__main__':
    USER = os.environ.get('POSTGRES_USER')
    PASSWORD = os.environ.get('POSTGRES_PASSWORD')
    HOST = os.environ.get('HOST')
    PORT = os.environ.get('POSTGRES_PORT')
    NAME = os.environ.get('NAME')

    app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}'
    db = SQLAlchemy(app)

    spot = Spot()
    tickets = spot.get_all_tickets()
    index_view = IndexView.as_view('index', tickets=tickets)
    app.add_url_rule('/', view_func=index_view)
    app.run(host='0.0.0.0', port=5000)

