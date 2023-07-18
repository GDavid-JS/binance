import asyncio
import time
from datetime import datetime, timedelta

import asyncpg

import secret
from interfaces import Spot, Future


class DatabaseSchemaCreator:
    '''
    Класс для создания и заполнения базы данных.
    '''

    def __init__(self, interfaces, **connection_params):
        '''
        Инициализация класса `DatabaseSchemaCreator`.

        :param interfaces: Список объектов интерфейса базы данных.
        :param connection_params: Параметры подключения к базе данных.
        '''
        self.loop = asyncio.get_event_loop()
        self.connection_params = connection_params
        self.interfaces = interfaces

        self.loop.run_until_complete(self.main())
    
    async def main(self):
        for interface in self.interfaces:
            await self.__init_connection(interface)

            # Создание схем и таблиц
            stopwatch_start_time = time.time()
            await self.create_tickets(interface)
            stopwatch_end_time = time.time()
            print('Создание схем и таблиц: ', stopwatch_end_time - stopwatch_start_time)

            # Получение задач и установка временных меток
            stopwatch_start_time = time.time()
            tasks = await self.get_tasks(interface)
            stopwatch_end_time = time.time()
            res = stopwatch_end_time - stopwatch_start_time
            print('Получение задач и установка временных меток: ', res)
            if 1-res > 0:
                time.sleep(1-res)

            # Вставка данных в базу данных
            stopwatch_start_time = time.time()
            await self.insert(interface, tasks)
            stopwatch_end_time = time.time()
            print('Запросы и вставка данных в базу данных: ', stopwatch_end_time - stopwatch_start_time)

    async def __init_connection(self, interface):
        '''
        Функция создает пул соединений.

        :param interface: Объект, представляющий интерфейс базы данных.
        '''
        self.pool = await asyncpg.create_pool(
            **self.connection_params,
            max_size=interface.max_connections
        )

    async def get_tasks(self, interface):
        '''
        Получает задачи из базы данных и устанавливает с какого периода надо получить свечки в объекте интерфейса.

        :param interface: Объект, представляющий интерфейс базы данных.
        '''
        tasks = []
        first_candle_time_params = []
        async with self.pool.acquire() as connection:
            for ticket, schema in interface:
                for i, table in enumerate(interface.tables):
                    query = f'''
                        SELECT MAX(time_close)
                        FROM {schema}.{table}
                    '''
                    start_time = await connection.fetchval(query)

                    if start_time:
                        tasks.append({
                            'ticket': ticket,
                            'interval': interface.intervals[i],
                            'startTime': start_time,
                            'schema': schema,
                            'table': table
                        })
                    else:
                        first_candle_time_params.append({
                            'ticket': ticket,
                            'interval': interface.intervals[i],
                            'schema': schema,
                            'table': table,
                        })
                        
        
        # first_candle_time_tasks = [
        #     asyncio.create_task(interface.get_first_candle_time(
        #         first_candle_time_task['ticket'],
        #         first_candle_time_task['interval']
        #     ))
        #     for first_candle_time_task in first_candle_time_params
        # ]




        # result = [time for time in await asyncio.gather(*first_candle_time_tasks)]

        for i, task_params in enumerate(first_candle_time_params):
            # tasks.append({**task_params, 'startTime': result[i], 'endTime': datetime.now()})
            tasks.append({**task_params, 'startTime': datetime.now()-timedelta(4), 'endTime': datetime.now()})
        


        return tasks

    async def insert(self, interface, insert_tasks):
        '''
        Вставляет данные в базу данных для каждой задачи в интерфейсе.

        :param interface: Объект, представляющий интерфейс базы данных.
        '''
        semaphore = asyncio.Semaphore(interface.max_connections)

        tasks = [
            asyncio.create_task(
                self.__insert_task(
                    semaphore,
                    interface,
                    item['schema'],
                    item['table'],
                    item['ticket'],
                    item['interval'],
                    item['startTime'],
                    item['endTime'],
                )
            )
            for item in insert_tasks
        ]

        await asyncio.gather(*tasks)

    async def __insert_task(self, semaphore, interface, schema, table, *params):
        '''
        Асинхронно вставляет свечи (candles) в базу данных для указанной задачи, интервала и таблицы.

        :param semaphore: Семафор для ограничения количества одновременных операций вставки.
        :param interface: Объект, представляющий интерфейс базы данных.
        :param ticket: Название тикета.
        :param interval: Интервал времени свечей.
        :param start_time: Время начала свечей.
        :param schema: Название схемы базы данных.
        :param table: Название таблицы базы данных.
        '''
        async with self.pool.acquire() as connection:
            async for candles in interface.get_candles(*params):
                async with connection.transaction():
                    await connection.executemany(
                        f'''
                        INSERT INTO {schema}.{table}
                        VALUES ($1, $2, $3, $4, $5, $6, $7);
                        ''',
                        candles
                    )
                    # ON CONFLICT (time_close)
                        # DO NOTHING;
                            
    async def create_tickets(self, interface):
        '''
        Создает схемы и таблицы в базе данных для каждого интерфейса.

        :param interface: Объект, представляющий интерфейс базы данных.
        '''
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

            missing_schemas = [schema for schema in interface.schemas if schema not in existing_schemas]

            for schema in missing_schemas:
                async with connection.transaction():
                    await connection.execute(f'CREATE SCHEMA {schema};')

                    for table in interface.tables:
                        await connection.execute(f'''CREATE TABLE {schema}.{table} (
                            time_open TIMESTAMP(3) NOT NULL,
                            open DOUBLE PRECISION NOT NULL,
                            close DOUBLE PRECISION NOT NULL,
                            high DOUBLE PRECISION NOT NULL,
                            low DOUBLE PRECISION NOT NULL,
                            volume DOUBLE PRECISION NOT NULL,
                            time_close TIMESTAMP(3) PRIMARY KEY
                        );''')

    async def __close_pool(self):
        '''
        Закрывает пул соединений с базой данных.
        '''
        await self.pool.close()

    def close(self):
        '''
        Закрывает все соединения и завершает цикл событий.
        '''
        self.loop.run_until_complete(self.__close_pool())
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()

    def __del__(self):
        '''
        Выполняется при удалении объекта класса.
        '''
        self.close()

def main():
    interfaces = [Spot(secret.API_KEY, secret.API_SECRET)]

    binance = DatabaseSchemaCreator(
        interfaces,
        user=secret.USER,
        password=secret.PASSWORD,
        host=secret.HOST,
        port=secret.PORT,
        database=secret.DATABASE
    )

if __name__ == '__main__':
    main()
    pass