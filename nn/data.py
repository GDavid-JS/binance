import os

import psycopg2
import numpy as np

class DataNN:
    def __init__(self, **connections_params):
        self.conn = psycopg2.connect(**connections_params)
        self.cursor = self.conn.cursor()
    
    def get_schemas(self):
        self.cursor.execute(
            '''
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT LIKE 'pg_%'
            AND schema_name NOT IN ('information_schema', 'public')
            '''
        )
        
        return [schema[0] for schema in self.cursor.fetchall()]

    
    def get_schema(self, ticket):
        self.cursor.execute(
            f'''
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name LIKE '%{ticket}'
            '''
        )
        
        return [schema[0] for schema in self.cursor.fetchall()]
    
    def get_tables(self, schema):
        self.cursor.execute(
            f'''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            '''
        )

        return [table[0] for table in self.cursor.fetchall()]
    
    def get_table(self, schema, timeframe):
        self.cursor.execute(
            f'''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}' and
                table_name LIKE '%{timeframe}'
            '''
        )

        return self.cursor.fetchone()[0]
    
    def get_data(self, schema, table):
        self.cursor.execute(f'SELECT value FROM "{schema}"."{table}" ORDER BY time_close;')

        data = np.array([row[0] for row in self.cursor.fetchall()], dtype=np.uint8)

        return data
    
    def get_timeframe_data(self, timeframe):
        for schema in self.get_schemas():
            table = self.get_table(schema, timeframe)
            yield self.get_data(schema, table)

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def __del__(self):
        self.close()


def transfer_data(source_conn_params, target_conn_params):
    source_conn = psycopg2.connect(**source_conn_params)
    source_cursor = source_conn.cursor()

    source_cursor.execute('''
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT LIKE 'pg_%'
        AND schema_name NOT IN ('information_schema', 'public')
    ''')
    schemas = [row[0] for row in source_cursor.fetchall()]

    target_conn = psycopg2.connect(**target_conn_params)
    target_cursor = target_conn.cursor()

    for schema in schemas:
        target_cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

        source_cursor.execute(f'SELECT table_name FROM information_schema.tables WHERE table_schema = \'{schema}\'')
        tables = [row[0] for row in source_cursor.fetchall()]

        for table in tables:
            target_cursor.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (value BOOLEAN, time_close TIMESTAMP(3) PRIMARY KEY);')

            source_cursor.execute(f'''
                SELECT CASE WHEN close > open THEN FALSE ELSE TRUE END, time_close
                FROM "{schema}"."{table}"
                ORDER BY time_close;
            ''')

            results = source_cursor.fetchall()

            target_cursor.executemany(f'INSERT INTO "{schema}"."{table}" (value, time_close) VALUES (%s, %s)', results)


        target_conn.commit()

    source_cursor.close()
    target_cursor.close()

    source_conn.close()
    target_conn.close()

