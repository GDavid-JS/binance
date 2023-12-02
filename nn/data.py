import os

import psycopg2
import numpy as np

def get_data(schema, table, **connections_params):
    conn = psycopg2.connect(**connections_params)
    cursor = conn.cursor()
    cursor.execute(f'SELECT value FROM "{schema}"."{table}" ORDER BY time_close;')

    cursor.close()
    conn.close()

    return np.array([row[0] for row in cursor.fetchall()], dtype=np.uint8)

def transfer_data(source_conn_params, target_conn_params):
    source_conn = psycopg2.connect(**source_conn_params)
    source_cursor = source_conn.cursor()

    source_cursor.execute('''
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT LIKE 'pg_%'
        AND schema_name NOT IN ('information_schema', 'public')
    ''')
    schemas_source = [row[0] for row in source_cursor.fetchall()]

    target_conn = psycopg2.connect(**target_conn_params)
    target_cursor = target_conn.cursor()

    target_cursor.execute('''
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT LIKE 'pg_%'
        AND schema_name NOT IN ('information_schema', 'public')
    ''')

    schemas_target = [row[0] for row in target_cursor.fetchall()]

    schemas = list(set(schemas_source) - set(schemas_target))

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

