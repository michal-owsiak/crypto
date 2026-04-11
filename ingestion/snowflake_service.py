import os
import sys
from pathlib import Path
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas

sys.path.append(str(Path(__file__).resolve().parents[1]))
from shared.snowflake_client import get_connection


def get_max_open_time(conn):
    query = '''
        select 
            max(open_time)
        from 
            BINANCE_BTCUSDT_1D
    '''

    with conn.cursor() as cur:
        cur.execute(f'use warehouse {os.environ['SNOWFLAKE_WAREHOUSE']}')
        cur.execute(f'use database {os.environ['SNOWFLAKE_DATABASE']}')
        cur.execute(f'use schema {os.environ['SNOWFLAKE_RAW_SCHEMA']}')
        cur.execute(query)
        row = cur.fetchone()

    if row is None:
        return None

    return row[0]


def load_to_snowflake(df):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'use warehouse {os.environ['SNOWFLAKE_WAREHOUSE']}')
            cur.execute(f'use database {os.environ['SNOWFLAKE_DATABASE']}')
            cur.execute(f'use schema {os.environ['SNOWFLAKE_RAW_SCHEMA']}')

        df = df.copy()
        df.columns = [c.upper() for c in df.columns]

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name='BINANCE_BTCUSDT_1D',
            database=os.environ['SNOWFLAKE_DATABASE'],
            schema=os.environ['SNOWFLAKE_RAW_SCHEMA'],
            auto_create_table=False,
            overwrite=False,
            use_logical_type=True,
        )
        print(f'Loaded {nrows} rows, success={success}')
    finally:
        conn.close()

