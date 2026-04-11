import sys
import subprocess
import os
from airflow.sdk import dag, task 
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

from shared.snowflake_client import get_connection
from ingestion.load_binance_ohlc import run_ingestion


@dag(
    dag_id='btc_pipeline',
    schedule='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False
)
def btc_pipeline():

    @task
    def run_snowflake_task():
        task_name = os.environ['LOAD_TASK']

        query = f'execute task {task_name}'

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'use warehouse {os.environ['SNOWFLAKE_WAREHOUSE']}')
                cur.execute(f'use database {os.environ['SNOWFLAKE_DATABASE']}')
                cur.execute(f'use schema {os.environ['SNOWFLAKE_RAW_SCHEMA']}')
                cur.execute(query)
                print(f'Executed Snowflake task "{task_name}"')
        finally:
            cur.close()
            conn.close()

    @task 
    def run_binance_ingestion():
        run_ingestion()
        print(f'Completed Binance OHLC ingestion')

    @task 
    def run_dbt():
        result = subprocess.run(
            ['dbt', 'run'],
            cwd=(project_root / 'dbt'),
            capture_output=True,
            text=True
        )

        print(result.stdout)

        if result.returncode != 0:
            print(result.stderr)
            raise Exception(f'dbt run failed: {result.stderr}')


    run_snowflake_task() >> run_binance_ingestion() >> run_dbt()

btc_pipeline()
