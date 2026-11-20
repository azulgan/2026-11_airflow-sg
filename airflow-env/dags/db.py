"""
db DAG.
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.sdk import get_current_context
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
import requests
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def log(str):
    logger = logging.getLogger(__name__)
    logger.info(str)   
        
def do_extract(conn_id='postgres_test'):
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    records = postgres_hook.get_records(sql="SELECT * FROM airflow_test")
    data = pd.DataFrame(records, columns=['dag_id', 'execution_date'])
    print(data)    

@dag(
    dag_id="adb-dag",
    schedule="@once",
    start_date=datetime(2026, 11, 18),
    catchup=False,
)
def taskflow_2():
    #drop_table = SQLExecuteQueryOperator(
    #    task_id='drop_table',
    #    conn_id='postgres_test',
    #    sql = """
    #        drop TABLE  airflow_test ;
    #    """
    #)
    
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_test',
        sql = """
            CREATE TABLE IF NOT EXISTS airflow_test (
                dag_id varchar,
                execution_date DATE NOT NULL,
                PRIMARY KEY (dag_id, execution_date));
        """
    )

    delete_current = SQLExecuteQueryOperator(
        task_id='delete_current',
        conn_id='postgres_test',
        sql = """
            delete from airflow_test where dag_id = '{{ dag.dag_id }}' 
            and execution_date = '{{ ds }}';
        """
    )

    display_currents = SQLExecuteQueryOperator(
        task_id='display_currents',
        conn_id='postgres_test',
        sql = """
            select * from airflow_test;
        """
    )

    insert_current = SQLExecuteQueryOperator(
        task_id='insert_current',
        conn_id='postgres_test',
        sql = """
            insert into airflow_test (dag_id, execution_date) 
            values ('{{ dag.dag_id }}', '{{ ds }}');
        """
    )

    extract = PythonOperator(
        task_id='extract',
        python_callable=do_extract,
    )

    timeDataSensor = TimeDeltaSensor(
        task_id="timeDataSensor",
        delta=timedelta(seconds=20),
        mode="reschedule",
        deferrable=True,
    )

    @task(priority_weight=10)
    def process_data():
        print("Processing data...")

    @task(priority_weight=5)
    def store_data():
        print("Storing data...")

    create_table >> delete_current 
    delete_current >> insert_current >> display_currents 
    display_currents >> extract >> timeDataSensor
    process_task = process_data()
    store_task = store_data()
    timeDataSensor >> [process_task, store_task]

dag = taskflow_2()