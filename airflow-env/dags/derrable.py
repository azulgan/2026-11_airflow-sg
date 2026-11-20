"""
deferrable DAG.
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.sdk import get_current_context
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
import requests
import logging

def log(str):
    logger = logging.getLogger(__name__)
    logger.info(str)


@dag(
    dag_id="deferrable-dag",
    schedule="@once",
    start_date=datetime(2026, 11, 18),
    catchup=False,
)
def taskflow_2():

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

    process_task = process_data()
    store_task = store_data()
    timeDataSensor >> [process_task, store_task]

dag = taskflow_2()