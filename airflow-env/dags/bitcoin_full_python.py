"""
bitcoin python DAG.
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.sdk import get_current_context
import requests
import logging

def log(str):
    logger = logging.getLogger(__name__)
    logger.info(str)

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history"

def extract():
    context = get_current_context()
    #print(context)
    timetoday = context.get("logical_date") or context["start_date"]
    print(timetoday)
    formatted_date = timetoday.date()
    with requests.get(
            API,
            params={"date": formatted_date, "localization": "false"},
            timeout=30,
        ) as response:
            response.raise_for_status()
            payload = response.json()
            usdd = payload["market_data"]["current_price"]["usd"]
            return usdd

@dag(
    dag_id="bicoin-full-pipeline",
    schedule="@once",
    start_date=datetime(2026, 11, 18),
    catchup=False,
)
def taskflow_2():
    @task(task_id="extract", retries=3)
    def extract_bitcoin_price():
        return extract()

    @task(multiple_outputs=True)
    def process_data(usd):
            return { "usd" : usd }

    @task
    def store_data(data):
        print(data)
        pass

    store_data(process_data(extract_bitcoin_price()))

dag = taskflow_2()