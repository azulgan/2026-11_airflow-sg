"""
bitcoin DAG.
"""

import textwrap
from datetime import datetime, timedelta
import requests

from airflow.sdk import get_current_context
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def extract_bitcoin_price():
    context = get_current_context()
    #print(context)
    timetoday = context.get("logical_date") or context["start_date"]
    print(timetoday)
    formatted_date = timetoday.date()
    with requests.get(
            "https://api.coingecko.com/api/v3/coins/bitcoin/history",
            params={"date": formatted_date, "localization": "false"},
            timeout=30,
        ) as response:
            response.raise_for_status()
            payload = response.json()
            usd = payload["market_data"]["current_price"]["usd"]
            print(usd)

default_args={
        "depends_on_past": False,
        "email": ["email@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2026, 11, 18),
        'start_date': datetime(2026, 11, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    }

with DAG(
    "bitcoin-dag",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    description="ditcoin DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 11, 18),
    catchup=False,
    tags=["example"],
) as dag:

    extract_bitcoin_price = PythonOperator(
        task_id="extract_bitcoin_price",
        python_callable=extract_bitcoin_price,
    )

    
    dag.doc_md = __doc__
