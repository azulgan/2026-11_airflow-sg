"""Recupere le cours du bitcoin et simule son enregistrement."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowException
from airflow.sdk import dag, get_current_context, task

from myplugin import (
    BitcoinOperator,
    calculate_weekly_average_prices,
    create_market_dataframe,
    process_market_history,
)


COINGECKO_HISTORY_URL = (
    "/api/v3/coins/bitcoin/history"
)


HISTORY_DAYS = 9


@dag(
    dag_id="bitcoin-benj-dag",
    description="Cours du bitcoin depuis CoinGecko avec TaskFlow",
    start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["example"],
)
def bitcoin_dag():
    @task
    def build_history_dates() -> list[str]:
        if HISTORY_DAYS < 2:
            raise AirflowException("HISTORY_DAYS doit valoir au moins 2 pour calculer le RSI.")

        context = get_current_context()
        execution_date = context.get("logical_date") or context["dag_run"].run_after
        execution_day = execution_date.astimezone(timezone.utc).date()
        return [
            (execution_day - timedelta(days=days_ago)).isoformat()
            for days_ago in range(HISTORY_DAYS - 1, -1, -1)
        ]

    extract_bitcoin_price = BitcoinOperator.partial(
        task_id="extract_bitcoin_price",
        base_url=COINGECKO_HISTORY_URL,
        conn_id="bitcoin_url",
        retries=4,
        retry_delay=timedelta(seconds=2),
        retry_exponential_backoff=True,
        max_active_tis_per_dag=1,
    ).expand(day=build_history_dates())

    @task(multiple_outputs=True)
    def process_data(history):
        return process_market_history(history)

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def create_dataframe(history, latest):
        return create_market_dataframe(history, latest)

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def calculate_weekly_average_prices_task(frame):
        return calculate_weekly_average_prices(frame)

    @task
    def store_data(data) -> None:
        logging.getLogger(__name__).info("Enregistrement simule : %s", data)
    history = extract_bitcoin_price
    processed = process_data(history)
    frame = create_dataframe(processed["history"], processed["latest"])
    calculate_weekly_average_prices_task(frame)
    store_data(processed["latest"])


dag = bitcoin_dag()
    
    
    
    
    