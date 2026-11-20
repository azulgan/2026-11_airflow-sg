    ------------------------------------------------
    
    """Recupere le cours du bitcoin et simule son enregistrement."""

import logging
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from itertools import pairwise
from math import isfinite
from statistics import fmean
from typing import TYPE_CHECKING, TypedDict

import requests

from airflow.exceptions import AirflowException
from airflow.sdk import dag, get_current_context, task

if TYPE_CHECKING:
    from pandas import DataFrame


HISTORY_DAYS = 9
MarketData = dict[str, dict[str, float]]


class HistoricalSnapshot(TypedDict):
    date: str
    market_data: MarketData


class DailyMarketData(TypedDict):
    date: str
    price_usd: float
    volume_usd: float


class DailyMarketDataWithRsi(DailyMarketData):
    rsi: float


class ProcessedMarketData(TypedDict):
    history: list[DailyMarketData]
    latest: DailyMarketDataWithRsi


@dag(
    dag_id="bitcoin-dag",
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

    @task(
        multiple_outputs=False,
        retries=4,
        retry_delay=timedelta(seconds=2),
        retry_exponential_backoff=True,
        max_active_tis_per_dag=1,
    )
    def extract_bitcoin_price(day: str) -> HistoricalSnapshot:
        with requests.get(
            "https://api.coingecko.com/api/v3/coins/bitcoin/history",
            params={"date": day, "localization": "false"},
            timeout=30,
        ) as response:
            response.raise_for_status()
            payload = response.json()

        market_data = payload.get("market_data") if isinstance(payload, dict) else None
        if not isinstance(market_data, dict):
            raise AirflowException(f"CoinGecko ne fournit pas de donnees de marche pour le {day}.")
        return {"date": day, "market_data": market_data}

    @task(multiple_outputs=True)
    def process_data(history: Sequence[HistoricalSnapshot]) -> ProcessedMarketData:
        if len(history) < 2:
            raise AirflowException(
                "Deux cours historiques au minimum sont requis pour calculer le RSI."
            )

        records: list[DailyMarketData] = []
        for snapshot in sorted(history, key=lambda snapshot: snapshot["date"]):
            day, market_data = snapshot["date"], snapshot["market_data"]
            try:
                price = market_data["current_price"]["usd"]
                volume = market_data["total_volume"]["usd"]
            except (KeyError, TypeError) as exc:
                raise AirflowException(f"Prix ou volume USD manquant pour le {day}.") from exc

            if (
                isinstance(price, bool)
                or not isinstance(price, (int, float))
                or not isfinite(price)
                or price <= 0
            ):
                raise AirflowException(f"Prix USD CoinGecko invalide pour le {day}.")
            if (
                isinstance(volume, bool)
                or not isinstance(volume, (int, float))
                or not isfinite(volume)
                or volume < 0
            ):
                raise AirflowException(f"Volume USD CoinGecko invalide pour le {day}.")
            records.append({"date": day, "price_usd": price, "volume_usd": volume})

        changes = [
            current["price_usd"] - previous["price_usd"]
            for previous, current in pairwise(records)
        ]
        average_gain = fmean(max(change, 0) for change in changes)
        average_loss = fmean(max(-change, 0) for change in changes)
        if average_loss == 0:
            rsi = 100.0 if average_gain > 0 else 50.0
        else:
            rsi = 100 - 100 / (1 + average_gain / average_loss)
        return {"history": records, "latest": {**records[-1], "rsi": rsi}}

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def create_dataframe(
        history: list[DailyMarketData], latest: DailyMarketDataWithRsi
    ) -> "DataFrame":
        import pandas as pd

        frame = pd.DataFrame.from_records(history, columns=["date", "price_usd"])
        frame["rsi"] = frame["date"].map({latest["date"]: latest["rsi"]})
        frame["date"] = pd.to_datetime(frame["date"], format="%Y-%m-%d")
        logging.getLogger(__name__).info("Historique Bitcoin :\n%s", frame.to_string(index=False))
        return frame

    @task(multiple_outputs=False, show_return_value_in_logs=False)
    def calculate_weekly_average_prices(frame: "DataFrame") -> "DataFrame":
        week_start = frame["date"].dt.to_period("W-SUN").dt.start_time.rename("week_start")
        weekly_prices = (
            frame.groupby(week_start)["price_usd"].mean().reset_index(name="average_price_usd")
        )
        logging.getLogger(__name__).info(
            "Prix moyens hebdomadaires du bitcoin :\n%s", weekly_prices.to_string(index=False)
        )
        return weekly_prices

    @task
    def store_data(data: DailyMarketDataWithRsi) -> None:
        logging.getLogger(__name__).info("Enregistrement simule : %s", data)

    history = extract_bitcoin_price.expand(day=build_history_dates())
    processed = process_data(history)
    frame = create_dataframe(processed["history"], processed["latest"])
    calculate_weekly_average_prices(frame)
    store_data(processed["latest"])


dag = bitcoin_dag()
    
    
    
    
    