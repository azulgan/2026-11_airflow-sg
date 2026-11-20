"""Extensions Airflow pour le pipeline Bitcoin."""

import logging
from collections.abc import Sequence
from itertools import pairwise
from math import isfinite
from statistics import fmean
from typing import TYPE_CHECKING, TypedDict

import requests

from airflow.exceptions import AirflowException
from airflow.sdk import BaseHook, BaseOperator

if TYPE_CHECKING:
    from pandas import DataFrame


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


class BitcoinAPIHook(BaseHook):
    def __init__(self, base_url : str,conn_id: str | None = None, **kwargs):
        super().__init__(kwargs)
        self.conn_id = conn_id

    def get_data(self, day: str) -> HistoricalSnapshot:
        connection = self.get_connection(self.conn_id)  # Vérifie la connexion
        with requests.get(
            connection.protocol + "://" + connection.host + self.base_url,
            params={"date": day, "localization": "false"},
            timeout=30,
        ) as response:
            response.raise_for_status()
            payload = response.json()

        market_data = payload.get("market_data") if isinstance(payload, dict) else None
        if not isinstance(market_data, dict):
            raise AirflowException(
                f"CoinGecko ne fournit pas de donnees de marche pour le {day}."
            )
        return {"date": day, "market_data": market_data}


class BitcoinOperator(BaseOperator):
    def __init__(self, base_url: str, conn_id : str, day: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.myHook = BitcoinAPIHook(base_url = base_url, conn_id=conn_id)
        self.day = day

    def execute(self, context):
        return self.myHook.get_data(self.day)


def extract_bitcoin_snapshot(day: str, baseurl: str) -> HistoricalSnapshot:
    return BitcoinAPIHook(baseurl).get_data(day)


def process_market_history(
    history: Sequence[HistoricalSnapshot],
) -> ProcessedMarketData:
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
            raise AirflowException(
                f"Prix ou volume USD manquant pour le {day}."
            ) from exc

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


def create_market_dataframe(
    history: list[DailyMarketData], latest: DailyMarketDataWithRsi
) -> "DataFrame":
    import pandas as pd

    frame = pd.DataFrame.from_records(history, columns=["date", "price_usd"])
    frame["rsi"] = frame["date"].map({latest["date"]: latest["rsi"]})
    frame["date"] = pd.to_datetime(frame["date"], format="%Y-%m-%d")
    logging.getLogger(__name__).info(
        "Historique Bitcoin :\n%s", frame.to_string(index=False)
    )
    return frame


def calculate_weekly_average_prices(frame: "DataFrame") -> "DataFrame":
    week_start = frame["date"].dt.to_period("W-SUN").dt.start_time.rename("week_start")
    weekly_prices = (
        frame.groupby(week_start)["price_usd"]
        .mean()
        .reset_index(name="average_price_usd")
    )
    logging.getLogger(__name__).info(
        "Prix moyens hebdomadaires du bitcoin :\n%s",
        weekly_prices.to_string(index=False),
    )
    return weekly_prices
