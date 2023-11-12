# Main task class

import numpy as np
import requests
import pandas as pd
import pandas_ta as ta
import uuid
import yfinance as yf

from datetime import datetime, timedelta
from sklearn.linear_model import BayesianRidge

from invaas.coinbase_client import CoinbaseClient
from invaas.task import Task


class MainTask(Task):
    """
    Task class to execute ETL processes for loading and preparing data.
    """

    def __init__(self, env: str = None):
        """Initialize ETL task class and variables

        Args:
            state (str): State to load data
            run_date (str, optional): Run date required for historical performance testing
        """

        super().__init__(env=env)
        self.logger.info(f"Initializing task for {self.env} environment")
        self.cb_client = CoinbaseClient(self.get_secret("COINBASE-API-KEY"), self.get_secret("COINBASE-API-SECRET"))
        self.crypto_ids = ["ATOM", "BTC", "DOT", "ETH", "SOL"]
        self.prediction_periods = 4
        self.prediction_label = "period_max_open_pct_change"
        self.df_fear_greed_index = None

    def get_df_history(self, product_id: str):
        if self.df_fear_greed_index is None:
            self.df_fear_greed_index = self.__get_df_fear_greed_index()

        self.logger.info(f"Retrieving historical data for {product_id}")

        df_history = yf.Ticker(product_id).history(interval="1h", period="90d")[:-1]
        df_history.columns = map(str.lower, df_history.columns)

        timestamps = [pd.to_datetime(x, utc=True) for x in df_history.index.values]
        timestamps_date_range = pd.date_range(start=timestamps[0], end=timestamps[-1], freq="H")

        df_history = df_history.set_index(pd.DatetimeIndex(timestamps)).reindex(timestamps_date_range, method="ffill")

        df_history.drop(columns=["dividends", "stock splits"], inplace=True)
        df_history["volume"] = df_history.volume.replace(to_replace=0, method="ffill")

        df_history = df_history.join(self.df_fear_greed_index[["fear_greed_index"]])
        df_history["fear_greed_index"] = df_history.fear_greed_index.fillna(method="ffill")

        def get_strategy_length(x):
            return x * 24

        CustomStrategy = ta.Strategy(
            name="Momo and Volatility",
            description="SMA 50,200, BBANDS, RSI, MACD and Volume SMA 20",
            ta=[
                {"kind": "sma", "length": get_strategy_length(10)},
                {"kind": "sma", "length": get_strategy_length(20)},
                {"kind": "sma", "length": get_strategy_length(50)},
                {"kind": "bbands", "length": get_strategy_length(20)},
                {"kind": "rsi", "length": get_strategy_length(14)},
                {
                    "kind": "macd",
                    "fast": get_strategy_length(8),
                    "slow": get_strategy_length(21),
                },
                {
                    "kind": "sma",
                    "close": "volume",
                    "length": get_strategy_length(10),
                    "prefix": "VOLUME",
                },
                {
                    "kind": "sma",
                    "close": "volume",
                    "length": get_strategy_length(20),
                    "prefix": "VOLUME",
                },
                {
                    "kind": "sma",
                    "close": "volume",
                    "length": get_strategy_length(50),
                    "prefix": "VOLUME",
                },
            ],
        )
        df_history.ta.strategy(CustomStrategy)
        df_history[self.prediction_label] = (
            (
                df_history[::-1].open.rolling(window=self.prediction_periods, min_periods=1).max().shift(1)
                - df_history.open
            )
            / df_history.open
            * 100
        )

        return df_history

    def get_period_next_open_pct_change(self, df_history: pd.DataFrame):
        current_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        period_end = pd.to_datetime(current_time, utc=True)
        period_start = pd.to_datetime(current_time - timedelta(days=60), utc=True)
        df_period = df_history.loc[df_history.index >= period_start]
        df_train = df_period.loc[df_period.index < period_end].dropna()
        df_test = df_period.loc[df_period.index >= period_end]

        feature_columns = [x for x in df_history.columns if x != self.prediction_label]
        x_train = np.array(df_train[feature_columns])
        x_test = np.array(df_test[feature_columns])[:1]
        y_train = np.array(df_train[self.prediction_label])

        model = BayesianRidge()
        model.fit(x_train, y_train.ravel())

        return model.predict(x_test)[0]

    def buy_crypto(self, crypto_id: str):
        available_cash = self.__get_available_balance("Cash (USD)")
        self.logger.info(f"Available cash: ${available_cash:.2f}")
        buy_amount = self.__floor_value(available_cash / 10, 2)
        self.logger.info(f"Buy amount: ${buy_amount:.2f}")
        product_id = f"{crypto_id}-USD"

        if buy_amount == 0:
            self.logger.info(f"Not enough fund to buy {product_id}")
        else:
            spot_price = self.cb_client.get_product(product_id)["price"]
            self.logger.info(f"Current {product_id.split('-')[0]} spot price: ${spot_price}")
            order_id = str(uuid.uuid4())
            self.logger.info(f"Placing market buy order for {product_id}: {order_id}")

            self.cb_client.create_order(
                order_id=order_id,
                product_id=product_id,
                side="BUY",
                order_configuration={"quote_size": str(buy_amount)},
            )

    def sell_crypto(self, crypto_id: str):
        available_crypto = self.__get_available_balance(f"{crypto_id} Wallet")
        self.logger.info(f"Available {crypto_id}: {available_crypto:.10f}")
        sell_amount = self.__floor_value(available_crypto, 8)
        self.logger.info(f"Sell amount: {sell_amount}")
        product_id = f"{crypto_id}-USD"

        if sell_amount > 0:
            spot_price = self.cb_client.get_product(product_id)["price"]
            self.logger.info(f"Current {crypto_id} spot price: ${spot_price}")
            order_id = str(uuid.uuid4())
            self.logger.info(f"Placing market sell order: {order_id}")

            self.cb_client.create_order(
                order_id=order_id,
                product_id=product_id,
                side="SELL",
                order_configuration={"base_size": str(sell_amount)},
            )

    def __floor_value(self, x: float, precision: int):
        return np.true_divide(np.floor(x * 10**precision), 10**precision)

    def __get_available_balance(self, account_name: str):
        df_accounts = pd.DataFrame(self.cb_client.list_accounts()["accounts"])
        cash_account = df_accounts.loc[df_accounts.name == account_name].to_dict(orient="records")[0]
        return float(cash_account["available_balance"]["value"])

    def __get_df_fear_greed_index(self):
        self.logger.info("Retrieving historical fear and greed index")
        fear_greed_index_response = requests.get(
            "https://api.alternative.me/fng/?limit=" + str(24 * 365 * 2), timeout=600
        )
        df_fear_greed_index = pd.DataFrame(fear_greed_index_response.json()["data"])
        df_fear_greed_index.set_index(
            pd.DatetimeIndex([pd.Timestamp(int(x), unit="s", tz="UTC") for x in df_fear_greed_index.timestamp]),
            inplace=True,
        )
        df_fear_greed_index.rename(columns={"value": "fear_greed_index"}, inplace=True)
        return df_fear_greed_index
