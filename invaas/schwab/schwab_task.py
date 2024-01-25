import pandas as pd
import pandas_ta as ta
import pyotp
import random
import re
import requests
import time
import yfinance as yf

from invaas.task import Task
from invaas.schwab.schwab_api.schwab import Schwab
from invaas.schwab.cnn_fear_greed_index import get_historical_cnn_fear_greed_index


class SchwabTask(Task):
    """
    Task class to execute ETL processes for loading and preparing data.
    """

    def __init__(self, env: str):
        super().__init__(env=env)

        self.historical_period = 14
        self.logger.info(f"Historical period used for analysis: {self.historical_period} days")

        self.min_buy_amount = 5
        self.max_buy_amount = 100
        self.logger.info(f"Min buy amount: ${self.min_buy_amount}")
        self.logger.info(f"Max buy amount: ${self.max_buy_amount}")

    async def setup_api_and_get_products(self):
        self.logger.info("Logging in to Schwab")
        username = self.get_secret(key="SCHWAB-USERNAME")
        password = self.get_secret(key="SCHWAB-PASSWORD") + str(
            pyotp.TOTP(self.get_secret(key="SCHWAB-TOTP-SECRET")).now()
        )

        self.schwab_api = Schwab(schwab_account_id=self.get_secret("SCHWAB-ACCOUNT-ID"))
        await self.schwab_api.setup()
        await self.schwab_api.login(username=username, password=password)

        top_products = []
        etfs = ["QQQ", "VUG", "VGT"]

        with requests.Session() as req:
            req.headers.update(
                {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"}
            )

            for etf in etfs:
                r = req.get(f"https://www.zacks.com/funds/etf/{etf}/holding")
                holdings = re.findall(r"etf\\\/(.*?)\\", r.text)
                etf_top_holdings = holdings[:50]

                if len(top_products) == 0:
                    top_products = etf_top_holdings
                else:
                    top_products = list(set(top_products).intersection(etf_top_holdings))

        max_equity_rating = 50
        self.logger.info(f"Max equity rating to buy: {max_equity_rating}")

        current_fear_greed_index, previous_max_fear_greed_index = self.__get_fear_greed_index_data()
        self.logger.info(f"Current fear greed index: {current_fear_greed_index}")
        self.logger.info(f"Previous max fear greed index: {previous_max_fear_greed_index}")

        bad_products = []

        for product_id in top_products:
            product_equity_rating = await self.schwab_api.get_equity_rating(ticker=product_id)
            product_rsi = self.__get_product_rsi(product_id=product_id)
            self.logger.info(f"Equity rating and RSI for {product_id}: {product_equity_rating}, {product_rsi}")
            time.sleep(5)

            if product_equity_rating > max_equity_rating or not (
                product_rsi >= previous_max_fear_greed_index or (product_rsi >= 50 and current_fear_greed_index >= 50)
            ):
                bad_products.append(product_id)

        for product in bad_products:
            top_products.remove(product)

        self.products_to_buy = top_products
        self.logger.info(f"Products to buy: {str(self.products_to_buy)}")

        await self.schwab_api.close_api_session()

    def __get_fear_greed_index_data(self):
        df = pd.DataFrame(data=get_historical_cnn_fear_greed_index()["data"])
        df.set_index(
            pd.DatetimeIndex([pd.Timestamp(x, unit="ms", tz="UTC") for x in df.x]),
            inplace=True,
        )
        df.rename(columns={"y": "fear_greed_index"}, inplace=True)
        df.sort_index(inplace=True)
        df["fear_greed_index"] = df.fear_greed_index.fillna(method="ffill").astype(float)
        df["previous_max_fear_greed_index"] = (
            df["fear_greed_index"].rolling(window=self.historical_period, min_periods=self.historical_period).max()
        )
        return int(df.fear_greed_index.values[-1]), int(df.previous_max_fear_greed_index.values[-1])

    def __get_product_rsi(self, product_id: str):
        df = yf.Ticker(product_id).history(interval="1d", period="1y")
        df.columns = map(str.lower, df.columns)

        timestamps = [pd.to_datetime(x, utc=True).round(freq="D") for x in df.index.values]
        timestamps_date_range = pd.date_range(start=timestamps[0], end=timestamps[-1], freq="D")
        df = df.set_index(pd.DatetimeIndex(timestamps)).reindex(timestamps_date_range, method="ffill")

        RsiStrategy = ta.Strategy(name="RSI", ta=[{"kind": "rsi", "length": self.historical_period}])
        df.ta.strategy(RsiStrategy)
        df.sort_index(inplace=True)

        return int(df[f"RSI_{self.historical_period}"].values[-1])

    def __buy_product(self, product_id: str, available_cash: float):
        buy_amount = int(available_cash / len(self.products_to_buy) / 10)
        buy_amount = buy_amount if buy_amount >= self.min_buy_amount else self.min_buy_amount
        buy_amount = buy_amount if buy_amount <= self.max_buy_amount else self.max_buy_amount

        if buy_amount >= available_cash:
            self.logger.info(f"Not enough funds to buy {product_id}")
        else:
            self.logger.info(f"Buying ${buy_amount:.2f} of {product_id}")
            messages, success = self.schwab_api.buy_slice_v2(
                tickers=[product_id],
                amount_usd=buy_amount,
                dry_run=(self.env == "local"),
            )

            if not success:
                raise Exception(f"Error buying {product_id}: {str(messages)}")

        return self.floor_value(available_cash - buy_amount, 2)

    def __sell_product(self, product_id: str, owned_product_quantity: float):
        self.logger.info(f"Selling {owned_product_quantity} shares of {product_id}")
        messages, success = self.schwab_api.trade_v2(
            ticker=product_id,
            qty=owned_product_quantity,
            side="Sell",
            dry_run=(self.env == "local"),
        )

        if not success:
            raise Exception(f"Error selling {product_id}: {str(messages)}")

    def create_orders(self):
        account_info = self.schwab_api.get_account_info_v2()
        available_cash = account_info["available_cash"]

        owned_products = [x["symbol"] for x in account_info["positions"]]
        self.logger.info(f"Owned products: {owned_products}")

        for position in account_info["positions"]:
            if position["symbol"] not in self.products_to_buy:
                self.__sell_product(product_id=position["symbol"], owned_product_quantity=position["quantity"])

        shuffled_product_ids = self.products_to_buy
        random.shuffle(shuffled_product_ids)

        for product_id in shuffled_product_ids:
            available_cash = self.__buy_product(product_id=product_id, available_cash=available_cash)
