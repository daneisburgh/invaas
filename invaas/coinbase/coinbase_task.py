import random
import requests
import uuid

import pandas as pd

from invaas.task import Task
from invaas.coinbase.coinbase_client import CoinbaseClient, OrderSide


class CoinbaseTask(Task):
    """
    Task class to execute ETL processes for loading and preparing data.
    """

    def __init__(self, env: str = "local"):
        super().__init__(env=env)

        self.cb_client = CoinbaseClient(self.get_secret("COINBASE-API-KEY"), self.get_secret("COINBASE-API-SECRET"))

        self.product_ids = ["ATOM-USD", "BTC-USD", "DOT-USD", "ETH-USD", "SOL-USD"]
        self.logger.info(f"Products to trade: {str(self.product_ids)}")

        self.min_fear_greed_index_to_buy = 60
        self.current_fear_greed_index = int(requests.get("https://api.alternative.me/fng/").json()["data"][0]["value"])
        self.logger.info(f"Minimum fear greed index to buy: {self.min_fear_greed_index_to_buy}")
        self.logger.info(f"Current fear greed index: {self.current_fear_greed_index}")

        self.min_buy_amount = 2
        self.max_buy_amount = 100
        self.max_owned_amount = 1000
        self.logger.info(f"Min buy amount: ${self.min_buy_amount}")
        self.logger.info(f"Max buy amount: ${self.max_buy_amount}")
        self.logger.info(f"Max owned amount: ${self.max_owned_amount}")

    def __get_available_balance(self, account_name: str):
        df_accounts = pd.DataFrame(self.cb_client.list_accounts()["accounts"])
        cash_account = df_accounts.loc[df_accounts.name == account_name].to_dict(orient="records")[0]
        return float(cash_account["available_balance"]["value"])

    def __get_crypto_id(self, product_id: str):
        return product_id.split("-")[0]

    def __buy_product(self, product_id: str, available_cash: float):
        buy_amount = self.floor_value(value=(available_cash / len(self.product_ids) / 10), precision=2)
        buy_amount = buy_amount if buy_amount >= self.min_buy_amount else self.min_buy_amount
        buy_amount = buy_amount if buy_amount <= self.max_buy_amount else self.max_buy_amount
        self.logger.info(f"Buy amount: ${buy_amount:.2f}")

        if buy_amount > available_cash:
            self.logger.info(f"Not enough funds to buy {product_id}")
        else:
            order_id = str(uuid.uuid4())
            self.logger.info(f"Placing market buy order for {product_id}: {order_id}")

            self.cb_client.create_order(
                order_id=order_id,
                product_id=product_id,
                side=OrderSide.BUY.value,
                order_configuration={"quote_size": str(buy_amount)},
            )

    def __sell_product(self, product_id: str, owned_crypto: float):
        crypto_id = self.__get_crypto_id(product_id)

        if product_id in ["BTC-USD", "ETH-USD"]:
            precision = 8
        elif product_id in ["DOT-USD", "SOL-USD"]:
            precision = 3
        elif product_id in ["ATOM-USD"]:
            precision = 2

        sell_amount = self.floor_value(value=owned_crypto, precision=precision)
        self.logger.info(f"Sell amount: {sell_amount}")

        if sell_amount == 0:
            self.logger.info(f"No {crypto_id} to be sold")
        else:
            order_id = str(uuid.uuid4())
            self.logger.info(f"Placing market sell order: {order_id}")

            self.cb_client.create_order(
                order_id=order_id,
                product_id=product_id,
                side=OrderSide.SELL.value,
                order_configuration={"base_size": str(sell_amount)},
            )

    def create_orders(self):
        shuffled_product_ids = self.product_ids
        random.shuffle(shuffled_product_ids)

        for product_id in shuffled_product_ids:
            self.logger.info(f"Running process for {product_id}")

            crypto_id = self.__get_crypto_id(product_id)
            owned_crypto = self.__get_available_balance(f"{crypto_id} Wallet")
            self.logger.info(f"Owned {crypto_id}: {owned_crypto:.10f}")

            spot_price = float(self.cb_client.get_product(product_id=product_id)["price"])
            self.logger.info(f"Current {crypto_id} spot price: ${spot_price:.2f}")

            available_cash = self.__get_available_balance("Cash (USD)")
            self.logger.info(f"Available cash: ${available_cash:.2f}")

            order_side = (
                OrderSide.BUY.value
                if (
                    available_cash > self.min_buy_amount
                    and (owned_crypto * spot_price) < self.max_owned_amount
                    and self.current_fear_greed_index > self.min_fear_greed_index_to_buy
                )
                else OrderSide.SELL.value
            )
            self.logger.info(f"Order side: {order_side}")

            if order_side == OrderSide.BUY.value:
                self.__buy_product(product_id=product_id, available_cash=available_cash)
            else:
                self.__sell_product(product_id=product_id, owned_crypto=owned_crypto)

            print()
