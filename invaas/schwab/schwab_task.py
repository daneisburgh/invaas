import random
import re
import requests

from invaas.task import Task
from invaas.schwab.schwab_api.schwab import Schwab
from invaas.schwab.cnn_fear_greed_index import get_current_cnn_fear_greed_index


class SchwabTask(Task):
    """
    Task class to execute ETL processes for loading and preparing data.
    """

    def __init__(self, env: str):
        super().__init__(env=env)

        self.product_ids = self.__get_top_stocks()
        self.logger.info(f"Products to trade: {str(self.product_ids)}")

        self.current_fear_greed_index = int(get_current_cnn_fear_greed_index().value)
        self.logger.info(f"Current fear greed index: {self.current_fear_greed_index}")

    async def setup_api(self):
        self.logger.info("Logging in to Schwab")
        username = self.get_secret(key="SCHWAB-USERNAME")
        password = self.get_secret(key="SCHWAB-PASSWORD")

        self.schwab_api = Schwab(schwab_account_id=self.get_secret("SCHWAB-ACCOUNT-ID"))
        await self.schwab_api.setup()
        await self.schwab_api.login(username=username, password=password)

    def __get_top_stocks(self):
        top_stocks = []
        etfs = ["QQQ", "VUG", "VGT"]

        with requests.Session() as req:
            req.headers.update(
                {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"}
            )

            for etf in etfs:
                r = req.get(f"https://www.zacks.com/funds/etf/{etf}/holding")
                holdings = re.findall(r"etf\\\/(.*?)\\", r.text)
                etf_top_holdings = holdings[:30]

                if len(top_stocks) == 0:
                    top_stocks = etf_top_holdings
                else:
                    top_stocks = list(set(top_stocks).intersection(etf_top_holdings))

        return top_stocks

    def __buy_product(self, product_id: str, buy_amount: float):
        messages, success = self.schwab_api.buy_slice_v2(
            tickers=[product_id],
            amount_usd=buy_amount,
            dry_run=True,
        )

        if not success:
            raise Exception(f"Error buying {product_id}: {str(messages)}")

    def __sell_product(self, product_id: str, owned_product: float):
        self.logger.info(f"Sell {owned_product} shares of {product_id}")
        messages, success = self.schwab_api.trade_v2(
            ticker=product_id,
            qty=owned_product,
            side="Sell",
            dry_run=True,
        )

        if not success:
            raise Exception(f"Error selling {product_id}: {str(messages)}")

    def create_orders(self):
        shuffled_product_ids = self.product_ids
        random.shuffle(shuffled_product_ids)

        account_info = self.schwab_api.get_account_info_v2()

        available_cash = account_info["available_cash"]
        self.logger.info(f"Available cash: ${available_cash:.2f}")

        owned_products = [x["symbol"] for x in account_info["positions"]]
        self.logger.info(f"Owned products: {owned_products}")

        for position in account_info["positions"]:
            if position["symbol"] not in self.product_ids:
                self.__sell_product(product_id=position["symbol"], owned_product=position["quantity"])

        min_buy_amount = 5
        max_buy_amount = 100
        self.logger.info(f"Min buy amount: ${min_buy_amount}")
        self.logger.info(f"Max buy amount: ${max_buy_amount}")

        total_products = len(self.product_ids)
        buy_amount = self.floor_value(value=(available_cash / total_products / 10), precision=2)
        buy_amount = buy_amount if buy_amount >= min_buy_amount else min_buy_amount
        buy_amount = buy_amount if buy_amount <= max_buy_amount else max_buy_amount
        self.logger.info(f"Buy amount: ${buy_amount:.2f}")

        if (buy_amount * total_products) > available_cash:
            self.logger.info(f"Not enough funds to buy")

        for product_id in self.product_ids:
            pass

    def test_orders(self):
        account_info = self.schwab_api.get_account_info_v2()
        self.logger.info("Account info:")
        self.logger.info(account_info)

        self.logger.info("AAPL stock quote:")
        quotes = self.schwab_api.quote_v2(["AAPL"])
        self.logger.info(quotes)

        self.logger.info("Placing a dry run sell trade for AAPL stock")
        messages, success = self.schwab_api.trade_v2(
            ticker="AAPL",
            qty=0.0256,
            side="Sell",
            dry_run=True,
        )

        self.logger.info("The order verification was " + "successful" if success else "unsuccessful")
        self.logger.info("The order verification produced the following messages:")
        self.logger.info(messages)

        self.logger.info("Placing a dry run trade for buy slice AAPL and GOOG stock")
        messages, success = self.schwab_api.buy_slice_v2(
            tickers=["AAPL", "GOOG"],
            amount_usd=10,
            dry_run=True,
        )

        self.logger.info("The order verification was " + "successful" if success else "unsuccessful")
        self.logger.info("The order verification produced the following messages:")
        self.logger.info(messages)
