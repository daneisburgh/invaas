import random

from invaas.schwab.schwab_api.schwab import Schwab
from invaas.schwab.cnn_fear_greed_index import get_current_cnn_fear_greed_index
from invaas.task import Task


class SchwabTask(Task):
    """
    Task class to execute ETL processes for loading and preparing data.
    """

    def __init__(self, env: str = None):
        super().__init__(env=env)

        self.product_ids = ["VUG"]
        self.logger.info(f"Products to trade: {str(self.product_ids)}")

        self.current_fear_greed_index = int(get_current_cnn_fear_greed_index().value)
        self.logger.info(f"Current fear greed index: {self.current_fear_greed_index}")

        self.min_buy_amount = 2
        self.max_buy_amount = 100
        self.logger.info(f"Min buy amount: ${self.min_buy_amount}")
        self.logger.info(f"Max buy amount: ${self.max_buy_amount}")

    async def setup_api(self):
        self.logger.info("Logging in to Schwab")
        username = self.get_secret(key="SCHWAB-USERNAME")
        password = self.get_secret(key="SCHWAB-PASSWORD")
        self.schwab_account_id = self.get_secret("SCHWAB-ACCOUNT-ID")
        self.schwab_api = Schwab()
        await self.schwab_api.setup()
        logged_in = await self.schwab_api.login(username=username, password=password)

        if not logged_in:
            raise Exception("Unabled to log in to Schwab")

    def __get_available_balance(self, account_name: str):
        pass

    def __buy_product(self, product_id: str, available_cash: float):
        pass

    def __sell_product(self, product_id: str, owned_crypto: float):
        pass

    def create_orders(self):
        # Get information about a few tickers
        quotes = self.schwab_api.quote_v2(["AAPL"])
        self.logger.info(quotes)

        self.logger.info("Placing a dry run trade for AAPL stock")
        messages, success = self.schwab_api.trade_v2(
            ticker="AAPL",
            side="Buy",
            qty=1,
            account_id=self.schwab_account_id,
            dry_run=True,
        )

        self.logger.info("The order verification was " + "successful" if success else "unsuccessful")
        self.logger.info("The order verification produced the following messages: ")
        self.logger.info(messages)

        shuffled_product_ids = self.product_ids
        random.shuffle(shuffled_product_ids)

        for product_id in shuffled_product_ids:
            pass
