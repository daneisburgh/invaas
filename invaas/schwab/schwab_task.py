# Task class for Schwab trading

import requests

from invaas.task import Task
from invaas.schwab.cnn_fear_greed_index import get_current_cnn_fear_greed_index


class SchwabTask(Task):
    """
    Task class to execute ETL processes for loading and preparing data.
    """

    def __init__(self, env: str = "local"):
        super().__init__(env=env)

        self.product_ids = ["VUG"]
        self.logger.info(f"Products to trade: {str(self.product_ids)}")

        self.current_fear_greed_index = int(get_current_cnn_fear_greed_index().value)
        self.logger.info(f"Current fear greed index: {self.current_fear_greed_index}")

        self.min_buy_amount = 2
        self.max_buy_amount = 100
        self.logger.info(f"Min buy amount: ${self.min_buy_amount}")
        self.logger.info(f"Max buy amount: ${self.max_buy_amount}")

    def __get_available_balance(self, account_name: str):
        pass

    def __buy_product(self, product_id: str, available_cash: float):
        pass

    def __sell_product(self, product_id: str, owned_crypto: float):
        pass

    def create_orders(self):
        pass
