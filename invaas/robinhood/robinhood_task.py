import json
import pandas as pd
import pandas_ta as ta
import pyotp
import time
import yfinance as yf

from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from datetime import datetime, timedelta, timezone

import invaas.robinhood.robinhood_api as robinhood_api

from invaas.task import Task
from invaas.schwab.cnn_fear_greed_index import get_historical_cnn_fear_greed_index


class RobinhoodTask(Task):
    """
    Task class to execute ETL processes for loading and preparing data.
    """

    def __init__(self, env: str):
        super().__init__(env=env)
        self.symbol = "SPY"
        self.periods = 20
        self.robinhood_api = robinhood_api
        # self.previous_run_output = self.__get_previous_run_output()

    def setup_api(self):
        self.logger.info("Logging in to Robinhood")
        client_id = self.get_secret(key="ROBINHOOD-CLIENT-ID")
        username = self.get_secret(key="ROBINHOOD-USERNAME")
        password = self.get_secret(key="ROBINHOOD-PASSWORD")
        totp = pyotp.TOTP(self.get_secret(key="ROBINHOOD-TOTP-SECRET")).now()

        self.robinhood_api.login(client_id=client_id, username=username, password=password, mfa_code=totp)

        self.available_cash = self.__get_available_cash()
        self.logger.info(f"Available cash: ${self.available_cash:.2f}")

    # def exit_with_output(self):
    #     self.dbutils.notebook.exit(
    #         json.dumps(
    #             {
    #                 "current_fear_greed_index": self.current_fear_greed_index,
    #                 "sold_options": self.sold_options,
    #                 "bought_options": self.bought_options,
    #             }
    #         )
    #     )

    # def create_options_orders(self):
    #     asset_class = "Option"
    #     ticker = "SPY"
    #     self.logger.info(f"Trading options for {ticker}")

    #     self.schwab_api.get_account_info()
    #     transaction_history = self.schwab_api.get_transaction_history()
    #     df_options_chain = self.__get_df_options_chain(ticker=ticker)
    #     owned_options, owned_call_options, owned_put_options = self.__get_owned_options()

    #     (
    #         current_fear_greed_index_timestamp,
    #         current_fear_greed_index,
    #         previous_max_fear_greed_index,
    #         previous_min_fear_greed_index,
    #     ) = self.__get_fear_greed_index_data(historical_periods=5)

    #     # (
    #     #     current_vix,
    #     #     current_vix_sma,
    #     # ) = self.__get_cboe_vix_data()

    #     fear_greed_index_diff = 0
    #     good_call_buy = current_fear_greed_index >= previous_max_fear_greed_index - fear_greed_index_diff
    #     good_put_buy = current_fear_greed_index <= previous_min_fear_greed_index + fear_greed_index_diff
    #     min_dte_sell = 7
    #     sold_options = 0
    #     max_loss_pct = -0.1
    #     min_gain_pct = 0.1

    #     self.logger.info(f"Buy calls: {good_call_buy}")
    #     self.logger.info(f"Buy puts: {good_put_buy}")
    #     self.logger.info(f"Min DTE to sell: {min_dte_sell}")
    #     self.logger.info(f"Max loss to sell: {int(max_loss_pct*100)}%")
    #     self.logger.info(f"Min gain to sell: {int(min_gain_pct*100)}%")
    #     print()

    #     def get_owned_option_transaction(owned_option):
    #         return next(
    #             transaction
    #             for transaction in transaction_history["brokerageTransactions"]
    #             if transaction["symbol"] == owned_option["displaySymbol"]
    #         )

    #     def get_sell_dte(current_dte, expire_date, owned_option_transaction):
    #         purchase_dte = (
    #             expire_date
    #             - datetime.strptime(
    #                 owned_option_transaction["transactionDate"],
    #                 "%m/%d/%Y",
    #             )
    #         ).days
    #         return current_dte < min_dte_sell or (purchase_dte - current_dte) > min_dte_sell

    #     for index, row in df_options_chain.iterrows():
    #         owned_call_option = next((option for option in owned_call_options if option["symbol"] == row.C_ID), None)
    #         owned_put_option = next((option for option in owned_put_options if option["symbol"] == row.P_ID), None)

    #         if owned_call_option is not None:
    #             self.logger.info(f"Reviewing owned option '{row.C_ID}'")
    #             owned_call_option_transaction = get_owned_option_transaction(owned_call_option)
    #             call_sell_dte = get_sell_dte(
    #                 current_dte=row.DTE,
    #                 expire_date=row.EXPIRE_DATE,
    #                 owned_option_transaction=owned_call_option_transaction,
    #             )
    #             self.logger.info(f"Sell option based on DTE: {call_sell_dte}")
    #             call_purchase_price = float(owned_call_option_transaction["executionPrice"].replace("$", "")) * 100
    #             call_bid_price = row.C_BID * 100
    #             call_price_pct_diff = (call_bid_price - call_purchase_price) / call_purchase_price
    #             self.logger.info(f"Option price diff: {call_price_pct_diff*100:.2f}%")
    #             min_call_gain = call_price_pct_diff > min_gain_pct
    #             max_call_loss = call_price_pct_diff < max_loss_pct

    #             if not good_call_buy or call_sell_dte or min_call_gain or max_call_loss:
    #                 sell_contracts_quantity = int(owned_call_option["shares"])
    #                 self.logger.info(
    #                     f"Selling {sell_contracts_quantity} contracts of '{row.C_ID}' for ${call_bid_price:.2f}"
    #                 )
    #                 self.__sell_product(
    #                     product_id=row.C_ID,
    #                     asset_class=asset_class,
    #                     quantity=sell_contracts_quantity,
    #                 )
    #                 sold_options += 1

    #             print()
    #         elif owned_put_option is not None:
    #             self.logger.info(f"Reviewing owned option '{row.C_ID}'")
    #             owned_put_option_transaction = get_owned_option_transaction(owned_put_option)
    #             put_sell_dte = get_sell_dte(
    #                 current_dte=row.DTE,
    #                 expire_date=row.EXPIRE_DATE,
    #                 owned_option_transaction=owned_put_option_transaction,
    #             )
    #             self.logger.info(f"Sell option based on DTE: {put_sell_dte}")
    #             put_purchase_price = float(owned_put_option_transaction["executionPrice"].replace("$", "")) * 100
    #             put_bid_price = row.P_BID * 100
    #             put_price_pct_diff = (put_bid_price - put_purchase_price) / put_purchase_price
    #             self.logger.info(f"Option price diff: {put_price_pct_diff*100:.2f}%")
    #             min_put_gain = put_price_pct_diff > min_gain_pct
    #             max_put_loss = put_price_pct_diff < max_loss_pct

    #             if not good_put_buy or put_sell_dte or min_put_gain or max_put_loss:
    #                 sell_contracts_quantity = int(owned_put_option["shares"])
    #                 self.logger.info(
    #                     f"Selling {sell_contracts_quantity} contracts of '{row.P_ID}' for ${put_bid_price:.2f}"
    #                 )
    #                 self.__sell_product(
    #                     product_id=row.P_ID,
    #                     asset_class=asset_class,
    #                     quantity=sell_contracts_quantity,
    #                 )
    #                 sold_options += 1

    #             print()

    #     if sold_options > 0:
    #         time.sleep(10)

    #     df_options_chain = self.__get_df_options_chain(ticker=ticker)
    #     owned_options, owned_call_options, owned_put_options = self.__get_owned_options()

    #     options_bought_today_symbols = [
    #         x["symbol"]
    #         for x in self.schwab_api.get_transaction_history()["brokerageTransactions"]
    #         if (
    #             x["action"] == "Buy to Open"
    #             and x["transactionDate"] == datetime.now(timezone.utc).strftime("%m/%d/%Y")
    #             and (x["description"].startswith("CALL") or x["description"].startswith("PUT"))
    #         )
    #     ]
    #     options_bought_today = len(options_bought_today_symbols)
    #     owned_options_bought_today = len(
    #         [x for x in owned_options if x["displaySymbol"] in options_bought_today_symbols]
    #     )

    #     available_cash = self.__get_available_cash_to_buy_options()
    #     buy_price_divisor = 3
    #     min_dte_buy = int(min_dte_sell * 2)
    #     min_volume = 100
    #     max_strike_distance_pct = 0.02
    #     max_buy_price = 1000
    #     max_buy_amount = 1
    #     buy_contracts_quantity = 1
    #     bought_options = 0

    #     self.logger.info(f"Available cash to buy options: {available_cash}")
    #     self.logger.info(f"Buy price: {int(100/buy_price_divisor)}% of available cash")
    #     self.logger.info(f"Max buy price: ${max_buy_price}")
    #     self.logger.info(f"Max unique options to buy: {max_buy_amount}")
    #     self.logger.info(f"Total options bought today: {options_bought_today}")
    #     self.logger.info(f"Total owned options bought today: {owned_options_bought_today}")
    #     self.logger.info(f"Buy contracts quantity: {buy_contracts_quantity}")
    #     self.logger.info(f"Min DTE to buy: {min_dte_buy}")
    #     self.logger.info(f"Min volume: {min_volume}")
    #     self.logger.info(f"Max strike distance: {int(max_strike_distance_pct*100)}%")
    #     print()

    #     if current_fear_greed_index_timestamp < (datetime.now(timezone.utc) - timedelta(minutes=20)):
    #         self.logger.info("Fear and greed index not updated recently")
    #     # elif options_bought_today >= max_buy_amount:
    #     elif owned_options_bought_today >= max_buy_amount:
    #         self.logger.info("Reached max allowed owned options bought today")
    #     else:
    #         for index, row in df_options_chain.iterrows():
    #             call_ask_price = row.C_ASK * 100
    #             put_ask_price = row.P_ASK * 100
    #             current_max_buy_price = available_cash / buy_contracts_quantity / buy_price_divisor
    #             current_max_buy_price = (
    #                 current_max_buy_price if current_max_buy_price < max_buy_price else max_buy_price
    #             )

    #             if (
    #                 row.DTE > min_dte_buy
    #                 and row.STRIKE_DISTANCE_PCT <= max_strike_distance_pct
    #                 and bought_options < max_buy_amount
    #             ):
    #                 if (
    #                     good_call_buy
    #                     and row.C_VOLUME > min_volume
    #                     # and row.UNDERLYING_LAST > row.STRIKE
    #                     and call_ask_price <= current_max_buy_price
    #                     and len([x for x in owned_call_options if x["symbol"] == row.C_ID]) == 0
    #                 ):
    #                     self.logger.info(
    #                         f"Buying {buy_contracts_quantity} contracts of '{row.C_ID}' for ${call_ask_price:.2f}"
    #                     )
    #                     self.__buy_product(
    #                         product_id=row.C_ID, asset_class=asset_class, quantity=buy_contracts_quantity
    #                     )
    #                     available_cash -= call_ask_price
    #                     bought_options += 1
    #                 elif (
    #                     good_put_buy
    #                     and row.P_VOLUME > min_volume
    #                     # and row.UNDERLYING_LAST < row.STRIKE
    #                     and put_ask_price <= current_max_buy_price
    #                     and len([x for x in owned_put_options if x["symbol"] == row.P_ID]) == 0
    #                 ):
    #                     self.logger.info(
    #                         f"Buying {buy_contracts_quantity} contracts of '{row.P_ID}' for ${put_ask_price:.2f}"
    #                     )
    #                     self.__buy_product(
    #                         product_id=row.P_ID, asset_class=asset_class, quantity=buy_contracts_quantity
    #                     )
    #                     available_cash -= put_ask_price
    #                     bought_options += 1

    #     self.current_fear_greed_index = current_fear_greed_index
    #     self.sold_options = sold_options
    #     self.bought_options = bought_options

    # def __get_previous_run_output(self):
    #     if self.env == "local":
    #         return []

    #     previous_run_output = []
    #     previous_start_time_ms = (datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000
    #     previous_job_run_ids = [
    #         x.run_id
    #         for x in self.workspace_client.jobs.list_runs(
    #             job_id=self.job_id,
    #             start_time_from=previous_start_time_ms,
    #         )
    #         if (
    #             x.state.life_cycle_state == RunLifeCycleState.TERMINATED
    #             and x.state.result_state == RunResultState.SUCCESS
    #         )
    #     ]

    #     for job_run_id in previous_job_run_ids:
    #         job_run = self.workspace_client.jobs.get_run(run_id=job_run_id)
    #         job_run_output = self.workspace_client.jobs.get_run_output(job_run.tasks[0].run_id)

    #         if job_run_output.notebook_output and job_run_output.notebook_output.result:
    #             try:
    #                 notebook_output = json.loads(job_run_output.notebook_output.result)
    #                 previous_run_output.append(notebook_output)
    #             except Exception as e:
    #                 self.logger.warning(f"Error getting notebook output: {e}")

    #     return previous_run_output

    def get_rsi(self):
        df_history = yf.Ticker(self.symbol).history(interval="1d", period="1y")
        df_history.columns = map(str.lower, df_history.columns)

        timestamps = [pd.to_datetime(x, utc=True).round(freq="D") for x in df_history.index.values]
        timestamps_date_range = pd.date_range(start=timestamps[0], end=timestamps[-1], freq="D")
        df_history.sort_index(ascending=True, inplace=True)
        df_history = df_history.set_index(pd.DatetimeIndex(timestamps)).reindex(timestamps_date_range, method="ffill")
        # df_history = df_history.set_index(pd.DatetimeIndex(timestamps))
        df_history.sort_index(ascending=True, inplace=True)
        df_history["close"] = df_history.close.ffill()

        CustomStrategy = ta.Strategy(
            name="Momo and Volatility",
            ta=[
                {"kind": "rsi", "length": self.periods},
            ],
        )
        df_history.ta.strategy(CustomStrategy)
        df_history.sort_index(ascending=True, inplace=True)

        rsi = round(float(df_history.to_dict(orient="records")[-1][f"RSI_{self.periods}"]))

        self.logger.info(f"Current {self.symbol} RSI: {rsi}")

        return rsi

    def get_fear_greed_index_data(self, historical_periods: int = None):
        if historical_periods is None:
            historical_periods = self.periods

        df = pd.DataFrame(data=get_historical_cnn_fear_greed_index()["data"])
        timestamps = [pd.Timestamp(x, unit="ms", tz="UTC") for x in df.x]
        timestamps_date_range = pd.date_range(start=timestamps[0], end=timestamps[-1], freq="D")
        df = df.set_index(pd.DatetimeIndex(timestamps)).reindex(timestamps_date_range, method="ffill")
        # df = df.set_index(pd.DatetimeIndex(timestamps))
        df.sort_index(ascending=True, inplace=True)
        df.rename(columns={"y": "fear_greed_index"}, inplace=True)
        df["fear_greed_index"] = df.fear_greed_index.astype(float).round()
        df["fear_greed_index"] = df.fear_greed_index.ffill()
        df["previous_max_fear_greed_index"] = (
            df["fear_greed_index"].rolling(window=historical_periods, min_periods=historical_periods).max()
        )
        df["previous_min_fear_greed_index"] = (
            df["fear_greed_index"].rolling(window=historical_periods, min_periods=historical_periods).min()
        )

        current_fear_greed_index_timestamp = df.index[-1]
        current_fear_greed_index = round(df.fear_greed_index.values[-1])

        self.logger.info(
            f"Current fear greed index timestamp: {current_fear_greed_index_timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.logger.info(f"Current fear greed index: {current_fear_greed_index}")

        previous_max_fear_greed_index = round(df.previous_max_fear_greed_index.values[-1])
        previous_min_fear_greed_index = round(df.previous_min_fear_greed_index.values[-1])

        # previous_run_fear_greed_indexes = [int(x["current_fear_greed_index"]) for x in self.previous_run_output]

        # if len(previous_run_fear_greed_indexes) > 0:
        #     previous_run_max_fear_greed_index = max(previous_run_fear_greed_indexes)
        #     previous_run_min_fear_greed_index = min(previous_run_fear_greed_indexes)

        #     if previous_run_max_fear_greed_index > previous_max_fear_greed_index:
        #         previous_max_fear_greed_index = previous_run_max_fear_greed_index

        #     if previous_run_min_fear_greed_index < previous_min_fear_greed_index:
        #         previous_min_fear_greed_index = previous_run_min_fear_greed_index

        self.logger.info(f"Previous max fear greed index: {previous_max_fear_greed_index}")
        self.logger.info(f"Previous min fear greed index: {previous_min_fear_greed_index}")

        return (
            current_fear_greed_index_timestamp,
            current_fear_greed_index,
            previous_max_fear_greed_index,
            previous_min_fear_greed_index,
        )

    # def __get_cboe_vix_data(self):
    #     vix_ticker = "^VIX"

    #     current_vix = (
    #         yf.Ticker(vix_ticker)
    #         .history(interval="1m", period="1d")
    #         .reset_index()
    #         .sort_values(by="Datetime", ascending=True)
    #         .Close.values[-1]
    #     )

    #     df_vix_history = yf.Ticker(vix_ticker).history(interval="1d", period="1y")

    #     timestamps = [pd.to_datetime(x, utc=True).round(freq="D") for x in df_vix_history.index.values]
    #     timestamps_date_range = pd.date_range(start=timestamps[0], end=timestamps[-1], freq="D")
    #     df_vix_history = df_vix_history.set_index(pd.DatetimeIndex(timestamps)).reindex(
    #         timestamps_date_range, method="ffill"
    #     )

    #     df_vix_history.ta.sma(length=50, append=True)
    #     df_vix_history.columns = [f"{vix_ticker}_{x}" for x in map(str.lower, df_vix_history.columns)]
    #     current_vix_sma = df_vix_history[f"{vix_ticker}_sma_50"].values[-1]

    #     self.logger.info(f"Current VIX: {current_vix:.2f}")
    #     self.logger.info(f"Current VIX SMA: {current_vix_sma:.2f}")

    #     return current_vix, current_vix_sma

    # def __get_df_options_chain(self, ticker: str):
    #     data = []
    #     options_chains_data = self.schwab_api.get_options_chains(ticker=ticker)
    #     underlying_last = float(options_chains_data["UnderlyingData"]["Last"])

    #     for expiration in options_chains_data["Expirations"]:
    #         days_until_expiration = expiration["ExpirationGroup"]["DaysUntil"]
    #         expiration_month_day = expiration["ExpirationGroup"]["MonthAndDay"].split(" ")
    #         expiration_month = expiration_month_day[0]
    #         expiration_day = expiration_month_day[1]
    #         expiration_year = expiration["ExpirationGroup"]["Year"]
    #         expiration_date = datetime.strptime(f"{expiration_month} {expiration_day} {expiration_year}", "%b %d %Y")

    #         for chain in expiration["Chains"]:
    #             data_obj = {}
    #             data_obj["OPTION_ID"] = chain["SymbolGroup"]
    #             data_obj["UNDERLYING_LAST"] = underlying_last
    #             data_obj["EXPIRE_DATE"] = expiration_date
    #             data_obj["DTE"] = days_until_expiration

    #             for leg in chain["Legs"]:
    #                 data_obj["STRIKE"] = float(leg["Strk"])
    #                 data_obj["STRIKE_DISTANCE"] = abs(data_obj["STRIKE"] - underlying_last)
    #                 data_obj["STRIKE_DISTANCE_PCT"] = data_obj["STRIKE_DISTANCE"] / underlying_last
    #                 leg_prefix = leg["OptionType"] + "_"
    #                 data_obj[leg_prefix + "ID"] = leg["Sym"]
    #                 data_obj[leg_prefix + "BID"] = float(leg["Bid"])
    #                 data_obj[leg_prefix + "ASK"] = float(leg["Ask"])
    #                 data_obj[leg_prefix + "VOLUME"] = int(leg["Vol"])

    #             data.append(data_obj)

    #     df = pd.DataFrame(data=data)
    #     df.sort_values(by=["EXPIRE_DATE", "DTE", "STRIKE_DISTANCE_PCT"], ascending=True, inplace=True)

    #     return df

    # def __get_owned_options(self):
    #     positions = self.schwab_api.get_balance_positions()["positionDetails"].get("positions", [])
    #     owned_options = [x for x in positions if x["securityType"] == "Option" and x["shares"] > 0]
    #     owned_call_options = [x for x in owned_options if x["symbolDescription"].startswith("CALL")]
    #     owned_put_options = [x for x in owned_options if x["symbolDescription"].startswith("PUT")]
    #     return owned_options, owned_call_options, owned_put_options

    # def __buy_product(self, product_id: str, asset_class: str, quantity: float = None, buy_price: float = None):
    #     if quantity is None and buy_price is None:
    #         raise Exception("Must include quantity or buy price")
    #     elif quantity is not None:
    #         messages, success = self.schwab_api.trade(
    #             ticker=product_id,
    #             asset_class=asset_class,
    #             side="Buy",
    #             qty=quantity,
    #             dry_run=(self.env == "local"),
    #         )

    #         if not success:
    #             raise Exception(f"Error buying {asset_class} {product_id}: {str(messages)}")
    #     elif buy_price is not None:
    #         messages, success = self.schwab_api.buy_slice(
    #             tickers=[product_id],
    #             price_usd=buy_price,
    #             dry_run=(self.env == "local"),
    #         )

    #         if not success:
    #             raise Exception(f"Error buying {asset_class} {product_id}: {str(messages)}")

    # def __sell_product(self, product_id: str, asset_class: str, quantity: float):
    #     messages, success = self.schwab_api.trade(
    #         ticker=product_id,
    #         asset_class=asset_class,
    #         side="Sell",
    #         qty=quantity,
    #         dry_run=(self.env == "local"),
    #     )

    #     if not success:
    #         raise Exception(f"Error selling {product_id}: {str(messages)}")

    def __get_available_cash(self):
        return float(self.robinhood_api.load_account_profile()["portfolio_cash"])
