import pandas as pd
import pandas_ta as ta
import pyotp
import time
import yfinance as yf

from datetime import datetime

from invaas.task import Task
from invaas.schwab.schwab_api.schwab import Schwab
from invaas.schwab.cnn_fear_greed_index import get_historical_cnn_fear_greed_index


class SchwabTask(Task):
    """
    Task class to execute ETL processes for loading and preparing data.
    """

    def __init__(self, env: str):
        super().__init__(env=env)
        self.schwab_api = Schwab(schwab_account_id=self.get_secret("SCHWAB-ACCOUNT-ID"))

    async def setup_api(self):
        self.logger.info("Logging in to Schwab")
        username = self.get_secret(key="SCHWAB-USERNAME")
        password = self.get_secret(key="SCHWAB-PASSWORD") + str(
            pyotp.TOTP(self.get_secret(key="SCHWAB-TOTP-SECRET")).now()
        )

        await self.schwab_api.setup()
        await self.schwab_api.login(username=username, password=password)
        await self.schwab_api.close_api_session()
        self.logger.info(f"Net worth: {self.__get_net_worth()}")
        self.logger.info(f"Available cash to buy stocks: {self.__get_available_cash_to_buy_stocks()}")
        self.logger.info(f"Available cash to buy options: {self.__get_available_cash_to_buy_options()}")

    def __get_fear_greed_index_data(self):
        historical_periods = 14
        df = pd.DataFrame(data=get_historical_cnn_fear_greed_index()["data"])
        df.set_index(
            pd.DatetimeIndex([pd.Timestamp(x, unit="ms", tz="UTC") for x in df.x]),
            inplace=True,
        )
        df.rename(columns={"y": "fear_greed_index"}, inplace=True)
        df.sort_index(inplace=True)
        df["fear_greed_index"] = df.fear_greed_index.astype(float).round()
        df["previous_max_fear_greed_index"] = (
            df["fear_greed_index"].rolling(window=historical_periods, min_periods=historical_periods).max()
        )
        df["previous_min_fear_greed_index"] = (
            df["fear_greed_index"].rolling(window=historical_periods, min_periods=historical_periods).min()
        )

        current_fear_greed_index = round(df.fear_greed_index.values[-1])
        previous_max_fear_greed_index = round(df.previous_max_fear_greed_index.values[-1])
        previous_min_fear_greed_index = round(df.previous_min_fear_greed_index.values[-1])

        self.logger.info(f"Current fear greed index: {current_fear_greed_index}")
        self.logger.info(f"Previous max fear greed index: {previous_max_fear_greed_index}")
        self.logger.info(f"Previous min fear greed index: {previous_min_fear_greed_index}")

        return (current_fear_greed_index, previous_max_fear_greed_index, previous_min_fear_greed_index)

    def __get_cboe_vix_data(self):
        vix_ticker = "^VIX"

        current_vix = (
            yf.Ticker(vix_ticker)
            .history(interval="1m", period="1d")
            .reset_index()
            .sort_values(by="Datetime", ascending=True)
            .Close.values[-1]
        )

        df_vix_history = yf.Ticker(vix_ticker).history(interval="1d", period="5y")

        timestamps = [pd.to_datetime(x, utc=True).round(freq="D") for x in df_vix_history.index.values]
        timestamps_date_range = pd.date_range(start=timestamps[0], end=timestamps[-1], freq="D")
        df_vix_history = df_vix_history.set_index(pd.DatetimeIndex(timestamps)).reindex(
            timestamps_date_range, method="ffill"
        )

        df_vix_history.ta.sma(length=50, append=True)
        df_vix_history.columns = [f"{vix_ticker}_{x}" for x in map(str.lower, df_vix_history.columns)]
        latest_vix_sma = df_vix_history[f"{vix_ticker}_sma_50"].values[-1]

        self.logger.info(f"Current VIX: {current_vix:.2f}")
        self.logger.info(f"Latest VIX SMA: {latest_vix_sma:.2f}")

        return current_vix, latest_vix_sma

    def __get_df_options_chain(self, ticker: str):
        data = []
        options_chains_data = self.schwab_api.get_options_chains(ticker=ticker)
        underlying_last = float(options_chains_data["UnderlyingData"]["Last"])

        for expiration in options_chains_data["Expirations"]:
            days_until_expiration = expiration["ExpirationGroup"]["DaysUntil"]
            expiration_month_day = expiration["ExpirationGroup"]["MonthAndDay"].split(" ")
            expiration_month = expiration_month_day[0]
            expiration_day = expiration_month_day[1]
            expiration_year = expiration["ExpirationGroup"]["Year"]
            expiration_date = datetime.strptime(f"{expiration_month} {expiration_day} {expiration_year}", "%b %d %Y")

            for chain in expiration["Chains"]:
                data_obj = {}
                data_obj["OPTION_ID"] = chain["SymbolGroup"]
                data_obj["UNDERLYING_LAST"] = underlying_last
                data_obj["EXPIRE_DATE"] = expiration_date
                data_obj["DTE"] = days_until_expiration

                for leg in chain["Legs"]:
                    data_obj["STRIKE"] = float(leg["Strk"])
                    data_obj["STRIKE_DISTANCE"] = abs(data_obj["STRIKE"] - underlying_last)
                    data_obj["STRIKE_DISTANCE_PCT"] = data_obj["STRIKE_DISTANCE"] / underlying_last
                    leg_prefix = leg["OptionType"] + "_"
                    data_obj[leg_prefix + "ID"] = leg["Sym"]
                    data_obj[leg_prefix + "BID"] = float(leg["Bid"])
                    data_obj[leg_prefix + "ASK"] = float(leg["Ask"])
                    data_obj[leg_prefix + "VOLUME"] = int(leg["Vol"])

                data.append(data_obj)

        df = pd.DataFrame(data=data)
        df.sort_values(by=["EXPIRE_DATE", "DTE", "STRIKE_DISTANCE_PCT"], ascending=True, inplace=True)

        return df

    def __get_owned_options(self):
        positions = self.schwab_api.get_balance_positions()["positionDetails"].get("positions", [])
        owned_option_positions = [x for x in positions if x["securityType"] == "Option" and x["shares"] > 0]
        owned_call_options = [x for x in owned_option_positions if x["symbolDescription"].startswith("CALL")]
        owned_put_options = [x for x in owned_option_positions if x["symbolDescription"].startswith("PUT")]
        return owned_call_options, owned_put_options

    def __buy_product(self, product_id: str, asset_class: str, quantity: float = None, buy_price: float = None):
        if quantity is None and buy_price is None:
            raise Exception("Must include quantity or available cash to buy")
        elif quantity is not None:
            messages, success = self.schwab_api.trade(
                ticker=product_id,
                asset_class=asset_class,
                side="Buy",
                qty=quantity,
                dry_run=(self.env == "local"),
            )

            if not success:
                raise Exception(f"Error buying {asset_class} {product_id}: {str(messages)}")
        elif buy_price is not None:
            messages, success = self.schwab_api.buy_slice(
                tickers=[product_id],
                price_usd=buy_price,
                dry_run=(self.env == "local"),
            )

            if not success:
                raise Exception(f"Error buying {asset_class} {product_id}: {str(messages)}")

    def __sell_product(self, product_id: str, asset_class: str, quantity: float):
        messages, success = self.schwab_api.trade(
            ticker=product_id,
            asset_class=asset_class,
            side="Sell",
            qty=quantity,
            dry_run=(self.env == "local"),
        )

        if not success:
            raise Exception(f"Error selling {product_id}: {str(messages)}")

    def __get_net_worth(self):
        return self.schwab_api.get_balance_positions()["balanceDetails"]["availableToTradeBalances"]["netWorth"]

    def __get_available_cash_to_buy_stocks(self):
        return self.schwab_api.get_balance_positions()["balanceDetails"]["availableToTradeBalances"]["cash"]

    def __get_available_cash_to_buy_options(self):
        return self.schwab_api.get_balance_positions()["balanceDetails"]["optionsBalances"]["longOptions"]

    def create_options_orders(self):
        ticker = "SPY"
        self.logger.info(f"Trading options for {ticker}")

        self.schwab_api.get_account_info()
        transaction_history = self.schwab_api.get_transaction_history()

        (
            current_fear_greed_index,
            previous_max_fear_greed_index,
            previous_min_fear_greed_index,
        ) = self.__get_fear_greed_index_data()

        (
            current_vix,
            latest_vix_sma,
        ) = self.__get_cboe_vix_data()

        good_call_buy = current_fear_greed_index >= previous_max_fear_greed_index - 2 and current_vix < latest_vix_sma
        good_put_buy = current_fear_greed_index <= previous_min_fear_greed_index + 2 and current_vix > latest_vix_sma

        self.logger.info(f"Buy calls: {good_call_buy}")
        self.logger.info(f"Buy puts: {good_put_buy}")

        df_options_chain = self.__get_df_options_chain(ticker=ticker)
        owned_call_options, owned_put_options = self.__get_owned_options()

        asset_class = "Option"
        min_dte_buy = 14
        min_dte_sell = int(min_dte_buy / 2)
        min_volume = 100
        max_strike_distance_pct = 0.02

        def get_sell_dte(owned_option, current_dte, expire_date, transaction_history):
            purchase_dte = (
                expire_date
                - datetime.strptime(
                    next(
                        transaction
                        for transaction in transaction_history["brokerageTransactions"]
                        if transaction["symbol"] == owned_option["displaySymbol"]
                    )["transactionDate"],
                    "%m/%d/%Y",
                )
            ).days
            return current_dte < min_dte_sell or (purchase_dte - current_dte) > min_dte_sell

        for index, row in df_options_chain.iterrows():
            call_bid_price = row.C_BID * 100
            put_bid_price = row.P_BID * 100
            owned_call_option = next((option for option in owned_call_options if option["symbol"] == row.C_ID), None)
            owned_put_option = next((option for option in owned_put_options if option["symbol"] == row.P_ID), None)

            if owned_call_option is not None:
                sell_dte = get_sell_dte(
                    owned_option=owned_call_option,
                    current_dte=row.DTE,
                    expire_date=row.EXPIRE_DATE,
                    transaction_history=transaction_history,
                )

                if not good_call_buy or sell_dte:
                    sell_contracts_quantity = int(owned_call_option["shares"])
                    self.logger.info(
                        f"Selling {sell_contracts_quantity} contracts of '{row.C_ID}' for ${call_bid_price:.2f}"
                    )
                    self.__sell_product(
                        product_id=row.C_ID,
                        asset_class=asset_class,
                        quantity=sell_contracts_quantity,
                    )
            elif owned_put_option is not None:
                sell_dte = get_sell_dte(
                    owned_option=owned_put_option,
                    current_dte=row.DTE,
                    expire_date=row.EXPIRE_DATE,
                    transaction_history=transaction_history,
                )

                if not good_put_buy or sell_dte:
                    sell_contracts_quantity = int(owned_put_option["shares"])
                    self.logger.info(
                        f"Selling {sell_contracts_quantity} contracts of '{row.P_ID}' for ${put_bid_price:.2f}"
                    )
                    self.__sell_product(
                        product_id=row.P_ID,
                        asset_class=asset_class,
                        quantity=sell_contracts_quantity,
                    )

        time.sleep(10)

        available_cash = self.__get_available_cash_to_buy_options()
        self.logger.info(f"Available cash to buy options: {available_cash}")

        df_options_chain = self.__get_df_options_chain(ticker=ticker)
        owned_call_options, owned_put_options = self.__get_owned_options()

        bought_options = 0
        max_buy_amount = 1
        buy_contracts_quantity = 1
        self.logger.info(f"Max unique options to buy: {max_buy_amount}")
        self.logger.info(f"Buy contracts quantity: {buy_contracts_quantity}")

        for index, row in df_options_chain.iterrows():
            call_ask_price = row.C_ASK * 100
            put_ask_price = row.P_ASK * 100
            current_max_buy_price = available_cash / buy_contracts_quantity

            if row.DTE > min_dte_buy and row.STRIKE_DISTANCE_PCT <= max_strike_distance_pct:
                if (
                    good_call_buy
                    and row.C_VOLUME > min_volume
                    and row.UNDERLYING_LAST > row.STRIKE
                    # and self.max_buy_price >= call_ask_price
                    and current_max_buy_price >= call_ask_price
                    and len([x for x in owned_call_options if x["symbol"] == row.C_ID]) == 0
                    and bought_options < max_buy_amount
                ):
                    self.logger.info(
                        f"Buying {buy_contracts_quantity} contracts of '{row.C_ID}' for ${call_ask_price:.2f}"
                    )
                    self.__buy_product(
                        product_id=row.C_ID,
                        asset_class=asset_class,
                        quantity=buy_contracts_quantity,
                        available_cash=available_cash,
                    )
                    available_cash -= call_ask_price
                    bought_options += 1
                elif (
                    good_put_buy
                    and row.P_VOLUME > min_volume
                    and row.UNDERLYING_LAST < row.STRIKE
                    # and self.max_buy_price >= put_ask_price
                    and current_max_buy_price >= put_ask_price
                    and len([x for x in owned_put_options if x["symbol"] == row.P_ID]) == 0
                    and bought_options < max_buy_amount
                ):
                    self.logger.info(
                        f"Buying {buy_contracts_quantity} contracts of '{row.P_ID}' for ${put_ask_price:.2f}"
                    )
                    self.__buy_product(
                        product_id=row.P_ID,
                        asset_class=asset_class,
                        quantity=buy_contracts_quantity,
                        available_cash=available_cash,
                    )
                    available_cash -= put_ask_price
                    bought_options += 1
