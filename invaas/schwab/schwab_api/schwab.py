import json
import re
import requests

from invaas.schwab.schwab_api import urls
from invaas.schwab.schwab_api.account_information import Position, Account
from invaas.schwab.schwab_api.authentication import SessionManager


class Schwab(SessionManager):
    def __init__(self, **kwargs):
        """
        The Schwab class. Used to interact with schwab.

        """
        self.headless = kwargs.get("headless", True)
        self.browserType = kwargs.get("browserType", "firefox")
        self.schwab_account_id = kwargs.get("schwab_account_id")
        super(Schwab, self).__init__()

    def trade(
        self,
        ticker,
        side,
        qty,
        asset_class="Stock",
        dry_run=True,
        order_type=49,
        duration=48,
        limit_price=0,
        stop_price=0,
        primary_security_type=None,
        valid_return_codes={0, 10},
    ):
        """
        ticker (str) - The symbol you want to trade.
        side (str) - Either 'Buy' or 'Sell'.
        asset_class (str) - Either 'Stock' or 'Option'.
        qty (int) - The amount of shares to buy/sell.
        order_type (int) - The order type. There exists types beyond 49 (Market) and 50 (Limit).
        duration (int) - The duration type for the order.
        limit_price (number) - The limit price to set with the order, if necessary.
        stop_price (number) -  The stop price to set with the order, if necessary.
        primary_security_type (int) - The type of the security being traded.

        Returns messages (list of strings), is_success (boolean)
        """

        if side == "Buy" and asset_class == "Stock":
            buy_sell_code = "49"
            primary_security_type = 46
        elif side == "Sell" and asset_class == "Stock":
            buy_sell_code = "50"
            primary_security_type = 46
        elif side == "Buy" and asset_class == "Mutual Fund":
            buy_sell_code = "49"
            primary_security_type = 49
        elif side == "Sell" and asset_class == "Mutual Fund":
            buy_sell_code = "50"
            primary_security_type = 49
        elif side == "Buy" and asset_class == "Option":
            buy_sell_code = "201"
            primary_security_type = 48
        elif side == "Sell" and asset_class == "Option":
            buy_sell_code = "204"
            primary_security_type = 48
        else:
            raise Exception(f"Invalid side and/or type: {side}, {asset_class}")

        self.update_token(token_type="update")

        data = {
            "UserContext": {"AccountId": str(self.schwab_account_id), "AccountColor": 0},
            "OrderStrategy": {
                # Unclear what the security types map to.
                "PrimarySecurityType": primary_security_type,
                "CostBasisRequest": {"costBasisMethod": "FIFO", "defaultCostBasisMethod": "FIFO"},
                "OrderType": str(order_type),
                "LimitPrice": str(limit_price),
                "StopPrice": str(stop_price),
                "Duration": str(duration),
                "AllNoneIn": False,
                "DoNotReduceIn": False,
                "OrderStrategyType": 1,
                "OrderLegs": [
                    {
                        "Quantity": str(qty),
                        "LeavesQuantity": str(qty),
                        "Instrument": {"Symbol": ticker},
                        "SecurityType": primary_security_type,
                        "Instruction": buy_sell_code,
                    }
                ],
            },
            # OrderProcessingControl seems to map to verification vs actually placing an order.
            "OrderProcessingControl": 1,
        }

        # Adding this header seems to be necessary.
        self.headers["schwab-resource-version"] = "1.0"

        r = requests.post(urls.order_verification_v2(), json=data, headers=self.headers)

        if r.status_code != 200:
            return [r.text], False

        response = json.loads(r.text)

        orderId = response["orderStrategy"]["orderId"]
        firstOrderLeg = response["orderStrategy"]["orderLegs"][0]

        if "schwabSecurityId" in firstOrderLeg:
            data["OrderStrategy"]["OrderLegs"][0]["Instrument"]["ItemIssueId"] = firstOrderLeg["schwabSecurityId"]

        messages = list()

        for message in response["orderStrategy"]["orderMessages"]:
            messages.append(message["message"])

        # TODO: This needs to be fleshed out and clarified.
        if response["orderStrategy"]["orderReturnCode"] not in valid_return_codes:
            return messages, False

        if dry_run:
            return messages, True

        # Make the same POST request, but for real this time.
        data["UserContext"]["CustomerId"] = 0
        data["OrderStrategy"]["OrderId"] = int(orderId)
        data["OrderProcessingControl"] = 2
        self.update_token(token_type="update")
        r = requests.post(urls.order_verification_v2(), json=data, headers=self.headers)

        if r.status_code != 200:
            return [r.text], False

        response = json.loads(r.text)

        messages = list()

        if "orderMessages" in response["orderStrategy"] and response["orderStrategy"]["orderMessages"] is not None:
            for message in response["orderStrategy"]["orderMessages"]:
                messages.append(message["message"])

        if response["orderStrategy"]["orderReturnCode"] in valid_return_codes:
            return messages, True

        return messages, False

    def buy_slice(
        self,
        tickers,
        price_usd,
        dry_run=True,
        valid_return_codes={20, 25},
    ):
        """
        tickers (List[str]) - The stock symbols you want to trade.
        price_usd (int) - The total dollar price to buy (min $5/stock).

        Returns messages (list of strings), is_success (boolean)
        """

        self.update_token(token_type="update")

        stocks = []

        for ticker in tickers:
            stocks.append(
                {
                    "Symbol": ticker,
                    "IsAffirmed": True,
                    "IsSelected": True,
                    "ReinvestDividend": True,
                    "ReinvestCapitalGains": True,
                    "OrderId": 0,
                    "SchwabOrderId": 0,
                }
            )

        data = {
            "AccountNo": self.schwab_account_id,
            "TotalAmount": price_usd,
            "IsAffirmed": True,
            "OrderBundleId": 0,
            "Stocks": stocks,
        }

        # Adding this header seems to be necessary.
        self.headers["schwab-resource-version"] = "1.0"

        request_headers = self.headers
        request_headers["host"] = "jfkgateway.schwab.com"

        r = requests.post(urls.bundle_order_verification_v2(), json=data, headers=request_headers)
        if r.status_code != 200:
            return [r.text], False

        response = json.loads(r.text)
        messages = list()

        for message in response["BundleMessages"]:
            messages.append(message)

        # TODO: This needs to be fleshed out and clarified.
        if response["OesReturnCode"] not in valid_return_codes:
            return messages, False

        if dry_run:
            return messages, True

        # Make the same PUT request, but for real this time
        data["OrderBundleId"] = int(response["OrderBundleId"])

        for stock_order in response["StockOrders"]:
            for stock in data["Stocks"]:
                if stock["Symbol"] == stock_order["Quote"]["Symbol"]:
                    stock["Amount"] = int(stock_order["EstimatedAmount"])
                    stock["OrderId"] = int(stock_order["OrderId"])
                    stock["SchwabOrderId"] = int(stock_order["SchwabOrderId"])

        self.update_token(token_type="update")
        r = requests.put(urls.bundle_order_verification_v2(), json=data, headers=request_headers)

        if r.status_code != 200:
            return [r.text], False

        response = json.loads(r.text)
        messages = list()

        for message in response["BundleMessages"]:
            messages.append(message)

        if response["OesReturnCode"] in valid_return_codes:
            return messages, True

        return messages, False

    def quote(self, tickers):
        """
        quote_v2 takes a list of Tickers, and returns Quote information through the Schwab API.
        """
        data = {"Symbols": tickers, "IsIra": False, "AccountRegType": "S3"}

        # Adding this header seems to be necessary.
        self.headers["schwab-resource-version"] = "1.0"

        self.update_token(token_type="update")
        r = requests.post(urls.ticker_quotes_v2(), json=data, headers=self.headers)
        if r.status_code != 200:
            return [r.text], False

        response = json.loads(r.text)
        return response["quotes"]

    def orders(self):
        """
        orders_v2 returns a list of orders for a Schwab Account. It is unclear to me how to filter by specific account.

        Currently, the query parameters are hard coded to return ALL orders, but this can be easily adjusted.
        """

        self.update_token(token_type="api")
        self.headers["schwab-resource-version"] = "2.0"
        self.headers["schwab-client-account"] = self.schwab_account_id
        r = requests.get(urls.orders_v2(), headers=self.headers)
        if r.status_code != 200:
            return [r.text], False

        response = json.loads(r.text)
        return response["Orders"]

    def get_balance_positions(self):
        self.update_token(token_type="api")
        r = requests.get(urls.balance_positions_v2(), headers=self.headers)
        return json.loads(r.text)

    def get_account_info(self):
        self.update_token(token_type="api")
        r = requests.get(urls.positions_v2(), headers=self.headers)
        response = json.loads(r.text)

        for account in response["accounts"]:
            positions = list()

            for security_group in account["groupedPositions"]:
                if security_group["groupName"] == "Cash":
                    continue

                for position in security_group["positions"]:
                    positions.append(
                        Position(
                            position["symbolDetail"]["symbol"],
                            position["symbolDetail"]["description"],
                            float(position["quantity"]),
                            (
                                0
                                if "costDetail" not in position
                                else float(position["costDetail"]["costBasisDetail"]["costBasis"])
                            ),
                            0 if "priceDetail" not in position else float(position["priceDetail"]["marketValue"]),
                        )._as_dict()
                    )

            if account["accountId"] == self.schwab_account_id:
                return Account(
                    account["accountId"],
                    positions,
                    account["totals"]["marketValue"],
                    account["totals"]["cashInvestments"],
                    account["totals"]["accountValue"],
                    account["totals"].get("costBasis", 0),
                )._as_dict()

        return None

    def update_token(self, token_type="api"):
        r = self.session.get(f"https://client.schwab.com/api/auth/authorize/scope/{token_type}")
        if not r.ok:
            raise ValueError(f"Error updating Bearer token: {r.reason}")
        token = json.loads(r.text)["token"]
        self.headers["authorization"] = f"Bearer {token}"

    async def get_equity_rating(self, ticker: str):
        async with self.page.expect_navigation():
            await self.page.goto(f"https://client.schwab.com/app/research/#/stocks/{ticker}", timeout=60000)
        equity_rating_section = await self.page.get_by_text("Percentile Ranking =").inner_text(timeout=60000)
        return int(equity_rating_section.split(" = ")[-1])

    def get_options_chains(self, ticker: str):
        options_info = requests.get(f"https://client.schwab.com/symlup/OptionsCwp/Underlying/{ticker}.txt").text
        options_info = re.sub(r"[\n\t\r]*", "", options_info)
        options_info = json.loads(options_info.replace('SuggestionBox.JsonpCallback("option",', "")[:-1])
        expirations = [x["Date"] for x in options_info["Expirations"]]
        options_chain_url = (
            f"{urls.options_chain_v2()}/chains?Symbol={ticker}&IncludeGreeks=false&{','.join(expirations)}"
        )
        self.update_token("update")
        return json.loads(requests.get(options_chain_url, headers=self.headers).text)

    def get_transaction_history(self):
        self.update_token("update")
        return json.loads(
            requests.post(
                urls.transaction_history_v2(),
                headers=self.headers,
                json={
                    "exportType": "Csv",
                    "includeOptionsInSearch": False,
                    "selectedTransactionTypes": [
                        "Adjustments",
                        "AtmActivity",
                        "BillPay",
                        "CorporateActions",
                        "Checks",
                        "Deposits",
                        "DividendsAndCapitalGains",
                        "ElectronicTransfers",
                        "Fees",
                        "Interest",
                        "Misc",
                        "SecurityTransfers",
                        "Taxes",
                        "Trades",
                        "VisaDebitCard",
                        "Withdrawals",
                    ],
                    "symbol": "",
                    "timeFrame": "All",
                    "bookmark": None,
                    "shouldPaginate": True,
                    "selectedAccountId": self.schwab_account_id,
                    "sortColumn": "Date",
                    "sortDirection": "Descending",
                    "accountNickname": "Individual",
                },
            ).text
        )
