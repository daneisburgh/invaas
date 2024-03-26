def homepage():
    return "https://www.schwab.com/"


# Old API
def positions_data():
    return "https://client.schwab.com/api/PositionV2/PositionsDataV2"


def order_verification():
    return "https://client.schwab.com/api/ts/stamp/verifyOrder"


def order_confirmation():
    return "https://client.schwab.com/api/ts/stamp/confirmorder"


def account_summary():
    return "https://client.schwab.com/clientapps/accounts/summary/"


def trade_ticket():
    # return "https://client.schwab.com/app/trade/tom/#/trade"
    return "https://client.schwab.com/app/trade/tom/trade?ShowUN=YES"


# New API
def order_verification_v2():
    return "https://ausgateway.schwab.com/api/is.TradeOrderManagementWeb/v1/TradeOrderManagementWebPort/orders"


def bundle_order_verification_v2():
    return "https://jfkgateway.schwab.com/api/is.stockbundles/V1/trade/stockbundles/bundleorder"


def account_info_v2():
    return (
        "https://ausgateway.schwab.com/api/is.TradeOrderManagementWeb/v1/TradeOrderManagementWebPort/customer/accounts"
    )


def positions_v2():
    return "https://ausgateway.schwab.com/api/is.Holdings/V1/Holdings/Holdings?=&includeCostBasis=true&includeRatings=true&includeUnderlyingOption=true"


def balance_positions_v2():
    return "https://ausgateway.schwab.com/api/is.TradeOrderManagementWeb/v1/TradeOrderManagementWebPort/account/balancespositions"


def transaction_history_v2():
    return "https://ausgateway.schwab.com/api/is.TransactionHistoryWeb/TransactionHistoryInterface/TransactionHistory/brokerage/transactions"


def ticker_quotes_v2():
    return (
        "https://ausgateway.schwab.com/api/is.TradeOrderManagementWeb/v1/TradeOrderManagementWebPort/market/quotes/list"
    )


def orders_v2():
    return "https://ausgateway.schwab.com/api/is.TradeOrderStatusWeb/ITradeOrderStatusWeb/ITradeOrderStatusWebPort/orders/listView?DateRange=All&OrderStatusType=All&SecurityType=AllSecurities&Type=All&ShowAdvanceOrder=true&SortOrder=Ascending&SortColumn=Status&CostMethod=M&IsSimOrManagedAccount=false&EnableDateFilterByActivity=true"


def options_chain_v2():
    return "https://ausgateway.schwab.com/api/is.CSOptionChainsWeb/v1/OptionChainsPort/OptionChains"
