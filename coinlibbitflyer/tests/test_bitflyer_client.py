import itertools
import time

from coinlib.datatypes import Ticker, OrderBook, OrderState, OrderSide, Order
from coinlib.datatypes.balance import BalanceType
from coinlibbitflyer.client import Client


def test_public(client: Client):
    instruments = client.instruments
    assert 'BTC_JPY' in instruments
    assert 'FX_BTC_JPY' in instruments

    currencies = client.currencies
    assert 'BTC' in currencies
    assert 'ETH' in currencies

    # ticker
    ticker = client.get_ticker('FX_BTC_JPY')
    assert isinstance(ticker, Ticker)
    assert ticker.instrument == 'FX_BTC_JPY'
    assert abs(ticker.timestamp - time.time()) < 2

    # order book
    order_book = client.get_order_book('FX_BTC_JPY')
    assert isinstance(order_book, OrderBook)
    asks = order_book.asks
    assert isinstance(asks, list)
    assert isinstance(asks[0][0], float)
    assert isinstance(asks[0][1], float)
    assert asks[0][2] is None
    assert asks[0][0] < asks[1][0]
    for ask in asks:
        assert ask[1] > 0
    bids = order_book.bids
    assert isinstance(bids, list)
    assert isinstance(bids[0][0], float)
    assert isinstance(bids[0][1], float)
    assert bids[0][2] is None
    assert bids[0][0] > bids[1][0]
    for bid in bids:
        assert bid[1] > 0
    assert isinstance(order_book.timestamp, float)
    assert abs(order_book.timestamp - time.time()) < 2

    executions = itertools.islice(client.get_public_executions('BTC_JPY', ), 0, 1001)
    id_set = set(x.execution_id for x in executions)
    assert len(id_set) == 1001


def test_get_balances(client: Client):
    all_balances = client.get_balances()
    assert set(all_balances.keys()) == {BalanceType.MAIN, BalanceType.MARGIN}
    balances = all_balances[BalanceType.MAIN]
    assert 'JPY' in balances
    balance = balances['JPY']
    assert isinstance(balance.total, float)
    assert isinstance(balance.locked, float)
    assert isinstance(balance.free, float)
    assert balance.free == balance.total - balance.locked


def test_order(client_write: Client):
    ticker = client_write.get_ticker('BTC_JPY')
    price = int(ticker.bid * 0.8)
    order: Order = client_write.submit_limit_order('BTC_JPY', price=price, qty=0.001, side='BUY')
    assert order.order_type == 'LIMIT'
    assert order.instrument == 'BTC_JPY'
    assert order.side == OrderSide.BUY
    assert order.price == price
    assert order.price_executed_average is None
    assert order.qty == 0.001
    assert order.qty_displayed is None
    assert order.qty_remained == 0.001
    assert order.qty_executed == 0
    assert order.state == OrderState.UNKNOWN
    assert order.is_hidden is False
    assert order.is_iceberg is False

    time.sleep(1)
    res = client_write.get_orders(order_ids=[order.order_id], instrument='BTC_JPY', active_only=False)
    res = list(res)
    assert len(res) == 1 and res[0].order_id == order.order_id
    res = client_write.get_orders(instrument='BTC_JPY')
    assert order.order_id in set(x.order_id for x in res)

    res = client_write.cancel_order(order.order_id)
    assert res is None
    time.sleep(1)
    res = client_write.get_orders(order_ids=[order.order_id], instrument='BTC_JPY', active_only=False)
    res = list(res)
    assert len(res) == 0
