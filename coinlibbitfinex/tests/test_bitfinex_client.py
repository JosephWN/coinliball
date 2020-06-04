import time

from coinlib.datatypes import Ticker, OrderBook, Execution, OrderSide, OrderState, Order
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.position import Position
from coinlibbitfinex.client import Client


def test_public(client: Client):
    instruments = client.instruments
    assert 'BTC_USD' in instruments
    assert 'XRP_USD' in instruments

    currencies = client.currencies
    assert 'BTC' in currencies
    assert 'XRP' in currencies

    # ticker
    ticker = client.get_ticker('BTC_USD')
    assert isinstance(ticker, Ticker)
    assert ticker.instrument == 'BTC_USD'
    assert abs(ticker.timestamp - time.time()) < 30

    # order book
    order_book = client.get_order_book('BTC_USD')
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
    assert abs(order_book.timestamp - time.time()) < 30


def test_get_balances(client: Client):
    all_balances = client.get_balances()
    assert set(all_balances.keys()).issubset(BalanceType._all)
    for balances in all_balances.values():
        assert 'USD' in balances
        balance = balances['USD']
        assert isinstance(balance.total, float)
        assert isinstance(balance.locked, float)
        assert isinstance(balance.free, float)
        assert balance.free == balance.total - balance.locked


def test_order(client_write: Client):
    order: Order = client_write.submit_limit_order('XRP_USD', price=0.0001, qty=25, side='BUY')
    assert order.order_type == 'exchange limit'
    assert order.instrument == 'XRP_USD'
    assert order.side == OrderSide.BUY
    assert order.price == 0.0001
    assert order.price_executed_average is None
    assert order.qty == 25
    assert order.qty_displayed is None
    assert order.qty_remained == 25
    assert order.qty_executed == 0
    assert order.state == OrderState.ACTIVE
    assert order.is_hidden is False
    assert order.is_iceberg is False

    res = client_write.get_orders(order_ids=[order.order_id])
    res = list(res)
    assert len(res) == 1 and res[0].order_id == order.order_id

    res = client_write.cancel_order(order.order_id)
    assert res.order_id == order.order_id
    assert res.state == OrderState.ACTIVE  # server return ACTIVE state

    res = client_write.get_orders(order_ids=[order.order_id])
    res = list(res)
    assert len(res) == 1 and res[0].state == OrderState.CANCELED


def test_get_private_executions(client: Client):
    for i, x in enumerate(client.get_private_executions('XRP_USD')):
        assert isinstance(x, Execution)
        assert x.instrument == 'XRP_USD'
        if i > 1:
            break


def test_get_positions(client: Client):
    for i, x in enumerate(client.get_positions()):
        assert isinstance(x, Position)
        assert x.qty_opened > 0
        if i > 1:
            break
