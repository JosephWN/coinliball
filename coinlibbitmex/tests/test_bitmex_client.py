import time
from pprint import pprint

from coinlib.datatypes import Ticker, OrderBook, OrderSide, OrderState, Order
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.position import Position
from coinlibbitmex.client import Client


def test_public(client: Client):
    instruments = client.instruments
    assert 'XBTUSD' in instruments

    currencies = client.currencies
    assert 'BTC' in currencies
    assert 'XRP' in currencies

    # ticker
    ticker = client.get_ticker('XBTUSD')
    assert isinstance(ticker, Ticker)
    assert ticker.instrument == 'XBTUSD'
    assert abs(ticker.timestamp - time.time()) < 30

    # order book
    order_book = client.get_order_book('XBTUSD')
    assert isinstance(order_book, OrderBook)
    asks = order_book.asks
    assert isinstance(asks, list)
    assert isinstance(asks[0][0], float)
    assert isinstance(asks[0][1], float)
    assert isinstance(asks[0][2], int)
    assert asks[0][0] < asks[1][0]
    for ask in asks:
        assert ask[1] > 0
    bids = order_book.bids
    assert isinstance(bids, list)
    assert isinstance(bids[0][0], float)
    assert isinstance(bids[0][1], float)
    assert isinstance(bids[0][2], int)
    assert bids[0][0] > bids[1][0]
    for bid in bids:
        assert bid[1] > 0
    assert isinstance(order_book.timestamp, float)
    assert abs(order_book.timestamp - time.time()) < 30


def test_get_balances(client: Client):
    all_balances = client.get_balances()
    assert set(all_balances.keys()).issubset(BalanceType._all)
    assert BalanceType.MARGIN in all_balances
    for balances in all_balances.values():
        assert 'BTC' in balances
        balance = balances['BTC']
        assert isinstance(balance.total, float)
        assert isinstance(balance.locked, float)
        assert isinstance(balance.free, float)
        assert balance.free == balance.total - balance.locked


def test_get_orders(client: Client):
    client.get_orders(active_only=False)


def test_order(client_write: Client):
    ticker = client_write.get_ticker('XBTUSD')
    bid = round(ticker.bid * 0.8, -1)
    order: Order = client_write.submit_limit_order('XBTUSD', price=bid, qty=1, side='BUY', margin=True)
    assert order.order_type == 'Limit'
    assert order.instrument == 'XBTUSD'
    assert order.side == OrderSide.BUY
    assert order.price == bid
    assert order.price_executed_average is None
    assert order.qty == 1
    assert order.qty_displayed is None
    assert order.qty_remained == 1
    assert order.qty_executed == 0
    assert order.state == OrderState.ACTIVE
    assert order.is_hidden is False
    assert order.is_iceberg is False

    res = client_write.get_orders(order_ids=[order.order_id])
    res = list(res)
    assert len(res) == 1 and res[0].order_id == order.order_id

    res = client_write.cancel_order(order.order_id)
    assert res.order_id == order.order_id
    assert res.state == OrderState.CANCELED

    res = client_write.get_orders(order_ids=[order.order_id])
    res = list(res)
    assert len(res) == 1 and res[0].state == OrderState.CANCELED


def test_get_private_executions(client: Client):
    for i, x in enumerate(client.get_private_executions('XBTUSD')):
        pass
        # assert isinstance(x, Execution)
        # assert x.instrument == 'XBTUSD'
        pprint(x.__dict__)
        if i > 100:
            break


def test_get_positions(client: Client):
    for i, x in enumerate(client.get_positions()):
        assert isinstance(x, Position)
        assert x.qty_opened > 0
        if i > 1:
            break
