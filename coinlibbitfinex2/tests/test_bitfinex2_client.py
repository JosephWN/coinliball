import math
import time

from coinlib.datatypes import Ticker, OrderBook, Execution, OrderSide, OrderState, Order
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.position import Position
from coinlibbitfinex2.client import Client


def test_get_ticker(client: Client):
    ticker = client.get_ticker('BTC_USD')
    assert isinstance(ticker, Ticker)
    ticker.validate()
    assert ticker.instrument == 'BTC_USD'
    assert abs(ticker.timestamp - time.time()) < 30


def test_get_order_book(client: Client):
    order_book = client.get_order_book('BTC_USD')
    assert isinstance(order_book, OrderBook)
    order_book.validate()
    assert abs(order_book.timestamp - time.time()) < 2


def test_get_public_executions(client: Client):
    execution_ids = set()
    timestamp = math.inf
    for i, execution in enumerate(client.get_public_executions('BTC_USD')):
        assert isinstance(execution, Execution)
        execution.validate()
        execution_ids.add(execution.execution_id)
        assert execution.timestamp <= timestamp
        timestamp = execution.timestamp
        if i >= 1000:
            break
    assert len(execution_ids) == 1001


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
    execution_ids = set()
    timestamp = math.inf
    for i, execution in enumerate(client.get_private_executions('XRP_USD')):
        assert isinstance(execution, Execution)
        assert execution.instrument == 'XRP_USD'
        execution_ids.add(execution.execution_id)
        assert execution.timestamp <= timestamp
        timestamp = execution.timestamp
        if i >= 1000:
            break
    assert len(execution_ids) == 1001


def test_get_positions(client: Client):
    for i, x in enumerate(client.get_positions()):
        assert isinstance(x, Position)
        assert x.qty_opened > 0
        if i > 1:
            break
