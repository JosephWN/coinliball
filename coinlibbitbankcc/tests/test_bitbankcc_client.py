import time

import pytest

from coinlib.datatypes import Ticker, OrderBook, OrderState, OrderSide, Order
from coinlib.datatypes.balance import BalanceType
from coinlib.errors import NotSupportedError
from coinlibbitbankcc.client import Client


def test_get_ticker(client: Client):
    ticker = client.get_ticker('XRP_JPY')
    assert isinstance(ticker, Ticker)
    ticker.validate()
    assert ticker.instrument == 'XRP_JPY'
    assert abs(ticker.timestamp - time.time()) < 30


def test_get_order_book(client: Client):
    order_book = client.get_order_book('XRP_JPY')
    assert isinstance(order_book, OrderBook)
    order_book.validate()
    assert abs(order_book.timestamp - time.time()) < 2


def test_get_public_executions(client: Client):
    with pytest.raises(NotSupportedError):
        res = client.get_public_executions('XRP_JPY')
        next(res)


def test_get_balances(client: Client):
    balances = client.get_balances()
    balances = balances[BalanceType.MAIN]
    assert 'JPY' in balances
    assert isinstance(balances['JPY'].total, float)
    assert isinstance(balances['JPY'].locked, float)
    assert isinstance(balances['JPY'].free, float)
    assert balances['JPY'].free == balances['JPY'].total - balances['JPY'].locked


def test_order(client_write: Client):
    order: Order = client_write.submit_limit_order('XRP_JPY', price=0.001, qty=0.001, side='BUY')
    assert order.order_type == 'limit'
    assert order.instrument == 'XRP_JPY'
    assert order.side == OrderSide.BUY
    assert order.price == 0.001
    assert order.price_executed_average is None
    assert order.qty == 0.001
    assert order.qty_displayed is None
    assert order.qty_remained == 0.001
    assert order.qty_executed == 0
    assert order.state == OrderState.ACTIVE
    assert order.is_hidden is False
    assert order.is_iceberg is False

    res = client_write.get_orders(instrument='XRP_JPY')
    res = list(res)
    assert len(res)
    assert order.order_id in set(x.order_id for x in res)

    res = client_write.cancel_order(order.order_id)
    assert res.order_id == order.order_id
    assert res.state == OrderState.CANCELED

    res = client_write.get_orders(order_ids=[order.order_id])
    res = list(res)
    assert len(res)
    assert order.order_id in set(x.order_id for x in res)
