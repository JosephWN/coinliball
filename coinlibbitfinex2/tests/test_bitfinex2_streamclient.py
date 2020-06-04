from queue import Queue, Empty
import time

import pytest

from coinlib.datatypes import Ticker, OrderBook, OrderSide
from coinlib.datatypes.streamdata import StreamData
from coinlibbitfinex2.client import Flag
from coinlibbitfinex2.streamclient import StreamClient

WAIT = 5
N = 50


@pytest.mark.skip(reason='currently not supported')
def test_ticker(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(5)
    stream_client.subscribe(ticker='XRP_USD')
    d: StreamData = q.get(timeout=WAIT)
    k = d.key
    d = d.data
    assert k == ('ticker', 'XRP_USD')
    assert isinstance(d, Ticker)


def test_order_book(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(5)
    stream_client.subscribe(order_book='XRP_USD')
    d: StreamData = q.get(timeout=WAIT)
    k, d = d.key, d.data
    assert k == ('order_book', 'XRP_USD')
    assert isinstance(d, OrderBook)
    assert d.instrument == 'XRP_USD'
    ask = d.asks[0]
    assert isinstance(ask[0], float) and isinstance(ask[1], float) and isinstance(ask[2], int)
    bid = d.bids[0]
    assert isinstance(bid[0], float) and isinstance(bid[1], float) and isinstance(bid[2], int)


def test_subscribe_unsubscribe(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(WAIT)
    stream_client.subscribe(order_book='XRP_USD')
    d = q.get(timeout=WAIT)
    k, d = d.key, d.data
    assert k == ('order_book', 'XRP_USD')
    assert isinstance(d, OrderBook)
    assert d.instrument == 'XRP_USD'

    stream_client.unsubscribe(order_book='XRP_USD')
    time.sleep(1)
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)

    stream_client.subscribe(order_book='BTC_USD')
    stream_client.subscribe(order_book='XRP_USD')
    keys = set()
    time.sleep(1)
    for _ in range(q.qsize() + N):
        d = q.get(timeout=WAIT)
        k = d.key
        keys.add(k)
        assert k in [('order_book', 'BTC_USD'), ('order_book', 'XRP_USD')]
    assert keys == {('order_book', 'BTC_USD'), ('order_book', 'XRP_USD')}

    stream_client.unsubscribe(order_book='XRP_USD')
    stream_client.unsubscribe(order_book='BTC_USD')
    time.sleep(1)
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)


def test_reconnect(stream_client: StreamClient):
    stream_client.reconnect_interval = 1
    connect_count = 0

    def on_open():
        nonlocal connect_count
        connect_count += 1

    q = Queue()

    stream_client.on_open = on_open
    stream_client.on_data = q.put

    assert stream_client.wait_connection(5)
    time.sleep(0.5)
    assert connect_count == 1

    stream_client.subscribe(order_book='XRP_USD')
    q.get(timeout=WAIT)

    # unexpectedly close, automatically re-subscribe
    stream_client.stream_api.stop()
    time.sleep(2)
    for _ in range(q.qsize() + N):
        d = q.get(timeout=WAIT)
        k = d.key
        assert k == ('order_book', 'XRP_USD')
    assert connect_count == 2

    # manually close, not reconnect
    stream_client.close()
    time.sleep(2)
    assert not stream_client.is_connected()
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)
    assert connect_count == 2

    # re-open, automatically re-subscribe
    stream_client.open()
    assert stream_client.wait_connection(5)
    for _ in range(q.qsize() + N):
        d = q.get(timeout=WAIT)
        k = d.key
        assert k == ('order_book', 'XRP_USD')
    assert connect_count == 3


def test_authenticate(stream_client_write: StreamClient):
    client = stream_client_write
    client.wait_connection(WAIT)
    client.authenticate()
    assert client.wait_authentication(WAIT)

    client.close()
    client.open()


def test_order_buy(stream_client_write: StreamClient):
    client = stream_client_write

    q = Queue()
    client.on_data = q.put

    client.wait_connection(WAIT)
    client.authenticate()
    assert client.wait_authentication(WAIT)

    ticker = client.get_ticker('XRP_USD')
    price = round(ticker.bid * 0.5, 4)
    order = client.submit_limit_order('XRP_USD', 'BUY', price, 25, margin=True)
    assert order.instrument == 'XRP_USD'
    assert order.side == OrderSide.BUY
    assert order.price == price
    assert order.qty == 25
    assert order.order_type == 'LIMIT'
    assert order._data['gid'] is None

    price = round(ticker.bid * 0.4, 4)
    order = client.update_order(order.order_id, gid=10000, price=price, qty_delta=-1, flags=Flag.HIDDEN)
    assert order.price == price
    assert order.qty == 24
    assert order._data['gid'] == 10000
    assert order._data['flags'] == Flag.HIDDEN
    price = round(ticker.bid * 0.3, 4)
    order = client.update_order(order.order_id, gid=10001, price=price, qty_delta=2, flags=Flag.HIDDEN | Flag.POST_ONLY)
    assert order.price == price
    assert order.qty == 26
    assert order._data['gid'] == 10001
    assert order._data['flags'] == (Flag.HIDDEN | Flag.POST_ONLY)

    res = client.cancel_order(order.order_id)
    assert order.order_id == res.order_id


def test_order_sell(stream_client_write: StreamClient):
    client = stream_client_write

    q = Queue()
    client.on_data = q.put

    client.wait_connection(WAIT)
    client.authenticate()
    assert client.wait_authentication(WAIT)

    ticker = client.get_ticker('XRP_USD')
    price = round(ticker.ask * 2, 4)
    order = client.submit_limit_order('XRP_USD', OrderSide.SELL, price, 25, margin=True)
    assert order.instrument == 'XRP_USD'
    assert order.side == OrderSide.SELL
    assert order.price == price
    assert order.qty == 25
    assert order.order_type == 'LIMIT'
    assert order._data['gid'] is None

    price = round(ticker.bid * 2.1, 4)
    order = client.update_order(order.order_id, gid=10000, price=price, qty_delta=-1, flags=Flag.HIDDEN)
    assert order.price == price
    assert order.qty == 24
    assert order._data['gid'] == 10000
    assert order._data['flags'] == Flag.HIDDEN
    price = round(ticker.bid * 2.2, 4)
    order = client.update_order(order.order_id, gid=10001, price=price, qty_delta=2, flags=Flag.HIDDEN | Flag.POST_ONLY)
    assert order.price == price
    assert order.qty == 26
    assert order._data['gid'] == 10001
    assert order._data['flags'] == (Flag.HIDDEN | Flag.POST_ONLY)

    res = client.cancel_order(order.order_id)
    assert order.order_id == res.order_id


def test_cancel_order_group(stream_client_write: StreamClient):
    client = stream_client_write

    q = Queue()
    client.on_data = q.put

    client.wait_connection(WAIT)
    client.authenticate()
    assert client.wait_authentication(WAIT)

    ticker = client.get_ticker('XRP_USD')
    price = round(ticker.bid * 0.5, 4)
    order = client.submit_limit_order('XRP_USD', 'BUY', price, 25, margin=True, params=dict(gid=10000))
    assert order._data['gid'] == 10000
    order = client.submit_limit_order('XRP_USD', 'BUY', price, 25, margin=True, params=dict(gid=10001))
    assert order._data['gid'] == 10001

    client.cancel_order_group(10000)
    group_ids = {order._data['gid'] for order in client.get_orders()}
    assert 10000 not in group_ids
    assert 10001 in group_ids

    client.cancel_order_group(10001)
    group_ids = {order._data['gid'] for order in client.get_orders()}
    assert 10000 not in group_ids
    assert 10001 not in group_ids
