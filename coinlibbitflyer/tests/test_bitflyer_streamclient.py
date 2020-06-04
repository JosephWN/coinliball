import time
from queue import Queue, Empty

import pytest

from coinlib.datatypes import Ticker, OrderBook
from coinlib.datatypes.streamdata import StreamData
from coinlibbitflyer.streamclient import StreamClient

WAIT = 5
N = 10


def test_ticker(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(5)
    stream_client.subscribe(ticker='BTC_JPY')
    d: StreamData = q.get(timeout=5)
    k, d = d.key, d.data
    assert k == ('ticker', 'BTC_JPY')
    assert isinstance(d, Ticker)


def test_order_book(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(5)
    stream_client.subscribe(order_book='FX_BTC_JPY')
    d: StreamData = q.get(timeout=WAIT)
    k, d = d.key, d.data
    assert k == ('order_book', 'FX_BTC_JPY')
    assert isinstance(d, OrderBook)
    assert d.instrument == 'FX_BTC_JPY'
    ask = d.asks[0]
    assert isinstance(ask[0], float) and isinstance(ask[1], float) and ask[2] is None
    bid = d.bids[0]
    assert isinstance(bid[0], float) and isinstance(bid[1], float) and bid[2] is None


def test_subscribe_unsubscribe(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(5)
    stream_client.subscribe(order_book='FX_BTC_JPY')
    d = q.get(timeout=WAIT)
    k, d = d.key, d.data
    assert k == ('order_book', 'FX_BTC_JPY')
    assert isinstance(d, OrderBook)
    assert d.instrument == 'FX_BTC_JPY'

    stream_client.unsubscribe(order_book='FX_BTC_JPY')
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)

    stream_client.subscribe(order_book='BTC_JPY')
    stream_client.subscribe(order_book='FX_BTC_JPY')
    keys = set()
    time.sleep(WAIT)
    for _ in range(q.qsize() + N * 2):
        d = q.get(timeout=WAIT)
        k = d.key
        keys.add(k)
        assert k in [('order_book', 'BTC_JPY'), ('order_book', 'FX_BTC_JPY')]
    assert keys == {('order_book', 'BTC_JPY'), ('order_book', 'FX_BTC_JPY')}

    stream_client.unsubscribe(order_book='FX_BTC_JPY')
    stream_client.unsubscribe(order_book='BTC_JPY')
    time.sleep(1)
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=2)


def test_reconnect(stream_client: StreamClient):
    stream_client.reconnect_interval = 1
    connect_count = 0

    def on_open():
        nonlocal connect_count
        connect_count += 1

    q = Queue()

    stream_client.on_open = on_open
    stream_client.on_data = q.put

    assert stream_client.wait_connection(WAIT)
    time.sleep(0.5)
    assert connect_count == 1

    stream_client.subscribe(order_book='BTC_JPY')
    q.get(timeout=WAIT * 2)

    # unexpectedly close, automatically re-subscribe
    stream_client.stream_api.stop()
    time.sleep(2)
    for _ in range(q.qsize() + N):
        d = q.get(timeout=WAIT)
        k = d.key
        assert k == ('order_book', 'BTC_JPY')
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
        assert k == ('order_book', 'BTC_JPY')
    assert connect_count == 3
