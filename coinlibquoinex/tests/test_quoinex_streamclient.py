import time
from queue import Queue, Empty

import pytest

from coinlib.datatypes import Ticker, OrderBook
from coinlib.datatypes.streamdata import StreamData
from coinlibbitbankcc.streamclient import StreamClient

WAIT = 10
N = 10


def test_ticker(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(5)
    stream_client.subscribe(ticker='BTC_JPY')
    d: StreamData = q.get(timeout=WAIT)
    assert d.key == ('ticker', 'BTC_JPY')
    assert isinstance(d.data, Ticker)
    assert abs(time.time() - d.data.timestamp) < 10


def test_order_book(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(5)
    stream_client.subscribe(order_book='BTC_JPY')
    d: StreamData = q.get(timeout=5)
    assert d.key == ('order_book', 'BTC_JPY')
    assert isinstance(d.data, OrderBook)
    assert d.data.instrument == 'BTC_JPY'
    ask = d.data.asks[0]
    assert isinstance(ask[0], float) and isinstance(ask[1], float) and ask[2] is None
    bid = d.data.bids[0]
    assert isinstance(bid[0], float) and isinstance(bid[1], float) and bid[2] is None


def test_subscribe_unsubscribe(stream_client: StreamClient):
    q = Queue()

    stream_client.on_data = q.put
    assert stream_client.wait_connection(5)
    stream_client.subscribe(ticker='BTC_JPY')
    d: StreamData = q.get(timeout=WAIT)
    k, d = d.key, d.data
    assert k == ('ticker', 'BTC_JPY')
    assert isinstance(d, Ticker)
    assert d.instrument == 'BTC_JPY'

    stream_client.unsubscribe(ticker='BTC_JPY')
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)

    stream_client.subscribe(ticker='BTC_JPY')
    stream_client.subscribe(order_book='BTC_JPY')
    keys = set()
    for _ in range(q.qsize() + N):
        d = q.get(timeout=WAIT)
        k = d.key
        keys.add(k)
        assert k in [('ticker', 'BTC_JPY'), ('order_book', 'BTC_JPY')]
    assert keys == {('ticker', 'BTC_JPY'), ('order_book', 'BTC_JPY')}

    stream_client.unsubscribe(ticker='BTC_JPY')
    stream_client.unsubscribe(order_book='BTC_JPY')
    time.sleep(2)
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

    stream_client.subscribe(order_book='BTC_JPY')
    q.get(timeout=WAIT)

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
