import time
from queue import Queue, Empty

import pytest

from coinlib.datatypes import Ticker, OrderBook
from coinlib.datatypes.streamdata import StreamData
from coinlibbitfinex.streamclient import StreamClient

WAIT = 5
N = 10


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
