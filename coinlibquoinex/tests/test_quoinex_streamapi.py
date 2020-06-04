import time
from queue import Queue, Empty

import pytest

from coinlibquoinex.streamapi import StreamApi

WAIT = 3
N = 10


def test_subscribe(stream_api: StreamApi):
    q = Queue()
    stream_api.on_raw_data = q.put

    stream_api.subscribe(('btc_buy', 'price_ladders_cash_btcjpy_buy'))
    stream_api.subscribe(('btc_sell', 'price_ladders_cash_btcjpy_sell'))
    keys = set()
    for _ in range(N):
        d = q.get(timeout=WAIT)
        keys.add(d.key)
    assert keys == {'btc_buy', 'btc_sell'}

    stream_api.unsubscribe('btc_buy')
    time.sleep(1)
    for _ in range(q.qsize() + N):
        q.get(timeout=WAIT)
    keys = set()
    for _ in range(q.qsize() + N):
        d = q.get(timeout=WAIT)
        keys.add(d.key)
    assert keys == {'btc_sell'}

    stream_api.unsubscribe('btc_sell')
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)

    # re-subscribe
    stream_api.subscribe(('btc_buy', 'price_ladders_cash_btcjpy_buy'), ('btc_sell', 'price_ladders_cash_btcjpy_sell'))
    keys = set()
    for _ in range(N):
        d = q.get(timeout=WAIT)
        keys.add(d.key)
    assert keys == {'btc_buy', 'btc_sell'}
