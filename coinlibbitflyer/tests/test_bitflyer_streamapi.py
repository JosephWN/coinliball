import time
from queue import Queue, Empty

import pytest

from coinlibbitflyer.streamapi import StreamApi

WAIT = 5
N = 10


def test_subscribe(stream_api: StreamApi):
    q = Queue()
    stream_api.on_raw_data = q.put

    stream_api.subscribe(('btc_jpy', 'lightning_ticker_BTC_JPY'))
    stream_api.subscribe(('fx_btc_jpy', 'lightning_ticker_FX_BTC_JPY'))
    keys = set()
    for _ in range(N):
        d = q.get(timeout=WAIT)
        k = d.key
        keys.add(k)
    assert keys == {'btc_jpy', 'fx_btc_jpy'}

    stream_api.unsubscribe('btc_jpy')
    time.sleep(1)
    for _ in range(q.qsize() + N):
        q.get(timeout=WAIT)
    keys = set()
    for _ in range(q.qsize() + N):
        k, _ = q.get(timeout=WAIT)
        keys.add(k)
    assert keys == {'fx_btc_jpy'}

    stream_api.unsubscribe('fx_btc_jpy')
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)

    # re-subscribe
    stream_api.subscribe(('btc_jpy', 'lightning_ticker_BTC_JPY'), ('fx_btc_jpy', 'lightning_ticker_FX_BTC_JPY'))
    keys = set()
    for _ in range(N):
        k, _ = q.get(timeout=WAIT)
        keys.add(k)
    assert keys == {'btc_jpy', 'fx_btc_jpy'}
