import time
from queue import Queue, Empty

import pytest

from coinlibbitmex.streamapi import StreamApi

WAIT = 10
N = 100


def test_subscribe(stream_api: StreamApi):
    q = Queue()
    stream_api.on_raw_data = q.put

    stream_api.subscribe(('quote', 'quote:XBTUSD'))
    stream_api.subscribe(('10', 'orderBook10:XBTUSD'))
    keys = set()
    for _ in range(N):
        d = q.get(timeout=WAIT)
        k = d.key
        keys.add(k)
    assert keys == {'quote', '10'}

    stream_api.unsubscribe('quote')
    time.sleep(2)
    for _ in range(q.qsize() + N):
        q.get(timeout=WAIT)
    keys = set()
    for _ in range(q.qsize() + N):
        k, _ = q.get(timeout=WAIT)
        keys.add(k)
    assert keys == {'10'}

    stream_api.unsubscribe('10')
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)

    # re-subscribe
    stream_api.subscribe(('quote', 'quote:XBTUSD'), ('10', 'orderBook10:XBTUSD'))
    keys = set()
    for _ in range(N):
        k, _ = q.get(timeout=WAIT)
        keys.add(k)
    assert keys == {'quote', '10'}
