import time
from queue import Queue, Empty

import pytest

from coinlibbitbankcc.streamapi import StreamApi

WAIT = 3
N = 10


def test_subscribe(stream_api: StreamApi):
    q = Queue()
    stream_api.on_raw_data = q.put

    stream_api.subscribe(('btc_jpy_depth', 'depth_btc_jpy'))
    stream_api.subscribe(('xrp_jpy_depth', 'depth_xrp_jpy'))
    keys = set()
    for _ in range(N):
        d = q.get(timeout=WAIT)
        keys.add(d.key)
    assert keys == {'btc_jpy_depth', 'xrp_jpy_depth'}

    stream_api.unsubscribe('btc_jpy_depth')
    time.sleep(1)
    assert stream_api._channel_name_map == {'depth_xrp_jpy': 'xrp_jpy_depth'}
    for _ in range(q.qsize() + N):
        q.get(timeout=WAIT)
    keys = set()
    for _ in range(q.qsize() + N):
        d = q.get(timeout=WAIT)
        keys.add(d.key)
    assert keys == {'xrp_jpy_depth'}

    stream_api.unsubscribe('xrp_jpy_depth')
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)

    # re-subscribe
    stream_api.subscribe(('btc_jpy_depth', 'depth_btc_jpy'), ('xrp_jpy_depth', 'depth_xrp_jpy'))
    keys = set()
    for _ in range(N):
        d = q.get(timeout=WAIT)
        keys.add(d.key)
    assert keys == {'btc_jpy_depth', 'xrp_jpy_depth'}
