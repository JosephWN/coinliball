from queue import Queue, Empty
import time

import pytest

from coinlib.datatypes.streamdata import StreamData
from coinlibbitbankcc.streamapi import StreamApi

WAIT = 3
N = 10


def test_subscribe(stream_api: StreamApi):
    xrp_usd_params = {
        'event': 'subscribe',
        'channel': 'book',
        'symbol': 'tXRPUSD',
        'prec': 'P0',
        'freq': 'F0',
        'len': '25',
    }
    xrp_btc_params = xrp_usd_params.copy()
    xrp_btc_params['symbol'] = 'tXRPBTC'
    q = Queue()
    stream_api.on_raw_data = q.put

    stream_api.subscribe(('xrp_usd', xrp_usd_params))
    stream_api.subscribe(('xrp_btc', xrp_btc_params))
    keys = set()
    time.sleep(1)
    for _ in range(N):
        d: StreamData = q.get(timeout=WAIT)
        k = d.key
        keys.add(k)
    assert keys == {'xrp_usd', 'xrp_btc'}

    stream_api.unsubscribe('xrp_usd')
    time.sleep(1)
    for _ in range(q.qsize() + N):
        q.get(timeout=WAIT)
    keys = set()
    for _ in range(q.qsize() + N):
        d = q.get(timeout=WAIT)
        k = d.key
        keys.add(k)
    assert keys == {'xrp_btc'}

    stream_api.unsubscribe('xrp_btc')
    with pytest.raises(Empty):
        for _ in range(q.qsize() + N):
            q.get(timeout=WAIT)

    # re-subscribe
    stream_api.subscribe(('xrp_usd', xrp_usd_params), ('xrp_btc', xrp_btc_params))
    keys = set()
    for _ in range(N):
        d = q.get(timeout=WAIT)
        k = d.key
        keys.add(k)
    assert keys == {'xrp_usd', 'xrp_btc'}
