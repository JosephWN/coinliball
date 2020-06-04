import pytest

from coinlib.errors import CoinError
from coinlibbitbankcc.restapi import RestApi


def test_rest_api(api: RestApi):
    assert api.NAME == 'bitbankcc'


def test_public(api: RestApi):
    # GET
    res = api.public_get('/btc_jpy/ticker')
    assert 'last' in res
    assert 'timestamp' in res


def test_private(api: RestApi):
    # GET no parameter
    res = api.private_get('/user/assets')
    assert 'assets' in res
    assert isinstance(res['assets'], list)

    # GET with parameter
    res = api.private_get('/user/spot/active_orders', pair='btc_jpy', count=1)
    assert 'orders' in res

    # POST no parameter
    with pytest.raises(CoinError) as exc_info:
        api.private_post('/user/spot/orders_info')
    e: CoinError = exc_info.value
    assert e.info['code'] == 30009

    # POST with parameter
    res = api.private_post('/user/spot/orders_info', pair='btc_jpy', order_ids=[1, 2, 3])
    assert res == {'orders': []}
