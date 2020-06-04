import pytest
from requests import HTTPError

from coinlib.errors import CoinError
from coinlibbitmex.restapi import RestApi


def test_rest_api(api: RestApi):
    assert api.NAME == 'bitmex'


def test_public(api: RestApi):
    # GET
    res = api.public_get('/orderBook/L2', symbol='XBTUSD')
    assert res


def test_private(api: RestApi):
    # GET no parameter
    res = api.private_get('/user')
    assert res

    # GET with parameter
    res = api.private_get('/user/wallet', currency='XBt')
    assert res


def test_private_write(api_write: RestApi):
    api = api_write
    # POST no parameter
    with pytest.raises(HTTPError) as exc_info:
        api.private_post('/order')
    e: CoinError = exc_info.value
    assert 'required' in str(e)

    # POST with parameter
    with pytest.raises(HTTPError) as exc_info:
        api.private_post('/order', symbol='invalid_symbol')
    e: CoinError = exc_info.value
    assert 'symbol is invalid' in str(e)
