import pytest
from requests import HTTPError

from coinlibbitbankcc.restapi import RestApi


def test_name(api: RestApi):
    assert api.NAME == 'bitflyer'


def test_public(api: RestApi):
    # GET no parameter
    res = api.public_get('/getticker')
    assert res['product_code'] == 'BTC_JPY'

    # GET with parameter
    res = api.public_get('/getticker', product_code='ETH_BTC')
    assert res['product_code'] == 'ETH_BTC'


def test_private(api: RestApi):
    # GET no parameter
    res = api.private_get('/getpermissions')
    assert '/v1/me/getpermissions' in res

    # GET with parameter
    res = api.private_get('/getcoinins', count=1)
    assert len(res) == 1


@pytest.mark.skip(reason='need write permission')
def test_private_post(api: RestApi):
    # POST no parameter
    with pytest.raises(HTTPError) as exc_info:
        api.private_post('/cancelchildorder')
    exc = exc_info.value
    assert 'Invalid product' in str(exc)

    # POST with parameter
    with pytest.raises(HTTPError) as exc_info:
        api.private_post('/cancelchildorder', product_code='XXX')
    exc = exc_info.value
    assert 'Invalid product' in str(exc)
