from coinlibbitfinex2.restapi import RestApi


def test_rest_api(api: RestApi):
    assert api.NAME == 'bitfinex2'


def test_public(api: RestApi):
    # GET
    res = api.public_get('/ticker/tBTCUSD')
    assert res and isinstance(res, list), res
