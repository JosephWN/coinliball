from coinlibbitfinex.restapi import RestApi


def test_rest_api(api: RestApi):
    assert api.NAME == 'bitfinex'


def test_public(api: RestApi):
    # GET
    res = api.public_get('/book/btcusd')
    assert 'asks' in res
    assert 'bids' in res


def test_private(api: RestApi):
    # POST no parameter
    res = api.private_post('/balances')
    assert len(res)
    for x in res:
        assert 'currency' in x
        assert 'type' in x
        assert 'amount' in x

    # POST with parameter
    res = api.private_post('/mytrades', symbol='btcusd', limit_trades=50)
    assert len(res)
