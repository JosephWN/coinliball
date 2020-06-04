import pytest
from requests import HTTPError

from coinlibzaif.restapi import RestApi


def test_name(api: RestApi):
    assert api.NAME == 'zaif'


def test_public(api: RestApi):
    # GET no parameter
    res = api.public_get('/')
    assert len(res)

    # GET with parameter
    res = api.public_get('/executions', product_id=5)
    assert len(res)


def test_private(api: RestApi):
    # GET no parameter
    res = api.private_get('/orders')
    assert len(res.get('models', []))

    # GET with parameter
    res = api.private_get('/orders', product_id=5, limit=2, page=1)
    assert len(res.get('models', []))


@pytest.mark.skip(reason='need write permission')
def test_private_post(api: RestApi):
    # TODO: private_post
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
