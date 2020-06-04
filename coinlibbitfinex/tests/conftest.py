import pytest
from _pytest.fixtures import FixtureRequest

from coinlib.utils import config
from coinlibbitfinex.client import Client
from coinlibbitfinex.restapi import RestApi
from coinlibbitfinex.streamapi import StreamApi
from coinlibbitfinex.streamclient import StreamClient


@pytest.fixture
def api(request: FixtureRequest):
    _ = request
    credential = config.load()['coinlib_test'][RestApi.NAME]
    return RestApi(credential)


@pytest.fixture
def stream_api(request: FixtureRequest):
    _ = request
    with StreamApi() as _stream_api:
        yield _stream_api


@pytest.fixture
def client(request: FixtureRequest):
    _ = request
    credential = config.load()['coinlib_test'][RestApi.NAME]
    return Client(credential)


@pytest.fixture
def client_write(request: FixtureRequest):
    _ = request
    credential = config.load()['coinlib_test'][RestApi.NAME + '.write']
    return Client(credential)


@pytest.fixture
def stream_client(request: FixtureRequest):
    _ = request
    with StreamClient() as _stream_client:
        yield _stream_client
