from _pytest.fixtures import FixtureRequest
import pytest

from coinlib.utils import config
from coinlibquoinex.client import Client
from coinlibquoinex.restapi import RestApi
from coinlibquoinex.streamapi import StreamApi
from coinlibquoinex.streamclient import StreamClient


@pytest.fixture
def api(request: FixtureRequest):
    _ = request
    credential = config.load()['coinlib_test'][RestApi.NAME]
    return RestApi(credential)


@pytest.fixture
def stream_api(request: FixtureRequest):
    _ = request
    credential = config.load()['coinlib_test'][RestApi.NAME]
    with StreamApi(pusher_key=credential['pusher_key']) as _stream_api:
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
    credential = config.load()['coinlib_test'][RestApi.NAME]
    with StreamClient(pusher_key=credential['pusher_key']) as _stream_client:
        yield _stream_client
