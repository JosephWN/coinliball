from abc import abstractmethod, ABC
import logging
import threading
from typing import Tuple, Hashable, Type, Optional, Any

from coinlib.datatypes.streamdata import StreamData
from .client import Client as ClientBase
from .streamapi import StreamApi, OnDataCallback

logger = logging.getLogger(__name__)


class StreamClient(ClientBase, ABC):
    STREAM_API_CLASS: Type[StreamApi] = None

    def __init__(self, credential: dict = None, *,
                 reconnect_interval: float = 10,
                 on_data: OnDataCallback = None,
                 **kwargs):
        super().__init__(credential, **kwargs)

        self.reconnect_interval = reconnect_interval
        self.on_data = on_data or (lambda *_: None)

        self.stream_api: StreamApi = None
        self._subscription_keys = set()
        self._is_connected = threading.Event()
        self._open_close_lock = threading.RLock()
        self._authentication_params = {}
        self._is_authenticated = threading.Event()

    def is_connected(self) -> bool:
        """Return connection is live or not."""
        return self._is_connected.is_set()

    def wait_connection(self, timeout: float = 30) -> bool:
        return self._is_connected.wait(timeout)

    def new_stream_api(self, *args, **kwargs) -> StreamApi:
        return self.STREAM_API_CLASS(*args, **kwargs)

    def open(self):
        with self._open_close_lock:
            assert not self.stream_api, 'already opened'

            def connect():
                def on_open():
                    self._is_connected.set()
                    self.on_open()
                    # re-authenticate
                    if self.is_authenticated():
                        self.authenticate()
                    # re-subscribe
                    for key in self._subscription_keys:
                        self.request_subscribe(key)

                def on_close():
                    self._is_connected.clear()
                    self.on_close()
                    # reconnect if not closed unexpectedly
                    with self._open_close_lock:
                        if self.stream_api and not self.stream_api.is_active():
                            connect()

                def on_raw_data(data: StreamData):
                    if stream_api.is_active():
                        data = self.convert_raw_data(data)
                        if data:
                            self.on_data(data)

                def on_auth(success: bool, data: Any):
                    _ = data
                    if success:
                        self._is_authenticated.set()

                stream_api = self.new_stream_api(on_open=on_open,
                                                 on_close=on_close,
                                                 on_raw_data=on_raw_data,
                                                 on_auth=on_auth)
                self.stream_api = stream_api
                self.stream_api.start()

            connect()

    def close(self):
        with self._open_close_lock:
            assert self.stream_api, 'not opened'
            stream_api = self.stream_api
            self.stream_api: StreamApi = None
            stream_api.stop()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def subscribe(self, **kwargs: Hashable):
        """
        example)
        obj.subscribe(order_book='BTC_JPY', ticker='BTC_JPY')
        """
        assert self.stream_api, 'not opened'
        for key in kwargs.items():
            if key not in self._subscription_keys:
                self._subscription_keys.add(key)
                self.request_subscribe(key)

    def unsubscribe(self, **kwargs: Hashable):
        """
        example)
        obj.unsubscribe(order_book='BTC_JPY', ticker='BTC_JPY')
        """
        assert self.stream_api, 'not opened'
        for key in kwargs.items():
            if key in self._subscription_keys:
                self._subscription_keys.remove(key)
                self.request_unsubscribe(key)

    @abstractmethod
    def request_subscribe(self, key: Tuple[str, Hashable]):
        """do send subscribe request"""

    @abstractmethod
    def request_unsubscribe(self, key: Tuple[str, Hashable]):
        """do send unsubscribe request"""

    def on_open(self):
        """
        called connection opened/re-opened.
        """

    def on_close(self):
        """
        called connection closed.
        """

    @abstractmethod
    def convert_raw_data(self, data: StreamData) -> Optional[StreamData]:
        """
        convert raw-data to non-raw data
        :param data:
        :return: None if no converted data
        """

    # private

    def authenticate(self, *, params: dict = None):
        self._authentication_params = params or self._authentication_params
        self.stream_api.authenticate(credential=self.credential, params=self._authentication_params)

    def wait_authentication(self, timeout: float = 30) -> bool:
        return self._is_authenticated.wait(timeout)

    def is_authenticated(self) -> bool:
        return self._is_authenticated.is_set()
