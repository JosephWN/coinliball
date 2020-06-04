from abc import ABC, abstractmethod
import logging
import threading
from typing import Any, Callable, Tuple, Hashable

from coinlib.datatypes.streamdata import StreamData
from coinlib.utils.threadmixin import ThreadMixin

logger = logging.getLogger(__name__)

OnOpenCallback = Callable[[], None]
OnCloseCallback = Callable[[], None]
OnDataCallback = Callable[[StreamData], None]
OnAuthCallback = Callable[[bool, Any], None]


class StreamApi(ThreadMixin, ABC):
    """
    Open connection when start() called, close connection when stop() called
    """

    def __init__(self, *,
                 on_open: OnOpenCallback = None,
                 on_close: OnCloseCallback = None,
                 on_raw_data: OnDataCallback = None,
                 on_auth: OnAuthCallback = None,
                 **kwargs):
        self.on_open: OnOpenCallback = on_open or (lambda: None)
        self.on_close: OnCloseCallback = on_close or (lambda: None)
        self.on_raw_data: OnDataCallback = on_raw_data or (lambda *_: None)
        self.on_auth: OnAuthCallback = on_auth or (lambda *_: None)
        self._thread_data = self.ThreadData()
        self._is_authenticated = threading.Event()

    @abstractmethod
    def subscribe(self, *args: Tuple[Hashable, Any]):
        """
        example)
        obj.subscribe(('btc_jpy_ticker', ticker_subscription_params))
        """

    @abstractmethod
    def unsubscribe(self, *args: Hashable):
        """
        example)
        obj.unsubscribe('btc_jpy_ticker')
        """

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    # private

    def authenticate(self, *, credential: dict, params: dict = None):
        pass

    def wait_authentication(self, timeout: float = None) -> bool:
        return self._is_authenticated.wait(timeout)

    def is_authenticated(self) -> bool:
        return self._is_authenticated.is_set()
