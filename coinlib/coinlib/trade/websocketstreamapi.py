import json
import logging
import threading
import time
from abc import abstractmethod
from collections import deque
from typing import Any, Hashable, Tuple

import websocket
from websocket import WebSocket, WebSocketTimeoutException

from .streamapi import StreamApi

logger = logging.getLogger(__name__)


class WebSocketStreamApi(StreamApi):
    URL = ''
    CHECK_INTERVAL = 0.2

    def __init__(self, url: str = None, **kwargs):
        super().__init__(**kwargs)
        self._url = url or self.URL
        self._subscription_q = deque()
        self._ws: WebSocket = None

    def subscribe(self, *args: Tuple[Hashable, Any]):
        for key, params in args:
            self._subscription_q.append(('subscribe', (key, params)))

    def unsubscribe(self, *args: Hashable):
        for key in args:
            self._subscription_q.append(('unsubscribe', (key, '')))

    @abstractmethod
    def _process_subscription_q(self, ws: WebSocket):
        pass

    def create_connection(self) -> WebSocket:
        self._ws = websocket.create_connection(self._url, enable_multithread=True)
        return self._ws

    def run(self):
        def subscription_q_loop():
            while self.is_active():
                self._process_subscription_q(ws)
                time.sleep(self.CHECK_INTERVAL)

        try:
            ws = self.create_connection()
            logger.debug('ws opened')
            self.on_open()
            threading.Thread(target=subscription_q_loop, daemon=True).start()
            try:
                while self.is_active():
                    try:
                        message_data = ws.recv()
                        self.on_message(message_data)
                    except WebSocketTimeoutException:
                        pass
            finally:
                ws.send_close()
                ws.shutdown()
                self.on_close()
                logger.debug('ws closed')
        except Exception as e:
            logger.exception(e)

    @abstractmethod
    def on_message(self, message_data: str):
        pass

    def send_message(self, message: Any):
        assert self._ws
        self._ws.send(json.dumps(message))
