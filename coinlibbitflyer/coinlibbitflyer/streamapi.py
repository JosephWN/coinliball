import itertools
import json
import logging
from typing import Dict, Any

from websocket import WebSocket

from coinlib.datatypes.streamdata import StreamData
from coinlib.trade.websocketstreamapi import WebSocketStreamApi

logger = logging.getLogger(__name__)


class StreamApi(WebSocketStreamApi):
    URL = 'wss://ws.lightstream.bitflyer.com/json-rpc'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._subscriptions = {}
        self._channel_name_map: Dict[str, str] = {}
        self._request_id = itertools.count(1)

    def _process_subscription_q(self, ws: WebSocket):
        # process one
        if len(self._subscription_q):
            op, (key, channel_name) = self._subscription_q.popleft()
            if op == 'subscribe':
                self._channel_name_map[channel_name] = key
                self.request('subscribe', dict(channel=channel_name))
            elif op == 'unsubscribe':
                for k, v in self._channel_name_map.items():
                    if v == key:
                        self._channel_name_map.pop(k)
                        self.request('unsubscribe', dict(channel=k))
                        break
            else:
                assert False, f'unknown operation={op}'

    def request(self, method: str, params: Any, version='2.0'):
        request_id = next(self._request_id)
        self.send_message({
            'jsonrpc': version,
            'method': method,
            'params': params,
            'id': request_id,
        })

    def on_message(self, message_data: str):
        message = json.loads(message_data)
        if isinstance(message, dict):
            method = message.get('method')
            if method == 'channelMessage':
                params = message['params']
                channel = params['channel']
                key = self._channel_name_map.get(channel)
                if key:
                    self.on_raw_data(StreamData(key, params['message']))
