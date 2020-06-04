import json
import logging
from typing import Dict

from requests.structures import CaseInsensitiveDict
from websocket import WebSocket

from coinlib.datatypes.streamdata import StreamData
from coinlib.trade.websocketstreamapi import WebSocketStreamApi

logger = logging.getLogger(__name__)


class StreamApi(WebSocketStreamApi):
    URL = 'wss://www.bitmex.com/realtime'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._channel_name_map: Dict[str, str] = CaseInsensitiveDict()

    def _process_subscription_q(self, ws: WebSocket):
        # process one
        if len(self._subscription_q):
            op, (key, channel_name) = self._subscription_q.popleft()
            if op == 'subscribe':
                self._channel_name_map[channel_name] = key
                self._subscribe_channel(channel_name)
                logger.debug(f'subscribe {key} {channel_name}')
            elif op == 'unsubscribe':
                for k, v in self._channel_name_map.items():
                    if v == key:
                        self._unsubscribe_channel(k)
                        logger.debug(f'unsubscribe {key} {k}')
                        break
            else:
                assert False, f'unknown operation={op}'

    def _subscribe_channel(self, channel_name: str):
        self.send_message({
            'op': 'subscribe', 'args': [channel_name],
        })

    def _unsubscribe_channel(self, channel_name: str):
        self.send_message({
            'op': 'unsubscribe', 'args': [channel_name],
        })

    def on_message(self, message_data: str):
        message = json.loads(message_data)
        if isinstance(message, dict):
            if message.get('subscribe') and message.get('success'):
                logger.debug(f'event subscribe {message}')
                return
            if message.get('unsubscribe') and message.get('success'):
                logger.debug(f'event unsubscribe {message}')
                return
            if message.get('error'):
                logger.error(f'event error {message}')
                return
            if message.get('table'):
                self.on_channel_data(message)
                return
            if message.get('info'):
                logger.info(f'event info {message}')
                return

        logger.warning(f'unknown message {message}')

    def on_channel_data(self, message: dict):
        table = message.get('table')
        data = message.get('data', [])
        if data:
            symbol = data[0].get('symbol')
            channel_name = f'{table}:{symbol}'
            key = self._channel_name_map.get(channel_name)
            if key:
                self.on_raw_data(StreamData(key, message))
        channel_name = f'{table}'
        key = self._channel_name_map.get(channel_name)
        if key:
            self.on_raw_data(StreamData(key, message))
