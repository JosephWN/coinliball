import json
import logging
from typing import Hashable, Dict

from websocket import WebSocket

from coinlib.datatypes.streamdata import StreamData
from coinlib.trade.websocketstreamapi import WebSocketStreamApi

logger = logging.getLogger(__name__)


class StreamApi(WebSocketStreamApi):
    URL = 'wss://api.bitfinex.com/ws'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._subscriptions = {}
        self._channel_id_map: Dict[int, Hashable] = {}

    def _process_subscription_q(self, ws: WebSocket):
        # process one
        if len(self._subscription_q):
            op, (key, params) = self._subscription_q.popleft()
            if op == 'subscribe':
                self._subscriptions[key] = params
                self._subscribe_channel(params)
                logger.debug(f'subscribe {key} {params}')
            elif op == 'unsubscribe':
                params = self._subscriptions.pop(key, None)
                if params is not None:
                    for channel_id, v in self._channel_id_map.items():
                        if v == key:
                            self._unsubscribe_channel(channel_id)
                            logger.debug(f'unsubscribe {key} {params} {channel_id}')
                            break
            else:
                assert False, f'unknown operation={op}'

    def _subscribe_channel(self, params: dict):
        request = dict(event='subscribe')
        request.update(params)
        self.send_message(request)

    def _unsubscribe_channel(self, channel_id: int):
        self.send_message({
            'event': 'unsubscribe',
            'chanId': channel_id,
        })

    def on_message(self, message_data: str):
        message = json.loads(message_data)
        if isinstance(message, dict):
            event = message.get('event')
            if event == 'info':
                logger.debug(f'event info {message}')
                return
            elif event == 'subscribed':
                self.on_subscribed(message)
                return
            elif event == 'unsubscribed':
                self.on_unsubscribed(message)
                return
            elif event == 'error':
                self.on_error(message)
                return
            else:
                logger.warning(f'event unsupported {message}')
                return
        if isinstance(message, list):
            self.on_channel_data(message)
            return

        logger.warning(f'unknown message {message}')

    def on_subscribed(self, message: dict):
        channel_name = message['channel']

        for key, params in self._subscriptions.items():
            if channel_name == params.get('channel'):
                if channel_name == 'book':
                    # TODO: distinguish between order_book and raw_order_book
                    if message['pair'].upper() != params.get('pair', '').upper():
                        continue
                channel_id = int(message['chanId'])
                self._channel_id_map[channel_id] = key
                logger.debug(f'event subscribed {message}')
                return

        logger.warning('unknown event subscribe {message}')

    def on_unsubscribed(self, message: dict):
        _ = self
        logger.debug(f'event unsubscribed {message}')

    def on_error(self, message: dict):
        _ = self
        logger.error(f'event error {message}')

    def on_channel_data(self, data: list):
        channel_id = data[0]
        key = self._channel_id_map.get(channel_id)
        if key:
            self.on_raw_data(StreamData(key, data))
