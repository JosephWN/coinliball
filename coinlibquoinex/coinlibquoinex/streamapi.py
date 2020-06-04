import json
import logging
import time
from typing import Dict

from websocket import WebSocket

from coinlib.datatypes.streamdata import StreamData
from coinlib.trade.websocketstreamapi import WebSocketStreamApi

logger = logging.getLogger(__name__)


class StreamApi(WebSocketStreamApi):
    def __init__(self, *, pusher_key: str, **kwargs):
        url = f'wss://ws.pusherapp.com/app/{pusher_key}?protocol=7'
        super().__init__(url, **kwargs)
        self._channel_name_map: Dict[str, str] = {}

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
                        self._channel_name_map.pop(k)
                        self._unsubscribe_channel(k)
                        logger.debug(f'unsubscribe {key} {k}')
                        break
            else:
                assert False, f'unknown operation={op}'

    def _subscribe_channel(self, channel_name: str):
        params = {
            'event': 'pusher:subscribe',
            'data': {
                'channel': channel_name,
            },
        }
        self.send_message(params)

    def _unsubscribe_channel(self, channel_name: str):
        self.send_message({
            'event': 'pusher:unsubscribe',
            'data': {
                'channel': channel_name,
            },
        })

    def on_message(self, message_data: str):
        message: dict = json.loads(message_data)
        event = message.get('event')
        channel_name = message.get('channel')
        data = message.get('data', {})
        if event == 'pusher:connection_established':
            logger.debug('event established {message}')
            # TODO: check connection by pusher_ping
            return
        if event == 'pusher_internal:subscription_succeeded':
            logger.debug(f'event subscribed {message}')
            return
        if event == 'pusher:ping':
            self.on_pusher_ping()
            return
        if event == 'pusher:pong':
            logger.warning(f'pusher_pong {message}')
            self.on_pusher_pong()
            return
        if event == 'pusher:error':
            logger.error(f'event error {message}')
            return
        if channel_name:
            key = self._channel_name_map.get(channel_name)
            if key:
                self.on_raw_data(StreamData(key, data))
            return
        logger.warning(f'unknown event {message}')

    def on_pusher_ping(self):
        self.pusher_pong()

    def on_pusher_pong(self):
        pass

    def pusher_ping(self):
        logger.debug('pusher ping')
        self.send_message({
            'event': 'pusher:ping',
            'data': time.time(),
        })

    def pusher_pong(self):
        logger.debug('pusher pong')
        self.send_message({
            'event': 'pusher:pong',
            'data': '',
        })
