import logging
from collections import deque
from typing import Dict, Tuple, Hashable, Any

from pubnub.enums import PNStatusCategory
from pubnub.models.consumer.pubsub import PNMessageResult
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub, SubscribeListener

from coinlib.datatypes.streamdata import StreamData
from coinlib.errors import CoinError
from .streamapi import StreamApi

logger = logging.getLogger(__name__)


class DisconnectListener(SubscribeListener):
    def __init__(self):
        super().__init__()
        self.unexpected_status = None

    def status(self, pub_nub: PubNub, status):
        # only handle disconnect event
        if status.category == PNStatusCategory.PNDisconnectedCategory:
            self.disconnected_event.set()
        elif status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
            self.unexpected_status = status
            self.disconnected_event.set()


class PubNubStreamApi(StreamApi):
    CHECK_INTERVAL = 0.2
    PN_KEY = ''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._subscription_q = deque()
        self._channel_name_map: Dict[str, str] = {}

    def subscribe(self, *args: Tuple[Hashable, Any]):
        for key, channel_name in args:
            self._subscription_q.append(('subscribe', (key, channel_name)))

    def unsubscribe(self, *args: Hashable):
        for key in args:
            self._subscription_q.append(('unsubscribe', (key, '')))

    def _process_subscription_q(self, pn: PubNub):
        # process one
        if len(self._subscription_q):
            op, (key, channel_name) = self._subscription_q.popleft()
            if op == 'subscribe':
                self._channel_name_map[channel_name] = key
                self._subscribe_channel(pn, channel_name)
            elif op == 'unsubscribe':
                for k, v in self._channel_name_map.items():
                    if v == key:
                        self._channel_name_map.pop(k)
                        self._unsubscribe_channel(pn, k)
                        break
            else:
                assert False, f'unknown operation={op}'

    def _subscribe_channel(self, pn: PubNub, channel_name: str):
        _ = self
        pn.subscribe().channels(channel_name).execute()
        logger.debug(f'subscribe channel={channel_name}')

    def _unsubscribe_channel(self, pn: PubNub, channel_name: str):
        _ = self
        pn.unsubscribe().channels(channel_name).execute()
        logger.debug(f'unsubscribe channel={channel_name}')

    def create_connection(self) -> PubNub:
        pn_config = PNConfiguration()
        pn_config.subscribe_key = self.PN_KEY
        pn_config.ssl = True
        return PubNub(pn_config)

    def run(self):
        def on_pn_message(_: PubNub, result: PNMessageResult):
            channel_name = result.channel
            key = self._channel_name_map.get(channel_name)
            self.on_raw_data(StreamData(key, result.message))

        try:
            pn = self.create_connection()
            logger.debug('pn started')
            listener = DisconnectListener()
            listener.message = on_pn_message
            pn.add_listener(listener)
            self.on_open()
            try:
                while self.is_active():
                    self._process_subscription_q(pn)
                    if listener.disconnected_event.wait(timeout=self.CHECK_INTERVAL):
                        if listener.unexpected_status:
                            raise CoinError(listener.unexpected_status)
            finally:
                pn.unsubscribe_all()
                self.on_close()
                logger.debug('pn stopped')
        except Exception as e:
            logger.exception(e)
