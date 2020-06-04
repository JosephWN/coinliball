import functools
import json
import time
from typing import Hashable, Tuple, Dict, Optional

from coinlib.datatypes import OrderBook
from coinlib.datatypes.streamdata import StreamData, StreamType
from coinlib.trade.streamclient import StreamClient as StreamClientBase
from .client import Client
from .streamapi import StreamApi


class StreamClient(Client, StreamClientBase):
    STREAM_API_CLASS = None

    def __init__(self, *args, pusher_key: str, **kwargs):
        self.STREAM_API_CLASS = functools.partial(StreamApi, pusher_key)
        super().__init__(*args, **kwargs)
        self._channel_data_cache: Dict[Hashable, dict] = {}

    def request_subscribe(self, key: Tuple[str, Hashable]):
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            instrument: str = key[1]
            info = self.instruments[instrument]
            pair = f'{info.base}{info.quote}'.lower()
            channel_name = f'product_cash_{pair}_{info.name_id}'
            self.stream_api.subscribe((key, channel_name))
            return
        if stream_type == StreamType.ORDER_BOOK:
            instrument: str = key[1]
            info = self.instruments[instrument]
            pair = f'{info.base}{info.quote}'.lower()
            self._channel_data_cache[key] = {}
            # subscribe two channels
            for sub_key in ['buy', 'sell']:
                channel_name = f'price_ladders_cash_{pair}_{sub_key}'
                self.stream_api.subscribe((key + (sub_key,), channel_name))
            return

    def request_unsubscribe(self, key: Tuple[str, Hashable]):
        stream_type = key[0]
        if stream_type == StreamType.ORDER_BOOK:
            self._channel_data_cache[key] = {}
            # unsubscribe two channels
            for sub_key in ['buy', 'sell']:
                self.stream_api.unsubscribe((key + (sub_key,)))
            return
        # else
        self.stream_api.unsubscribe(key)

    def convert_raw_data(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[Hashable, ...] = data.key
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            instrument: str = key[1]
            ticker = self._convert_ticker(time.time(), instrument, json.loads(data.data))
            return StreamData(key, ticker)
        if stream_type == StreamType.ORDER_BOOK:
            return self.convert_order_book(data)

        return None

    def convert_order_book(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[Hashable, ...] = data.key
        assert key[0] == StreamType.ORDER_BOOK
        instrument: str = key[1]
        sub_key: str = key[2]
        raw_data: dict = json.loads(data.data)

        cache = self._channel_data_cache.setdefault(key[:2], {})
        if sub_key == 'sell':
            cache['asks'] = [(float(price), float(qty), None) for price, qty in raw_data]
        else:
            cache['bids'] = [(float(price), float(qty), None) for price, qty in raw_data]
        asks = cache.get('asks')
        bids = cache.get('bids')
        if asks and bids:
            order_book = OrderBook(timestamp=time.time(), instrument=instrument, asks=asks, bids=bids, _data=None)
            return StreamData(key[:2], order_book)
