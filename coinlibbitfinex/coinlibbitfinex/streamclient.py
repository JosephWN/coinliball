from collections import defaultdict
import time
from typing import Hashable, Tuple, Optional, DefaultDict

from coinlib.datatypes import OrderBook
from coinlib.datatypes.streamdata import StreamData, StreamType
from coinlib.trade.streamclient import StreamClient as StreamClientBase
from .client import Client
from .streamapi import StreamApi


class StreamClient(Client, StreamClientBase):
    STREAM_API_CLASS = StreamApi

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._channel_data_cache: DefaultDict[Hashable, dict] = defaultdict(dict)

    def request_subscribe(self, key: Tuple[str, Hashable]):
        self._channel_data_cache[key].clear()
        stream_type = key[0]
        if stream_type == StreamType.ORDER_BOOK:
            instrument: str = key[1]
            pair = self.instruments[instrument].name_id
            params = {
                'event': 'subscribe',
                'channel': 'book',
                'pair': pair,
                'prec': 'P0',
                'freq': 'F0',
                'len': '25',
            }
            self.stream_api.subscribe((key, params))
            return

    def request_unsubscribe(self, key: Tuple[str, Hashable]):
        self.stream_api.unsubscribe(key)

    def convert_raw_data(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[str, Hashable] = data.key
        stream_type = key[0]
        if stream_type == StreamType.ORDER_BOOK:
            return self.convert_order_book(data)

        return None

    def convert_order_book(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[str, Hashable] = data.key
        instrument: str = key[1]
        data: list = data.data
        assert isinstance(data, list), data
        body = data[1:]
        cache: dict = self._channel_data_cache[key]
        if body[0] != 'hb':
            if isinstance(body[0], list):
                # snapshot
                body = body[0]
                cache['snapshot'] = True
            else:
                # update
                body = [body]
            prices = cache.setdefault('prices', {})
            for price, count, amount in body:
                prices[price] = (amount, count)
                if count == 0:
                    cache.pop(price, None)
        if not cache.get('snapshot'):
            return
        asks = []
        bids = []
        for price, (amount, count) in cache['prices'].items():
            if amount > 0:
                asks.append((float(price), abs(float(amount)), int(count)))
            elif amount < 0:
                bids.append((float(price), abs(float(amount)), int(count)))
        order_book = OrderBook(timestamp=time.time(), instrument=instrument, asks=asks, bids=bids, _data=None)
        return StreamData(key, order_book)
