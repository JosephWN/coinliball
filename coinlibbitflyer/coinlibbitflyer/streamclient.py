from collections import defaultdict
import time
from typing import Hashable, Tuple, Any, Optional, DefaultDict

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
        if stream_type == StreamType.TICKER:
            instrument: str = key[1]
            pair = self.instruments[instrument].name_id
            self.stream_api.subscribe((key, f'lightning_ticker_{pair}'))
            return
        if stream_type == StreamType.ORDER_BOOK:
            instrument: str = key[1]
            pair = self.instruments[instrument].name_id
            # subscribe two channels
            internal_key = key + ('snapshot',)
            internal_key2 = key + ('update',)
            self.stream_api.subscribe((internal_key, f'lightning_board_snapshot_{pair}'))
            self.stream_api.subscribe((internal_key2, f'lightning_board_{pair}'))
            return

    def request_unsubscribe(self, key: Tuple[str, Hashable]):
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            self.stream_api.unsubscribe(key)
            return
        if stream_type == StreamType.ORDER_BOOK:
            # unsubscribe two channels
            internal_key = key + ('snapshot',)
            internal_key2 = key + ('update',)
            self.stream_api.unsubscribe(internal_key)
            self.stream_api.unsubscribe(internal_key2)
            return

    def convert_raw_data(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[Hashable, ...] = data.key
        data: Any = data.data
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            instrument: str = key[1]
            ticker = self._convert_ticker(instrument, data)
            return StreamData(key, ticker)
        elif stream_type == StreamType.ORDER_BOOK:
            instrument: str = key[1]
            kind: str = key[2]
            cache: dict = self._channel_data_cache[key[:2]]
            timestamp = time.time()
            if kind == 'snapshot':
                for k in ['asks', 'bids']:
                    cache[k] = {x['price']: x['size'] for x in data[k]}
                cache['snapshot'] = True
            else:
                for k in ['asks', 'bids']:
                    cache.setdefault(k, {})
                    for x in data[k]:
                        cache[k][x['price']] = x['size']
                        if not x['size']:
                            del cache[k][x['price']]
            if cache.get('snapshot'):
                asks = [(float(price), float(qty), None) for price, qty in cache['asks'].items()]
                bids = [(float(price), float(qty), None) for price, qty in cache['bids'].items()]
                order_book = OrderBook(timestamp=timestamp, instrument=instrument,
                                       asks=asks, bids=bids, _data=None)
                # return (key, data)
                return StreamData(key[:2], order_book)

        return None
