from typing import Hashable, Tuple, Optional

from coinlib.datatypes.streamdata import StreamData, StreamType
from coinlib.trade.streamclient import StreamClient as StreamClientBase
from .client import Client
from .streamapi import StreamApi


class StreamClient(Client, StreamClientBase):
    STREAM_API_CLASS = StreamApi

    def request_subscribe(self, key: Tuple[str, Hashable]):
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            instrument: str = key[1]
            pair = self.instruments[instrument].name_id
            self.stream_api.subscribe((key, f'ticker_{pair}'))
        elif stream_type == StreamType.ORDER_BOOK:
            instrument: str = key[1]
            pair = self.instruments[instrument].name_id
            self.stream_api.subscribe((key, f'depth_{pair}'))

    def request_unsubscribe(self, key: Tuple[str, Hashable]):
        self.stream_api.unsubscribe(key)

    def convert_raw_data(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[str, Hashable] = data.key
        data = data.data
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            instrument: str = key[1]
            ticker = self._convert_ticker(instrument, data['data'])
            return StreamData(key, ticker)
        elif stream_type == StreamType.ORDER_BOOK:
            instrument: str = key[1]
            order_book = self._convert_order_book(instrument, data['data'])
            return StreamData(key, order_book)

        return None
