from collections import defaultdict
import logging
import time
from typing import Hashable, Tuple, Any, Optional, Dict, List

from coinlib.datatypes import OrderBook, Ticker
from coinlib.datatypes.streamdata import StreamData, StreamType
from coinlib.trade.streamclient import StreamClient as StreamClientBase
from .client import Client
from .streamapi import StreamApi

logger = logging.getLogger(__name__)


class StreamClient(Client, StreamClientBase):
    STREAM_API_CLASS = StreamApi

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._channel_data_cache: Dict[Hashable, dict] = defaultdict(dict)

    def request_subscribe(self, key: Tuple[str, Hashable]):
        self._channel_data_cache[key].clear()
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            instrument: str = key[1]
            symbol = self.instruments[instrument].name_id
            # two channels
            self.stream_api.subscribe((key + ('quote',), f'quote:{symbol}'))
            self.stream_api.subscribe((key + ('trade',), f'trade:{symbol}'))
            return
        if stream_type == StreamType.ORDER_BOOK:
            instrument: str = key[1]
            symbol = self.instruments[instrument].name_id
            self.stream_api.subscribe((key, f'orderBookL2:{symbol}'))
            return

    def request_unsubscribe(self, key: Tuple[str, Hashable]):
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            # two channels
            self.stream_api.unsubscribe(key + ('quote',))
            self.stream_api.unsubscribe(key + ('trade',))
            return
        self.stream_api.unsubscribe(key)

    def convert_raw_data(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[Hashable, ...] = data.key
        data: Any = self.MessageData(data.data)
        stream_type = key[0]
        if stream_type == StreamType.TICKER:
            cache = self._channel_data_cache[key[:2]]
            instrument: str = key[1]
            sub_type: str = key[2]
            symbol = self.rinstruments[instrument]
            time_str = None
            if sub_type == 'quote':
                if data.action == 'partial':
                    cache['quote_partial_received'] = True
                if not cache.get('quote_partial_received'):
                    return
                for x in sorted(data.data, key=lambda _: _['timestamp']):
                    assert x['symbol'] == symbol, data.message
                    cache['quote'] = x
                    time_str = x['timestamp']
            elif sub_type == 'trade':
                if data.action == 'partial':
                    cache['trade_partial_received'] = True
                if not cache.get('trade_partial_received'):
                    return
                for x in sorted(data.data, key=lambda _: _['timestamp']):
                    assert x['symbol'] == symbol, data.message
                    cache['trade'] = x
                    time_str = x['timestamp']
            else:
                logger.error(f'unknown route {data.message}')
                return

            quote = cache.get('quote')
            trade = cache.get('trade')
            if not quote or not trade or not time_str:
                return
            ticker = Ticker(timestamp=self._parse_time(time_str), instrument=instrument,
                            ask=quote['askPrice'], bid=quote['bidPrice'], last=trade['price'], _data=None)
            return StreamData(key[:2], ticker)
        elif stream_type == StreamType.ORDER_BOOK:
            cache = self._channel_data_cache[key]
            instrument: str = key[1]
            symbol = self.rinstruments[instrument]
            if data.action == 'partial':
                cache['partial_received'] = True

            if not cache.get('partial_received'):
                return

            id_map = cache.setdefault('id_map', {})
            if data.action in ('partial', 'insert'):
                for x in data.data:
                    assert x['symbol'] == symbol, data.message
                    id_map[x['id']] = x
            elif data.action == 'update':
                for x in data.data:
                    assert x['symbol'] == symbol, data.message
                    if x['id'] not in id_map:
                        logger.warning(f'{x} not found')
                    else:
                        id_map[x['id']].update(x)
            elif data.action == 'delete':
                for x in data.data:
                    assert x['symbol'] == symbol, data.message
                    if x['id'] not in id_map:
                        logger.warning(f'{x} not found')
                    else:
                        del id_map[x['id']]

            asks = []
            bids = []
            for x in id_map.values():
                # {'symbol': 'XBTUSD', 'id': 1234567890, 'side': 'Sell', 'size': 100, 'price': 123456}
                side = x['side'].lower()
                if side == 'sell':
                    asks.append((float(x['price']), float(x['size']), x['id']))
                else:
                    assert side == 'buy'
                    bids.append((float(x['price']), float(x['size']), x['id']))
            order_book = OrderBook(timestamp=time.time(), instrument=instrument,
                                   asks=asks, bids=bids, _data=None)
            return StreamData(key[:2], order_book)

        return None

    class MessageData:
        def __init__(self, message: dict):
            self.message = message
            self.table: str = message['table']
            self.action: str = message['action']
            self.data: List[Any] = message['data']
            self.keys: Tuple[str, ...] = message.get('keys', ())
            self.foreign_keys: Dict[str, str] = message.get('foreignKeys', {})
            self.types: Dict[str, str] = message.get('types', {})
            self.filter: Dict[str, str] = message.get('filter', {})
            self.attributes: Dict[str, str] = message.get('attributes', {})
