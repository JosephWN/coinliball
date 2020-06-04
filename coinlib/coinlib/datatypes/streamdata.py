from typing import Hashable, Any

from dataclasses import dataclass


class StreamType:
    TICKER = 'ticker'
    ORDER_BOOK = 'order_book'
    EXECUTION = 'execution'
    # TODO: support following types(currently not supported)
    CANDLE = 'candle'
    PRIVATE_BALANCE = 'private_balance'
    PRIVATE_EXECUTION = 'private_execution'
    PRIVATE_ORDER = 'private_order'
    PRIVATE_POSITION = 'private_position'

    _all = {TICKER, ORDER_BOOK, EXECUTION, CANDLE,
            PRIVATE_BALANCE, PRIVATE_EXECUTION, PRIVATE_ORDER, PRIVATE_POSITION}

    @classmethod
    def validate(cls, stream_type: str) -> str:
        stream_type = stream_type.upper()
        assert stream_type in cls._all, f'{stream_type} not in {cls._all}'
        return stream_type


@dataclass
class StreamData:
    key: Hashable
    data: Any
