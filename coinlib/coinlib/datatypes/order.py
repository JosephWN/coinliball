from numbers import Real
from typing import Any, Hashable

from dataclasses import dataclass
from requests.utils import CaseInsensitiveDict


class OrderType:
    """only simple order types"""
    MARKET = '_MARKET_'
    LIMIT = '_LIMIT_'


class OrderSide:
    BUY = 'BUY'
    SELL = 'SELL'

    _all = {BUY, SELL}
    _reverse_side_map = CaseInsensitiveDict(BUY=SELL, SELL=BUY)

    @classmethod
    def validate(cls, side: str) -> str:
        side = side.upper()
        assert side in cls._all, f'{side} not in {cls._all}'
        return side

    @classmethod
    def get_reverse_side(cls, side: str) -> str:
        assert side in cls._reverse_side_map
        return cls._reverse_side_map[side]


class OrderState:
    ACTIVE = 'ACTIVE'  # include partially filled
    FILLED = 'FILLED'  # fully filled
    CANCELED = 'CANCELED'  # include partially filled
    ERROR = 'ERROR'
    UNKNOWN = 'UNKNOWN'

    _all = {ACTIVE, FILLED, CANCELED, ERROR, UNKNOWN}

    @classmethod
    def validate(cls, order_state: str) -> str:
        order_state = order_state.upper()
        assert order_state in cls._all, f'{order_state} not in {cls._all}'
        return order_state


@dataclass
class Order:
    order_id: Hashable
    timestamp: float
    instrument: str
    order_type: str  # server dependent string
    side: str
    qty: float
    state: str
    price: float = None
    price_executed_average: float = None
    qty_displayed: float = None
    qty_executed: float = None
    is_hidden: bool = False
    is_iceberg: bool = False
    timestamp_update: float = None
    _data: Any = None

    def __post_init__(self):
        self.side = OrderSide.validate(self.side)
        self.state = OrderState.validate(self.state)
        self.validate()

    @property
    def qty_remained(self) -> float:
        return self.qty - self.qty_executed

    def validate(self):
        assert self.order_id
        assert isinstance(self.timestamp, Real)
        assert self.instrument and isinstance(self.instrument, str)
        assert self.order_type and isinstance(self.order_type, str)
        assert OrderSide.validate(self.side)
        assert isinstance(self.qty, Real)
        assert OrderState.validate(self.state)
        assert isinstance(self.price, (Real, type(None)))
        assert isinstance(self.price_executed_average, (Real, type(None)))
        assert isinstance(self.qty_displayed, (Real, type(None)))
        assert isinstance(self.qty_executed, (Real, type(None)))
        assert isinstance(self.is_hidden, bool)
        assert isinstance(self.is_iceberg, bool)
        assert isinstance(self.timestamp_update, (Real, type(None)))
