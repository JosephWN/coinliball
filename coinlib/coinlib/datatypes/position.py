from numbers import Real
from typing import Any, Hashable, Optional

from dataclasses import dataclass

from coinlib.datatypes import OrderSide

PositionSide = OrderSide


class PositionState:
    ACTIVE = 'ACTIVE'  # not fully closed
    CLOSED = 'CLOSED'  # fully closed
    CANCELED = 'CANCELED'  # position not opened and order canceled
    UNKNOWN = 'UNKNOWN'

    _all = {ACTIVE, CLOSED, CANCELED, UNKNOWN}

    @classmethod
    def validate(cls, state: str) -> str:
        state = state.upper()
        assert state in cls._all, f'{state} not in {cls._all}'
        return state


@dataclass
class Position:
    position_id: Optional[Hashable]
    timestamp: float
    instrument: str
    side: str
    state: str
    price_ordered: float = None
    price_opened_average: float = None
    price_closed_average: float = None
    qty_ordered: float = None
    qty_opened: float = 0
    qty_closed: float = 0
    timestamp_closed: float = None
    limit: float = None
    stop: float = None
    _data: Any = None

    def __post_init__(self):
        self.side = PositionSide.validate(self.side)
        self.state = PositionState.validate(self.state)
        self.validate()

    @property
    def qty_remained(self) -> float:
        return self.qty_opened - self.qty_closed

    def validate(self):
        assert isinstance(self.timestamp, Real)
        assert self.instrument and isinstance(self.instrument, str)
        assert PositionSide.validate(self.side)
        assert PositionState.validate(self.state)
        assert isinstance(self.price_ordered, (Real, type(None)))
        assert isinstance(self.price_opened_average, (Real, type(None)))
        assert isinstance(self.price_closed_average, (Real, type(None)))
        assert isinstance(self.qty_ordered, (Real, type(None)))
        assert isinstance(self.qty_opened, (Real, type(None)))
        assert isinstance(self.qty_closed, (Real, type(None)))
        assert isinstance(self.limit, (Real, type(None)))
        assert isinstance(self.stop, (Real, type(None)))
