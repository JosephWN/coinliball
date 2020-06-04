from numbers import Real
from typing import Hashable, Any

from dataclasses import dataclass

from .order import OrderSide


@dataclass
class Execution:
    execution_id: Hashable
    timestamp: float
    instrument: str
    side: str
    price: float
    qty: float
    _data: Any = None

    def __post_init__(self):
        self.side = OrderSide.validate(self.side)
        self.validate()

    def validate(self):
        assert self.execution_id
        assert isinstance(self.timestamp, Real)
        assert self.instrument and isinstance(self.instrument, str)
        assert OrderSide.validate(self.side)
        assert isinstance(self.price, Real)
        assert isinstance(self.qty, Real)
