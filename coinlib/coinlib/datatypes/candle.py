from numbers import Real
from typing import Any

from dataclasses import dataclass


@dataclass
class Candle:
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float = None
    _data: Any = None

    def __post_init__(self):
        self.validate()

    def validate(self):
        assert isinstance(self.timestamp, Real)
        assert isinstance(self.open, Real)
        assert isinstance(self.high, Real)
        assert isinstance(self.low, Real)
        assert isinstance(self.close, Real)
        assert isinstance(self.volume, (Real, type(None)))
