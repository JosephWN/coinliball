from numbers import Real
from typing import Any

from dataclasses import dataclass


@dataclass
class Ticker:
    timestamp: float
    instrument: str
    ask: float
    bid: float
    last: float
    volume_24h: float = None
    _data: Any = None

    def __post_init__(self):
        self.validate()

    def validate(self):
        assert isinstance(self.timestamp, Real)
        assert self.instrument and isinstance(self.instrument, str)
        assert isinstance(self.ask, Real)
        assert isinstance(self.bid, Real)
        assert isinstance(self.last, Real)
        assert isinstance(self.volume_24h, (Real, type(None)))
