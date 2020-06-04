import math
from numbers import Real
from typing import List, Tuple, Any

from dataclasses import dataclass
from requests.structures import CaseInsensitiveDict


@dataclass
class OrderBook:
    timestamp: float
    instrument: str
    asks: List[Tuple[float, float, Any]]
    bids: List[Tuple[float, float, Any]]
    _data: Any = None

    def __post_init__(self):
        self.asks = self.asks.copy()
        self.asks.sort()
        self.bids = self.bids.copy()
        self.bids.sort(reverse=True)
        self.validate()

    _reverse_side_map = CaseInsensitiveDict(asks='bids', bids='asks')

    @classmethod
    def get_reverse_side(cls, side: str) -> str:
        return cls._reverse_side_map[side]

    def validate(self):
        assert isinstance(self.timestamp, Real)
        assert self.instrument and isinstance(self.instrument, str)
        assert isinstance(self.asks, list)
        price = -math.inf
        for ask in self.asks:
            assert len(ask) == 3
            assert isinstance(ask[0], Real)
            assert isinstance(ask[1], Real)
            assert ask[0] >= price
            price = ask[0]
        assert isinstance(self.bids, list)
        price = math.inf
        for bid in self.bids:
            assert len(bid) == 3
            assert isinstance(bid[0], Real)
            assert isinstance(bid[1], Real)
            assert bid[0] <= price
            price = bid[0]
