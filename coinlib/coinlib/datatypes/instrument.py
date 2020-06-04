from typing import Any

from dataclasses import dataclass


@dataclass
class Instrument:
    name: str
    base: str
    quote: str
    name_id: str
    base_id: str = None
    quote_id: str = None
    _data: Any = None

    def __post_init__(self):
        self.validate()

    def validate(self):
        assert self.name and isinstance(self.name, str)
        assert self.base and isinstance(self.base, str)
        assert self.quote and isinstance(self.quote, str)
        assert self.name_id and isinstance(self.name_id, str)
        assert isinstance(self.base_id, (str, type(None)))
        assert isinstance(self.quote_id, (str, type(None)))
