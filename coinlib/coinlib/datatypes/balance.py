from numbers import Real
from typing import Any, Optional

from dataclasses import dataclass


class BalanceType:
    MAIN = 'main'
    MARGIN = 'margin'
    LOAN = 'loan'  # TODO: support(currently experimental)
    UNKNOWN = 'unknown'

    _all = (MAIN, MARGIN, LOAN, UNKNOWN)


@dataclass
class Balance:
    balance_type: str
    currency: str
    total: float
    locked: float = None
    _data: Any = None

    def __post_init__(self):
        self.validate()

    @property
    def free(self) -> Optional[Real]:
        if self.locked is None:
            return None
        return self.total - self.locked

    @free.setter
    def free(self, value: Real):
        self.locked = self.total - value

    def validate(self):
        # noinspection PyProtectedMember
        assert self.balance_type in BalanceType._all
        assert self.currency and isinstance(self.currency, str)
        assert isinstance(self.total, Real)
        assert isinstance(self.locked, (Real, type(None)))
