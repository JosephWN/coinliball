from abc import abstractmethod, ABC
from collections import defaultdict
import logging
from typing import Dict, Hashable, cast, Type, Optional, Iterable, Iterator, List

from requests.structures import CaseInsensitiveDict

from coinlib.datatypes import Instrument, OrderBook, Order, Execution, Ticker, Balance
from coinlib.datatypes.candle import Candle
from coinlib.datatypes.order import OrderType
from coinlib.datatypes.position import Position
from coinlib.trade.restapi import RestApi

logger = logging.getLogger(__name__)


class Client(ABC):
    REST_API_CLASS: Type[RestApi] = None

    def __init__(self, credential: dict = None, **kwargs):
        credential = credential or {}
        self.credential = credential
        self.api = self.REST_API_CLASS(credential, **kwargs)

        self._instruments: Dict[str, Instrument] = CaseInsensitiveDict()
        self._rinstruments: Dict[str, str] = CaseInsensitiveDict()
        self._currencies: Dict[str, str] = CaseInsensitiveDict()
        self._rcurrencies: Dict[str, str] = CaseInsensitiveDict()

    @property
    def api_key(self) -> str:
        return self.credential.get('api_key')

    @property
    def api_secret(self) -> str:
        return self.credential.get('api_secret')

    def public_get(self, path: str, **kwargs):
        return self.api.public_get(path, **kwargs)

    def public_post(self, path: str, **kwargs):
        return self.api.public_post(path, **kwargs)

    def public_put(self, path: str, **kwargs):
        return self.api.public_put(path, **kwargs)

    def public_patch(self, path: str, **kwargs):
        return self.api.public_patch(path, **kwargs)

    def public_delete(self, path: str, **kwargs):
        return self.api.public_delete(path, **kwargs)

    def private_get(self, path: str, **kwargs):
        return self.api.private_get(path, **kwargs)

    def private_post(self, path: str, **kwargs):
        return self.api.private_post(path, **kwargs)

    def private_put(self, path: str, **kwargs):
        return self.api.private_put(path, **kwargs)

    def private_patch(self, path: str, **kwargs):
        return self.api.private_patch(path, **kwargs)

    def private_delete(self, path: str, **kwargs):
        return self.api.private_delete(path, **kwargs)

    @abstractmethod
    def get_instruments(self) -> Dict[str, Instrument]:
        pass

    @property
    def instruments(self) -> Dict[str, Instrument]:
        if not self._instruments:
            instrument_map = CaseInsensitiveDict({k.upper(): v for k, v in self.get_instruments().items()})
            self._instruments = instrument_map
        return cast(Dict[str, Instrument], self._instruments)

    @instruments.setter
    def instruments(self, value):
        self._instruments = value

    @property
    def rinstruments(self) -> Dict[str, str]:
        if not self._rinstruments:
            self._rinstruments = CaseInsensitiveDict(
                {str(v.name_id).upper(): k for k, v in self.instruments.items()})
        return cast(Dict[str, str], self._rinstruments)

    def get_currencies(self) -> Dict[str, str]:
        currencies = {}
        for v in self.instruments.values():
            currencies[v.base] = v.base_id
            currencies[v.quote] = v.quote_id
        return currencies

    @property
    def currencies(self) -> Dict[str, str]:
        if not self._currencies:
            currency_map = CaseInsensitiveDict({k.upper(): v for k, v in self.get_currencies().items()})
            self._currencies = currency_map
        return cast(Dict[str, str], self._currencies)

    @currencies.setter
    def currencies(self, value):
        self._currencies = value

    @property
    def rcurrencies(self) -> Dict[str, str]:
        if not self._rcurrencies:
            self._rcurrencies = CaseInsensitiveDict({v.upper(): k.upper() for k, v in self.currencies.items()})
        return cast(Dict[str, str], self._rcurrencies)

    # public methods

    @abstractmethod
    def get_ticker(self, instrument: str, *, params: dict = None) -> Ticker:
        pass

    @abstractmethod
    def get_order_book(self, instrument: str, *, params: dict = None) -> OrderBook:
        pass

    @abstractmethod
    def get_public_executions(self, instrument: str, *, params: dict = None) -> Iterator[Execution]:
        """time descending order"""

    def get_candles(self, instrument: str, candle_size: str, *, params: dict = None) -> Iterator[Candle]:
        """time descending order"""

    # private methods

    @abstractmethod
    def get_balances_list(self, *, params: dict = None) -> List[Balance]:
        pass

    def get_balances(self, *, params: dict = None) -> Dict[str, Dict[str, Balance]]:
        balances: dict = defaultdict(CaseInsensitiveDict)
        for balance in self.get_balances_list(params=params):
            balances[balance.balance_type][balance.currency] = balance
        return balances

    @abstractmethod
    def get_orders(self, *, instrument: str = None, active_only: bool = True,
                   order_ids: Iterable[Hashable] = None, params: dict = None) -> Iterator[Order]:
        """
        each filtering-argument(instrument, active_only, ...) is available or not depends on server spec
        not raised if order not found

        :param instrument: filter by instrument
        :param active_only: filter by order-state is ACTIVE
        :param order_ids: filter by order-ids. if specified, other filtering-arguments are ignored
        :param params:
        :return: Order generator
        """
        pass

    @abstractmethod
    def submit_order(self, instrument: str, order_type: str, side: str, price: Optional[float], qty: float,
                     *, margin: bool = False, leverage: float = None, params: dict = None) -> Order:
        """
        :param instrument:
        :param order_type:
        :param side:
        :param price:
        :param qty:
        :param margin: specify True if margin order
        :param leverage: leverage-level
        :param params: server dependent REST API parameters
        :return: Order depends on server spec
        """
        pass

    def submit_market_order(self, instrument: str, side: str, qty: float,
                            *, margin: bool = False, leverage: float = None, params: dict = None) -> Order:
        return self.submit_order(instrument, OrderType.MARKET, side, None, qty,
                                 margin=margin, leverage=leverage, params=params)

    def submit_limit_order(self, instrument: str, side: str, price: float, qty: float,
                           *, margin: bool = False, leverage: float = None, params: dict = None) -> Order:
        return self.submit_order(instrument, OrderType.LIMIT, side, price, qty,
                                 margin=margin, leverage=leverage, params=params)

    @abstractmethod
    def cancel_order(self, order_id: Hashable, *, params: dict = None) -> Optional[Order]:
        """
        :param order_id: Order.order_id
        :param params:
        :return: Order if server return order-data else None
        """
        pass

    @abstractmethod
    def update_order(self, order_id: Hashable, *, params: dict = None) -> Optional[Order]:
        """
        :param order_id: Order.order_id
        :param params:
        :return: Order if server return order-data else None
        """
        pass

    @abstractmethod
    def get_private_executions(self, instrument: str, *, params: dict = None) -> Iterator[Execution]:
        """time descending order"""
        pass

    @abstractmethod
    def get_positions(self, *, instrument: str = None, active_only: bool = True,
                      position_ids: Iterable[Hashable] = None, params: dict = None) -> Iterator[Position]:
        """
        each filtering-argument(instrument, active_only, ...) is available or not depends on server spec
        not raised if position not found

        :param instrument: filter by instrument
        :param active_only: filter by position-state is ACTIVE
        :param position_ids: filter by position-ids. if specified, other filtering-arguments are ignored
        :param params:
        :return: Position generator
        """
        pass

    @abstractmethod
    def close_position(self, position_id: Hashable, *, qty: float = None, params: dict = None) -> Optional[Position]:
        """
        :param position_id:
        :param qty: close partially if supported
        :param params:
        :return:
        """
        pass
