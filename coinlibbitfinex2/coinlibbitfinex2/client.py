from collections import OrderedDict
import enum
import logging
import time
from typing import Hashable, Iterable, Optional, Iterator, List

from coinlib.datatypes import Instrument, OrderBook, Balance, Execution, Ticker
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.order import OrderState, Order, OrderType, OrderSide
from coinlib.datatypes.position import Position, PositionState
from coinlib.errors import NotSupportedError
from coinlib.trade.client import Client as ClientBase
from coinlib.utils.decorators import dedup
from coinlibbitfinex.client import Client as V1Client
from .restapi import RestApi

logger = logging.getLogger(__name__)


class Flag(enum.IntFlag):
    HIDDEN = 2 ** 6  # 64 hidden order
    CLOSE = 2 ** 9  # 512 close position if position present
    REDUCE_ONLY = 2 ** 10  # 1024
    POST_ONLY = 2 ** 12  # 4096
    OCO = 2 ** 14  # 16384 OCO
    NO_VR = 2 ** 19  # 524288 NO Variable Rates


class Client(ClientBase):
    CURRENCY_MAP = {
        # TODO: map all currencies
        'DSH': 'DASH',
        'IOT': 'IOTA',
        'QSH': 'QASH',
    }
    REST_API_CLASS = RestApi

    def get_instruments(self):
        instrument_map = {}
        for info in V1Client().public_get('/symbols_details'):
            pair = info['pair'].upper()
            base_id, quote_id = pair[:-3], pair[-3:]
            base, quote = base_id.upper(), quote_id.upper()
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            name = f'{base}_{quote}'
            symbol = f't{pair}'
            instrument_map[name] = Instrument(name=name, base=base, quote=quote,
                                              name_id=symbol, base_id=base_id, quote_id=quote_id, _data=info)
        return instrument_map

    # public methods

    def _convert_ticker(self, timestamp: float, instrument: str, data: list) -> Ticker:
        return Ticker(timestamp=timestamp, instrument=instrument,
                      ask=float(data[2]), bid=float(data[0]), last=float(data[6]),
                      volume_24h=float(data[7]), _data=data)

    def get_ticker(self, instrument: str) -> Ticker:
        symbol = self.instruments[instrument].name_id
        timestamp = time.time()
        res = self.public_get(f'/ticker/{symbol}')
        return self._convert_ticker(timestamp, instrument, res)

    def _convert_order_book(self, timestamp: float, instrument: str, data: list):
        asks = []
        bids = []
        for price, count, amount in data:
            if amount < 0:
                asks.append((float(price), float(amount), count))
            else:
                bids.append((float(price), float(amount), count))
        return OrderBook(timestamp=timestamp, instrument=instrument, asks=asks, bids=bids, _data=data)

    def get_order_book(self, instrument: str) -> OrderBook:
        symbol = self.instruments[instrument].name_id
        res = self.public_get(f'/book/{symbol}/P0')
        return self._convert_order_book(time.time(), instrument, res)

    def _convert_public_execution(self, instrument: str, data: list) -> Execution:
        keys = [
            'id', 'mts', 'amount', 'price',
        ]
        data = OrderedDict(zip(keys, data), _extra=data[len(keys):])
        qty = data['amount']
        side = OrderSide.BUY if qty > 0 else OrderSide.SELL
        qty = abs(qty)
        return Execution(execution_id=data['id'],
                         timestamp=data['mts'] / 1000,
                         instrument=instrument,
                         side=side, price=data['price'], qty=qty, _data=data)

    @dedup(lambda x: x.execution_id)
    def get_public_executions(self, instrument: str) -> Iterator[Execution]:
        symbol = self.instruments[instrument].name_id
        limit = 500
        kwargs = dict(limit=limit, sort=-1)
        end = int(time.time() * 1000)
        last_id = None
        while True:
            kwargs.update(end=end)
            res = self.public_get(f'/trades/{symbol}/hist', **kwargs)
            last_execution = None
            for x in res:
                last_execution = self._convert_public_execution(instrument, x)
                yield last_execution
            if len(res) < limit:
                break
            assert last_id != last_execution.execution_id
            last_id = last_execution.execution_id
            # noinspection PyProtectedMember
            end = last_execution._data['mts']

    # private methods

    def _convert_balance(self, data: list) -> Balance:
        keys = [
            'wallet_type', 'currency', 'balance', 'unsettled_interest', 'balance_available',
        ]
        data = OrderedDict(zip(keys, data), _extra=data[len(keys):])
        balance_type = self._balance_type_map.get(data['wallet_type'])
        currency = self.CURRENCY_MAP.get(data['currency'], data['currency'])
        total = data['balance']
        available = data['balance_available']
        if available is not None:
            locked = total - available
        else:
            locked = None
        return Balance(balance_type=balance_type, currency=currency, total=total, locked=locked, _data=data)

    _balance_type_map = {
        'exchange': BalanceType.MAIN,
        'margin': BalanceType.MARGIN,
        'funding': BalanceType.LOAN,
    }

    def get_balances_list(self) -> List[Balance]:
        balances = []
        for data in self.private_post('/auth/r/wallets'):
            balance = self._convert_balance(data)
            balances.append(balance)
        return balances

    def _convert_order(self, data: list) -> Order:
        keys = [
            'id', 'gid', 'cid',
            'symbol', 'mts_create', 'mts_update',
            'amount', 'amount_orig', 'type', 'type_prev',
            '_0', '_1',
            'flags', 'status',
            '_2', '_3',
            'price', 'price_avg', 'price_aux_limit',
            '_4', '_5', '_6',
            'hidden', 'place_id',
        ]
        order = OrderedDict(zip(keys, data), _extra=data[len(keys):])
        instrument = self.rinstruments[order['symbol']]
        qty = order['amount_orig']
        side = OrderSide.BUY if qty > 0 else OrderSide.SELL
        qty = abs(qty)
        status = order['status']
        status_map = {
            'ACTIVE': OrderState.ACTIVE,
            'EXECUTED': OrderState.FILLED,
            'PARTIALLY FILLED': OrderState.ACTIVE,
            'CANCELED': OrderState.CANCELED,
        }
        state = status_map.get(status)
        if not state:
            # assume streaming response
            if 'EXECUTED' in state:
                state = OrderState.FILLED
            elif 'CANCELED' in state or 'was:' in state:
                state = OrderState.CANCELED
            else:
                state = OrderState.ACTIVE
        order['flags'] = Flag(order['flags'])
        return Order(order_id=order['id'],
                     timestamp=order['mts_create'] / 1000,
                     order_type=order['type'],
                     instrument=instrument,
                     side=side,
                     price=order['price'],
                     price_executed_average=order['price_avg'] or None,
                     qty=qty,
                     qty_executed=qty - abs(order['amount']),
                     is_hidden=Flag.HIDDEN in order['flags'],
                     state=state,
                     timestamp_update=order['mts_update'] / 1000,
                     _data=order)

    def get_orders(self, *, instrument: str = None, active_only: bool = True,
                   order_ids: Iterable[Hashable] = None) -> Iterator[Order]:
        if order_ids:
            raise NotSupportedError('order_ids not supported')
        if not active_only:
            raise NotSupportedError('active orders only')
        if not instrument:
            raise NotSupportedError('instrument required')
        symbol = self.instruments[instrument].name_id
        res = self.private_post(f'/auth/r/orders/{symbol}')
        for order in res:
            yield self._convert_order(order)

    _order_type_map = {
        # (type, margin)
        (OrderType.MARKET, False): 'exchange market',
        (OrderType.LIMIT, False): 'exchange limit',
        (OrderType.MARKET, True): 'market',
        (OrderType.LIMIT, True): 'limit',
    }

    def submit_order(self, instrument: str, order_type: str, side: str, price: float, qty: float,
                     *, margin: bool = False, leverage: float = None, params: dict = None) -> Order:
        raise NotSupportedError('not supported by REST API')

    def cancel_order(self, order_id: Hashable, *, params: dict = None) -> Optional[Order]:
        raise NotSupportedError('not supported by REST API')

    def update_order(self, order_id: Hashable, *, params: dict = None) -> Optional[Order]:
        raise NotSupportedError('not supported by REST API')

    def _convert_private_execution(self, data: list) -> Execution:
        keys = [
            'id', 'symbol', 'mts_create', 'order_id',
            'exec_amount', 'exec_price', 'order_type', 'order_price',
            'maker', 'fee', 'fee_currency',
        ]
        data = OrderedDict(zip(keys, data), _extra=data[len(keys):])
        qty = data['exec_amount']
        side = OrderSide.BUY if qty > 0 else OrderSide.SELL
        qty = abs(qty)
        return Execution(execution_id=data['id'],
                         timestamp=data['mts_create'] / 1000,
                         instrument=self.rinstruments[data['symbol']],
                         side=side, price=data['exec_price'], qty=qty, _data=data)

    @dedup(lambda x: x.execution_id)
    def get_private_executions(self, instrument: str) -> Iterator[Execution]:
        symbol = self.instruments[instrument].name_id
        limit = 500
        kwargs = dict(limit=limit)
        end = int(time.time() * 1000)
        last_id = None
        while True:
            kwargs.update(end=end)
            res = self.private_post(f'/auth/r/trades/{symbol}/hist', **kwargs)
            last_execution = None
            for x in res:
                last_execution = self._convert_private_execution(x)
                yield last_execution
            if len(res) < limit:
                break
            assert last_id != last_execution.execution_id
            last_id = last_execution.execution_id
            # noinspection PyProtectedMember
            end = last_execution._data['mts_create']

    def _convert_position(self, data: list) -> Position:
        keys = [
            'symbol', 'status', 'amount', 'base_price',
            'margin_funding', 'margin_funding_type',
            'pl', 'pl_perc', 'price_liq', 'leverage',
        ]
        position = OrderedDict(zip(keys, data), _extra=data[len(keys):])
        instrument = self.rinstruments[position['symbol']]
        status_map = {
            'ACTIVE': PositionState.ACTIVE,
            'CLOSED': PositionState.CLOSED,
        }
        state = status_map.get(position['status'])
        assert state, position
        price = position['base_price']
        qty = position['amount']
        side = 'BUY' if qty > 0 else 'SELL'
        qty = abs(qty)
        return Position(position_id=None, timestamp=time.time(),
                        instrument=instrument, side=side, state=state,
                        price_opened_average=price,
                        qty_opened=qty, _data=position)

    def get_positions(self, *, instrument: str = None, active_only: bool = True,
                      position_ids: Iterable[Hashable] = None) -> Iterator[Position]:
        if position_ids:
            raise NotSupportedError('position_ids not supported')
        if not active_only:
            raise NotSupportedError('active orders only')
        for data in self.private_post('/auth/r/positions'):
            position = self._convert_position(data)
            if not instrument or instrument == position.instrument:
                yield position

    def close_position(self, position_id: Hashable, *, qty: float = None) -> Optional[Position]:
        raise NotSupportedError('not supported by REST API')
