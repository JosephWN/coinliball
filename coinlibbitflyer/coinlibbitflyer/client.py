import time
from typing import Hashable, Iterable, Optional, Iterator, List

import dateutil.parser
import pytz

from coinlib.datatypes import Instrument, OrderBook, Balance, Execution, Order, Ticker
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.order import OrderState, OrderType
from coinlib.datatypes.position import Position, PositionState
from coinlib.errors import NotSupportedError
from coinlib.trade.client import Client as ClientBase
from .restapi import RestApi


class Client(ClientBase):
    REST_API_CLASS = RestApi
    CURRENCY_MAP = {
        'BTC': 'BTC', 'JPY': 'JPY', 'ETH': 'ETH', 'LTC': 'LTC', 'BCH': 'BCH',
        'ETC': 'ETC', 'MONA': 'MONA', 'LISK': 'LSK',
    }

    def _check_is_margin_instrument(self, instrument: str) -> bool:
        return instrument == 'FX_BTC_JPY' or '_' not in instrument

    def get_instruments(self):
        instrument_map = {}
        for product in self.public_get('/markets'):
            product_code = product['product_code']
            split = product_code.split('_')
            if len(split) == 2:
                # spot
                base_id, quote_id = split
                base, quote = base_id, quote_id
                name = f'{base}_{quote}'
            else:
                # not spot
                base_id, quote_id = None, None
                base, quote = base_id, quote_id
                name = product_code
            instrument_map[name] = Instrument(name=name, base=base, quote=quote,
                                              name_id=product_code, base_id=base_id, quote_id=quote_id,
                                              _data=product)
        return instrument_map

    def get_currencies(self):
        return self.CURRENCY_MAP.copy()

    def _parse_time(self, time_str: str) -> float:
        dt = dateutil.parser.parse(time_str)
        if not dt.tzinfo:
            dt = pytz.UTC.localize(dt)
        return dt.timestamp()

    def _convert_ticker(self, instrument: str, data: dict) -> Ticker:
        return Ticker(timestamp=self._parse_time(data['timestamp']), instrument=instrument,
                      ask=data['best_ask'], bid=data['best_bid'], last=data['ltp'],
                      volume_24h=data['volume'], _data=data)

    def get_ticker(self, instrument: str) -> Ticker:
        product_code = self.instruments[instrument].name_id
        res = self.public_get('/ticker', product_code=product_code)
        return self._convert_ticker(instrument, res)

    def _convert_order_book(self, timestamp: float, instrument: str, data: dict) -> OrderBook:
        _ = self
        asks_bids = {}
        for k in ['asks', 'bids']:
            asks_bids[k] = [(v['price'], v['size'], None) for v in data[k]]
        return OrderBook(timestamp=timestamp, instrument=instrument, **asks_bids, _data=data)

    def get_order_book(self, instrument: str) -> OrderBook:
        product_code = self.instruments[instrument].name_id
        timestamp = time.time()
        res = self.public_get('/board', product_code=product_code)
        order_book = self._convert_order_book(timestamp, instrument, res)
        order_book.timestamp = timestamp
        return order_book

    def get_public_executions(self, instrument: str, **kwargs) -> Iterator[Execution]:
        limit = 500
        kwargs = {
            'product_code': self.instruments[instrument].name_id,
            'count': limit,
        }
        kwargs.update(_params or {})
        while True:
            res = self.public_get('/executions', **kwargs)
            for x in res:
                execution_id = x['id']
                timestamp = self._parse_time(x['exec_date'])
                execution = Execution(execution_id=execution_id,
                                      timestamp=timestamp,
                                      instrument=instrument,
                                      side=x['side'],
                                      price=x['price'],
                                      qty=x['size'],
                                      _data=x)
                yield execution
            if len(res) < limit:
                break
            kwargs.update(before=res[-1]['id'])

    def get_balances_list(self) -> List[Balance]:
        balances = []
        for data in self.private_get('/getbalance'):
            currency = self.rcurrencies[data['currency_code']]
            balance = Balance(balance_type=BalanceType.MAIN,
                              currency=currency,
                              total=data['amount'],
                              locked=data['amount'] - data['available'],
                              _data=data)
            balances.append(balance)
        for data in self.private_get('/getcollateralaccounts'):
            currency = self.rcurrencies[data['currency_code']]
            balance = Balance(balance_type=BalanceType.MARGIN,
                              currency=currency,
                              total=data['amount'],
                              _data=data)
            balances.append(balance)
        return balances

    def _convert_order(self, order: dict) -> Order:
        instrument = self.rinstruments[order['product_code']]
        side = order['side']
        child_state = order['child_order_state'].upper()
        if child_state == 'ACTIVE':
            state = OrderState.ACTIVE
        elif child_state == 'COMPLETED':
            state = OrderState.FILLED
        elif child_state in ('CANCELED', 'EXPIRED'):
            state = OrderState.CANCELED
        elif child_state == 'REJECTED':
            state = OrderState.ERROR
        else:
            state = OrderState.UNKNOWN
        return Order(order_id=(instrument, order['child_order_acceptance_id']),
                     timestamp=self._parse_time(order['child_order_date']),
                     order_type=order['child_order_type'],
                     instrument=instrument,
                     side=side,
                     price=order['price'],
                     price_executed_average=order['average_price'],
                     qty=order['size'],
                     qty_executed=order['executed_size'],
                     state=state,
                     _data=order)

    def get_orders(self, *, instrument: str = None, active_only: bool = True,
                   order_ids: Iterable[Hashable] = None) -> Iterator[Order]:
        if order_ids:
            for instrument, order_id in order_ids:
                kwargs = {
                    'product_code': self.instruments[instrument].name_id,
                    'child_order_acceptance_id': order_id,
                }
                res = self.private_get('/getchildorders', **kwargs)
                for v in res:
                    yield self._convert_order(v)
        else:
            assert instrument, 'instrument required'
            limit = 500
            kwargs = {
                'product_code': self.instruments[instrument].name_id,
                'count': limit,
            }
            if active_only:
                kwargs.update(child_order_state='ACTIVE')
            while True:
                res = self.private_get('/getchildorders', **kwargs)
                for v in res:
                    yield self._convert_order(v)
                if len(res) < limit:
                    break
                kwargs.update(before=res[-1]['id'])

    _order_type_map = {
        OrderType.MARKET: 'MARKET',
        OrderType.LIMIT: 'LIMIT',
    }

    def submit_order(self, instrument: str, order_type: str, side: str, price: float, qty: float,
                     *, margin: bool = False, leverage: float = None, params: dict = None) -> Order:
        kwargs = {}
        product_code = self.instruments[instrument].name_id
        order_type = self._order_type_map.get(order_type, order_type)
        kwargs.update(product_code=product_code, side=side, child_order_type=order_type, size=qty)
        if price:
            kwargs.update(price=price)
        assert self._check_is_margin_instrument(instrument) is margin
        kwargs.update(params or {})
        res = self.private_post('/sendchildorder', **kwargs)
        return Order(order_id=(instrument, res['child_order_acceptance_id']),
                     timestamp=0,
                     order_type=order_type,
                     instrument=instrument,
                     side=side,
                     price=price,
                     qty=qty,
                     qty_executed=.0,
                     state='UNKNOWN',
                     _data=res)

    def cancel_order(self, order_id: Hashable) -> Optional[Order]:
        assert isinstance(order_id, tuple)
        instrument, order_id = order_id
        product_code = self.instruments[instrument].name_id
        self.private_post('/cancelchildorder', product_code=product_code, child_order_acceptance_id=order_id)
        return None

    def update_order(self, order_id: Hashable, *, params: dict) -> Optional[Order]:
        raise NotSupportedError('not supported')

    def get_private_executions(self, instrument: str):
        limit = 500
        kwargs = {
            'product_code': self.instruments[instrument].name_id,
            'count': limit,
        }
        while True:
            res = self.private_get('/executions', **kwargs)
            for x in res:
                execution_id = x['id']
                timestamp = self._parse_time(x['exec_date'])
                execution = Execution(execution_id=execution_id,
                                      timestamp=timestamp,
                                      instrument=instrument,
                                      side=x['side'],
                                      price=x['price'],
                                      qty=x['size'],
                                      _data=x)
                yield execution
            if len(res) < limit:
                break
            kwargs.update(before=res[-1]['id'])

    def _convert_position(self, data: dict) -> Position:
        instrument = self.rinstruments[data['product_code']]
        return Position(position_id=None,
                        timestamp=self._parse_time(data['open_date']),
                        instrument=instrument,
                        side=data['side'],
                        state=PositionState.ACTIVE,
                        price_opened_average=data['price'],
                        qty_opened=data['size'],
                        _data=data)

    def get_positions(self, *, instrument: str = None, active_only: bool = True,
                      position_ids: Iterable[Hashable] = None) -> Iterator[Position]:
        assert active_only, 'active positions only'
        position_ids = set(position_ids or [])
        for data in self.private_get('/getpositions'):
            position = self._convert_position(data)
            if not instrument or instrument == position.instrument:
                if not position_ids or position.position_id in position_ids:
                    yield position

    def close_position(self, position_id: Hashable, *, qty: float = None) -> Optional[Position]:
        raise NotSupportedError('position managed by FIFO. submit reverse order')
