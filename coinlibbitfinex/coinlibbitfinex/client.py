import logging
import time
from typing import Hashable, Iterable, Optional, Iterator, List

from coinlib.datatypes import Instrument, OrderBook, Balance, Execution, Ticker
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.order import OrderState, Order, OrderType
from coinlib.datatypes.position import Position, PositionState
from coinlib.errors import NotSupportedError, NotFoundError
from coinlib.trade.client import Client as ClientBase
from coinlib.utils.decorators import dedup
from .restapi import RestApi

logger = logging.getLogger(__name__)


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
        for info in self.public_get('/symbols_details'):
            pair = info['pair'].upper()
            base_id, quote_id = pair[:-3], pair[-3:]
            base, quote = base_id.upper(), quote_id.upper()
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            name = f'{base}_{quote}'
            instrument_map[name] = Instrument(name=name, base=base, quote=quote,
                                              name_id=pair, base_id=base_id, quote_id=quote_id, _data=info)
        return instrument_map

    # public methods

    def get_ticker(self, instrument: str) -> Ticker:
        symbol = self.instruments[instrument].name_id
        res = self.public_get(f'/pubticker/{symbol}')
        return Ticker(timestamp=float(res['timestamp']), instrument=instrument,
                      ask=float(res['ask']), bid=float(res['bid']), last=float(res['last_price']),
                      volume_24h=float(res['volume']), _data=res)

    def get_order_book(self, instrument: str) -> OrderBook:
        symbol = self.instruments[instrument].name_id
        res = self.public_get(f'/book/{symbol}', limit_asks=50, limit_bids=50)
        asks_bids = {}
        timestamp = .0
        for k in ['asks', 'bids']:
            values = []
            for v in res[k]:
                values.append((float(v['price']), float(v['amount']), None))
                timestamp = max([timestamp, float(v['timestamp'])])
            asks_bids[k] = values
        return OrderBook(timestamp=timestamp, instrument=instrument, **asks_bids, _data=res)

    def get_public_executions(self, instrument: str, **kwargs) -> Iterator[Execution]:
        raise NotSupportedError('currently not supported')

    # private methods

    def get_balances_list(self) -> List[Balance]:
        balances = []
        type_map = {
            'exchange': BalanceType.MAIN,
            'trading': BalanceType.MARGIN,
            'deposit': BalanceType.LOAN,
        }
        for data in self.private_post('/balances'):
            assert data['type'] in type_map, data
            balance_type = type_map[data['type']]
            currency = self.rcurrencies[data['currency']]
            balance = Balance(balance_type=balance_type,
                              currency=currency,
                              total=float(data['amount']),
                              locked=float(data['amount']) - float(data['available']),
                              _data=data)
            balances.append(balance)
        return balances

    def _convert_order(self, order: dict) -> Order:
        instrument = self.rinstruments[order['symbol']]
        side = order['side']
        if order['is_live']:
            state = OrderState.ACTIVE
        elif order['is_cancelled']:
            state = OrderState.CANCELED
        elif float(order['remaining_amount']) == 0:
            state = OrderState.FILLED
        else:
            logger.warning(f'unknown state {order}')
            state = OrderState.UNKNOWN
        return Order(order_id=order['id'],
                     timestamp=float(order['timestamp']),
                     order_type=order['type'],
                     instrument=instrument,
                     side=side,
                     price=float(order['price']),
                     price_executed_average=float(order['avg_execution_price']) or None,
                     qty=float(order['original_amount']),
                     qty_executed=float(order['executed_amount']),
                     is_hidden=order['is_hidden'],
                     state=state,
                     _data=order)

    def get_orders(self, *, instrument: str = None, active_only: bool = True,
                   order_ids: Iterable[Hashable] = None) -> Iterator[Order]:
        if order_ids:
            order_ids = set(order_ids or [])
            for order_id in order_ids:
                try:
                    order = self.private_post('/order/status', order_id=order_id)
                    yield self._convert_order(order)
                except NotFoundError:
                    pass
        else:
            assert active_only, 'require active_only'
            res = self.private_post('/orders')
            for order in res:
                order = self._convert_order(order)
                if not instrument or instrument == order.instrument:
                    yield order

    _order_type_map = {
        # (type, margin)
        (OrderType.MARKET, False): 'exchange market',
        (OrderType.LIMIT, False): 'exchange limit',
        (OrderType.MARKET, True): 'market',
        (OrderType.LIMIT, True): 'limit',
    }

    def submit_order(self, instrument: str, order_type: str, side: str, price: float, qty: float,
                     *, margin: bool = False, leverage: float = None, params: dict = None) -> Order:
        kwargs = {}
        symbol = self.instruments[instrument].name_id
        order_type = self._order_type_map.get((order_type, margin), order_type)
        if margin:
            assert 'exchange' not in order_type, f'margin={margin}, but spot order'
        kwargs.update(symbol=symbol, side=side.lower(), type=order_type, price=str(price), amount=str(qty))
        kwargs.update(params or {})
        res = self.private_post('/order/new', **kwargs)
        return self._convert_order(res)

    def cancel_order(self, order_id: Hashable) -> Order:
        """WARNING: server return ACTIVE state"""
        res = self.private_post('/order/cancel', order_id=order_id)
        return self._convert_order(res)

    def update_order(self, order_id: Hashable, *, params: dict) -> Order:
        raise NotSupportedError('not supported')

    @dedup(lambda x: x.execution_id)
    def get_private_executions(self, instrument: str) -> Iterator[Execution]:
        symbol = self.instruments[instrument].name_id
        kwargs = dict(symbol=symbol, limit_trades=500)
        until = str(time.time())
        last_id = None
        while True:
            kwargs.update(until=until)
            res = self.private_post('/mytrades', **kwargs)
            for x in res:
                execution_id = x['tid']
                timestamp = float(x['timestamp'])
                execution = Execution(execution_id=execution_id,
                                      timestamp=timestamp,
                                      instrument=instrument,
                                      side=x['type'],
                                      price=float(x['price']),
                                      qty=float(x['amount']),
                                      _data=x)
                yield execution
            if len(res) < kwargs['limit_trades']:
                break
            assert last_id != res[-1]['tid']
            last_id = res[-1]['tid']
            until = res[-1]['timestamp']

    def _convert_position(self, data: dict) -> Position:
        instrument = self.rinstruments[data['symbol']]
        if data['status'] == 'ACTIVE':
            state = PositionState.ACTIVE
        else:
            state = PositionState.UNKNOWN
        price = float(data['base'])
        qty = float(data['amount'])
        side = 'BUY' if qty > 0 else 'SELL'
        qty = abs(qty)
        return Position(position_id=data['id'], timestamp=float(data['timestamp']),
                        instrument=instrument, side=side, state=state,
                        price_opened_average=price,
                        qty_opened=qty, _data=data)

    def get_positions(self, *, instrument: str = None, active_only: bool = True,
                      position_ids: Iterable[Hashable] = None) -> Iterator[Position]:
        assert active_only, 'active positions only'
        position_ids = set(position_ids or [])
        for data in self.private_post('/positions'):
            position = self._convert_position(data)
            if not instrument or instrument == position.instrument:
                if not position_ids or position.position_id in position_ids:
                    yield position

    def close_position(self, position_id: Hashable, *, qty: float = None) -> Optional[Position]:
        raise NotSupportedError('position managed by FIFO. submit reverse order')
