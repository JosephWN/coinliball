import logging
import time
from typing import Hashable, Dict, Iterable, Optional, Iterator

from requests.structures import CaseInsensitiveDict

from coinlib.datatypes import Instrument, Ticker, OrderBook, Execution, OrderSide, Balance, Order, OrderState, OrderType
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.position import Position, PositionState
from coinlib.trade.client import Client as ClientBase
from coinlib.utils.decorators import dedup
from coinlib.utils.funcs import no_none_dict

logger = logging.getLogger(__name__)


class Client(ClientBase):
    REST_API_CLASS = (lambda *_: None)

    def __init__(self, credential: dict = None):
        super().__init__(credential)

    def get_instruments(self):
        instruments = {}
        for product in self.public_get('/products'):
            base_id, quote_id = product['base_currency'], product['quoted_currency']
            base, quote = base_id, quote_id
            if base not in self.CURRENCIES or quote not in self.CURRENCIES:
                continue
            name = f'{base}_{quote}'
            instruments[name] = Instrument(name=name, base=base, quote=quote,
                                           name_id=str(product['id']),
                                           base_id=base_id, quote_id=quote_id,
                                           _data=product)
        return instruments

    def _convert_ticker(self, timestamp: float, instrument: str, data: dict) -> Ticker:
        return Ticker(timestamp=timestamp, instrument=instrument,
                      ask=float(data['market_ask']), bid=float(data['market_bid']),
                      last=float(data['last_traded_price']),
                      volume_24h=float(data['volume_24h']),
                      _data=data)

    def get_ticker(self, instrument: str) -> Ticker:
        timestamp = time.time()
        product_id = self.instruments[instrument].name_id
        res = self.public_get(f'/products/{product_id}')
        return self._convert_ticker(timestamp, instrument, res)

    def get_order_book(self, instrument: str, *_, _full: bool = False) -> OrderBook:
        product_id = self.instruments[instrument].name_id
        timestamp = time.time()
        kwargs = {}
        if _full:
            kwargs['full'] = 1
        res = self.public_get(f'/products/{product_id}/price_levels', **kwargs)
        asks_bids = {}
        for k, k2 in [('asks', 'sell_price_levels'), ('bids', 'buy_price_levels')]:
            asks_bids[k] = [(float(price), float(qty), None) for price, qty in res[k2]]
        return OrderBook(timestamp=timestamp, instrument=instrument, **asks_bids, _data=res)

    @dedup(lambda x: x.execution_id)
    def get_public_executions(self, instrument: str) -> Iterator[Execution]:
        product_id = self.instruments[instrument].name_id
        limit = 1000
        page = 1
        kwargs = dict(product_id=product_id, limit=limit)
        while True:
            kwargs.update(page=page)
            page += 1
            res = self.public_get('/executions', **kwargs)
            for x in res['models']:
                yield Execution(execution_id=x['id'],
                                timestamp=float(x['created_at']),
                                instrument=instrument,
                                side=OrderSide.validate(x['taker_side']),
                                price=float(x['price']),
                                qty=float(x['quantity']),
                                _data=x)
            if len(res) < limit:
                break

    def get_balances(self) -> Dict[str, Dict[str, Balance]]:
        balances: dict = CaseInsensitiveDict()
        for data in self.private_get('/accounts/balance'):
            currency = data['currency']
            balances[currency] = Balance(currency=currency,
                                         total=float(data['balance']),
                                         _data=data)
        return {
            BalanceType.MAIN: balances,
        }

    def _convert_order(self, order: dict) -> Order:
        iceberg_qty = float(order['iceberg_total_quantity'])
        if iceberg_qty:
            qty = iceberg_qty
            qty_displayed = float(order['disc_quantity'])
        else:
            qty = float(order['quantity'])
            qty_displayed = None
        side = order['side']
        status = order['status'].lower()  # type: str
        if status == 'live':
            state = OrderState.ACTIVE
        elif status == 'cancelled':
            state = OrderState.CANCELED
        elif status == 'filled':
            state = OrderState.FILLED
        else:
            logger.warning(f'unknown state {order}')
            state = OrderState.UNKNOWN
        return Order(order_id=order['id'],
                     timestamp=float(order['created_at']),
                     timestamp_update=float(order['updated_at']),
                     order_type=order['order_type'],
                     instrument=self.rinstruments[str(order['product_id'])],
                     side=side,
                     price=float(order['price']),
                     price_executed_average=float(order['average_price']) or None,
                     qty=qty,
                     qty_executed=float(order['filled_quantity']),
                     is_iceberg=bool(iceberg_qty),
                     qty_displayed=qty_displayed,
                     state=state,
                     _data=order)

    @dedup(lambda x: x.order_id)
    def get_orders(self, *, instrument: str = None, active_only: bool = True,
                   order_ids: Iterable[Hashable] = None, _with_details: bool = False) -> Iterator[Order]:
        if order_ids:
            for order_id in order_ids:
                res = self.private_get(f'/orders/{order_id}')
                yield self._convert_order(res)
        else:
            limit = 100
            kwargs = dict(limit=limit)
            if active_only:
                kwargs['status'] = 'live'
            if instrument:
                kwargs['product_id'] = self.instruments[instrument].name_id
            if _with_details:
                kwargs['with_details'] = 1
            page = 1
            while True:
                kwargs.update(page=page)
                page += 1
                res = self.private_get('/orders', **kwargs)
                for x in res['models']:
                    order = self._convert_order(x)
                    yield order
                if len(res) < limit:
                    break

    _order_type_map = {
        OrderType.MARKET: 'market',
        OrderType.LIMIT: 'limit',
    }
    _leverage_levels = (2, 4, 5, 10, 25)

    def submit_order(self, instrument: str, order_type: str, side: str, price: float, qty: float,
                     *, margin: bool = False, leverage: float = None, params: dict = None) -> Order:
        product_id = self.instruments[instrument].name_id
        order_type = self._order_type_map.get(order_type, order_type)
        kwargs = dict(order_type=order_type.lower(),
                      product_id=product_id,
                      side=side.lower(),
                      price=price,
                      quantity=qty)
        if margin:
            assert leverage in self._leverage_levels, f'leverage={leverage} not in {self._leverage_levels}'
            kwargs['leverage_level'] = leverage
        kwargs.update(params or {})
        res = self.private_post('/orders', **kwargs)
        return self._convert_order(res)

    def cancel_order(self, order_id: Hashable) -> Optional[Order]:
        res = self.private_put(f'/orders/{order_id}/cancel')
        return self._convert_order(res)

    def update_order(self, order_id: Hashable, *, params: dict) -> Optional[Order]:
        res = self.private_put(f'/orders/{order_id}', **params)
        return self._convert_order(res)

    @dedup(lambda x: x.execution_id)
    def get_private_executions(self, instrument: str) -> Iterator[Execution]:
        product_id = self.instruments[instrument].name_id
        limit = 1000
        page = 1
        kwargs = dict(product_id=product_id, limit=limit)
        while True:
            kwargs.update(page=page)
            page += 1
            res = self.private_get('/executions/me', **kwargs)
            for x in res['models']:
                yield Execution(execution_id=x['id'],
                                timestamp=float(x['created_at']),
                                instrument=instrument,
                                side=OrderSide.validate(x['my_side']),
                                price=float(x['price']),
                                qty=float(x['quantity']),
                                _data=x)
            if len(res) < limit:
                break

    _position_state_map = {
        'open': PositionState.ACTIVE,
        'closed': PositionState.CLOSED,
    }

    def _convert_position(self, data: dict) -> Position:
        state = self._position_state_map.get(data['status'], PositionState.UNKNOWN)
        instrument = self.rinstruments[str(data['product_id'])]
        closed_at = None
        if state == PositionState.CLOSED:
            closed_at = float(data['updated_at'])
        return Position(position_id=data['id'],
                        timestamp=float(data['created_at']),
                        instrument=instrument,
                        side='SELL' if data['side'] == 'short' else 'BUY',
                        state=state,
                        price_opened_average=data['open_price'],
                        price_closed_average=data['close_price'],
                        qty_opened=data['open_quantity'],
                        qty_closed=data['close_quantity'],
                        limit=float(data['take_profit']) or None,
                        stop=float(data['stop_loss']) or None,
                        timestamp_closed=closed_at,
                        _data=data)

    @dedup(lambda x: x.position_id)
    def get_positions(self, *, instrument: str = None, active_only: bool = True,
                      position_ids: Iterable[Hashable] = None, _params: dict = None) -> Iterator[Position]:
        limit = 100
        kwargs = dict(limit=100)
        if position_ids:
            position_ids = set(position_ids or [])
        else:
            if instrument:
                kwargs.update(product_id=self.instruments[instrument].name_id)
            if active_only:
                kwargs.update(status='open')
        kwargs.update(_params or {})
        page = 1
        while True:
            kwargs.update(page=page)
            page += 1
            res = self.private_get('/trades', **kwargs)
            for x in res['models']:
                position = self._convert_position(x)
                if position_ids:
                    if position.position_id in position_ids:
                        yield position
                        position_ids.remove(position.position_id)
                        if not position_ids:
                            break
                else:
                    if not instrument or instrument == position.instrument:
                        yield position
            if len(res) < limit:
                break

    def close_position(self, position_id: Hashable, *, qty: float = None) -> Optional[Position]:
        kwargs = no_none_dict(closed_quantity=qty)
        res = self.private_put(f'/trades/{position_id}/close', **kwargs)
        return self._convert_position(res)

    def update_position(self, position_id: Hashable, limit: float = None, stop: float = None) -> Position:
        kwargs = no_none_dict(take_profit=limit, stop_loss=stop)
        res = self.private_put(f'/trades/{position_id}', **kwargs)
        return self._convert_position(res)
