from collections import defaultdict
import logging
from typing import Hashable, Dict, Iterable, Optional, Iterator, List

from coinlib.datatypes import Instrument, OrderBook, Balance, Execution, Ticker
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.order import OrderState, Order, OrderType
from coinlib.datatypes.position import Position
from coinlib.errors import NotSupportedError
from coinlib.trade.client import Client as ClientBase
from coinlib.utils.funcs import no_none_dict
from .restapi import RestApi

logger = logging.getLogger(__name__)


class Client(ClientBase):
    PAIRS = (
        'btc_jpy', 'xrp_jpy', 'ltc_btc', 'eth_btc',
        'mona_jpy', 'mona_btc', 'bcc_jpy', 'bcc_btc',
    )
    CURRENCY_MAP = {
        'BCC': 'BCH',
    }
    REST_API_CLASS = RestApi

    def get_instruments(self) -> Dict[str, Instrument]:
        instrument_map = {}
        for pair in self.PAIRS:
            base_id, quote_id = pair.split('_')
            base, quote = pair.upper().split('_')
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            name = f'{base}_{quote}'
            instrument_map[name] = Instrument(name=name, base=base, quote=quote,
                                              name_id=pair, base_id=base_id, quote_id=quote_id,
                                              _data=pair)
        return instrument_map

    # public methods

    def _convert_ticker(self, instrument: str, data: dict) -> Ticker:
        _ = self
        return Ticker(timestamp=data['timestamp'] / 1000, instrument=instrument,
                      ask=float(data['sell']), bid=float(data['buy']), last=float(data['last']),
                      volume_24h=float(data['vol']), _data=data)

    def get_ticker(self, instrument: str, *, params: dict = None) -> Ticker:
        pair = self.instruments[instrument].name_id
        res = self.public_get(f'/{pair}/ticker')
        return self._convert_ticker(instrument, res)

    def _convert_order_book(self, instrument: str, data: dict) -> OrderBook:
        _ = self
        asks_bids: Dict[str, list] = {}
        for k in ['asks', 'bids']:
            asks_bids[k] = [(float(price), float(qty), None) for price, qty in data[k]]
        return OrderBook(timestamp=data['timestamp'] / 1000, instrument=instrument, **asks_bids, _data=data)

    def get_order_book(self, instrument: str, *, params: dict = None) -> OrderBook:
        pair = self.instruments[instrument].name_id
        res = self.public_get(f'/{pair}/depth')
        return self._convert_order_book(instrument, res)

    def get_public_executions(self, instrument: str, *, params: dict = None) -> Iterator[Execution]:
        raise NotSupportedError('not implemented yet')

    # private methods

    def get_balances_list(self, *, params: dict = None) -> List[Balance]:
        kwargs = params or {}
        res = self.private_get('/user/assets', **kwargs)
        balances = []
        for asset in res['assets']:
            currency = self.rcurrencies[asset['asset']]
            balance = Balance(balance_type=BalanceType.MAIN,
                              currency=currency,
                              total=float(asset['onhand_amount']),
                              locked=float(asset['locked_amount']),
                              _data=asset)
            balances.append(balance)
        return balances

    _state_map = {
        OrderState.ACTIVE: {'UNFILLED', 'PARTIALLY_FILLED'},
        OrderState.FILLED: {'FULLY_FILLED'},
        OrderState.CANCELED: {'CANCELED_UNFILLED', 'CANCELED_PARTIALLY_FILLED'}
    }

    def _convert_order(self, order: dict) -> Order:
        instrument = self.rinstruments[order['pair']]
        side = order['side']
        status = order['status'].upper()
        state = None
        for k, v in self._state_map.items():
            if status in v:
                state = k
                break
        assert state, state
        return Order(order_id=(instrument, order['order_id']),
                     timestamp=order['ordered_at'] / 1000,
                     order_type=order['type'],
                     instrument=instrument,
                     side=side,
                     price=float(order.get('price', None)),
                     price_executed_average=float(order['average_price']) or None,
                     qty=float(order['start_amount']),
                     qty_executed=float(order['executed_amount']),
                     state=state,
                     _data=order)

    def get_orders(self, *, instrument: str = None, active_only: bool = True,
                   order_ids: Iterable[Hashable] = None, params: dict = None) -> Iterator[Order]:
        params = params or {}
        if order_ids:
            instrument_order_ids = defaultdict(list)
            for instrument, order_id in order_ids:
                instrument_order_ids[instrument].append(order_id)
            for instrument, order_ids in instrument_order_ids.items():
                kwargs = {
                    'pair': self.instruments[instrument].name_id,
                    'order_ids': order_ids,
                }
                kwargs.update(params)
                res = self.private_post('/user/spot/orders_info', **kwargs)
                for order in res['orders']:
                    yield self._convert_order(order)
        else:
            assert instrument and active_only, 'require instrument and active_only'
            kwargs = {
                'pair': self.instruments[instrument].name_id,
            }
            kwargs.update(params)
            res = self.private_get('/user/spot/active_orders', **kwargs)
            for order in res['orders']:
                yield self._convert_order(order)

    _order_type_map = {
        OrderType.MARKET: 'market',
        OrderType.LIMIT: 'limit',
    }

    def submit_order(self, instrument: str, order_type: str, side: str, price: Optional[float], qty: float,
                     *, margin: bool = False, leverage: float = None, params: dict = None) -> Order:
        if margin:
            raise NotSupportedError('margin trade not supported')
        pair = self.instruments[instrument].name_id
        order_type = self._order_type_map.get(order_type, order_type)
        side = side.lower()
        kwargs = no_none_dict(pair=pair, side=side, type=order_type, price=price, amount=qty)
        kwargs.update(params or {})
        res = self.private_post('/user/spot/order', **kwargs)
        return self._convert_order(res)

    def cancel_order(self, order_id: Hashable, *, params: dict = None) -> Optional[Order]:
        assert isinstance(order_id, tuple)
        instrument, order_id = order_id
        pair = self.instruments[instrument].name_id
        res = self.private_post('/user/spot/cancel_order', pair=pair, order_id=order_id)
        return self._convert_order(res)

    def update_order(self, order_id: Hashable, *, params: dict = None) -> Optional[Order]:
        raise NotSupportedError('update_order is not supported')

    def get_private_executions(self, instrument: str, *, params: dict = None) -> Iterable[Execution]:
        raise NotSupportedError('currently server restrict for performance reason')

    def get_positions(self, *, instrument: str = None, active_only: bool = True,
                      position_ids: Iterable[Hashable] = None, params: dict = None) -> Iterator[Position]:
        raise NotSupportedError('margin trade not supported')

    def close_position(self, position_id: Hashable, *, qty: float = None, params: dict = None) -> Optional[Position]:
        raise NotSupportedError('margin trade not supported')
