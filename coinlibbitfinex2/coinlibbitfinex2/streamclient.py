from collections import defaultdict, deque, OrderedDict
import copy
import logging
import threading
import time
from typing import Hashable, Tuple, Optional, DefaultDict, Dict, Deque, List, Any, Iterable, Iterator

import dataclasses
from dataclasses import dataclass

from coinlib.datatypes import OrderBook, Order, Position, Execution, OrderState, Balance, OrderSide, OrderType
from coinlib.datatypes.streamdata import StreamData, StreamType
from coinlib.errors import NotFoundError, CoinError, NotSupportedError
from coinlib.trade.streamclient import StreamClient as StreamClientBase
from coinlib.utils.funcs import no_none_dict
from .client import Client
from .streamapi import StreamApi

logger = logging.getLogger(__name__)


@dataclass
class Notification:
    timestamp: float
    notify_type: str
    notify_info: Any
    status: str  # SUCCESS, ERROR, FAILURE, ...
    _data: Any


@dataclass
class AccountInfo:
    balances: Dict[str, Dict[str, Balance]] = dataclasses.field(default_factory=lambda: defaultdict(dict))
    _balances_lock: threading.RLock = dataclasses.field(default_factory=threading.RLock, repr=False)
    orders: Dict[Hashable, Order] = dataclasses.field(default_factory=lambda: defaultdict(lambda: None))
    _orders_lock: threading.RLock = dataclasses.field(default_factory=threading.RLock, repr=False)
    positions: Dict[str, Position] = dataclasses.field(default_factory=lambda: defaultdict(lambda: None))
    _positions_lock: threading.RLock = dataclasses.field(default_factory=threading.RLock, repr=False)
    executions: Deque[Execution] = None
    _executions_lock: threading.RLock = dataclasses.field(default_factory=threading.RLock, repr=False)
    notifications: Deque[Notification] = None
    _notifications_lock: threading.RLock = dataclasses.field(default_factory=threading.RLock, repr=False)
    q_len: int = 5000
    ttl: float = 3600

    def __post_init__(self):
        self.executions = deque(maxlen=self.q_len)
        self.notifications = deque(maxlen=self.q_len)

    def get_balances_list(self) -> List[Balance]:
        balances = []
        for type_balances in self.get_balances().values():
            balances.extend(type_balances.values())
        return balances

    def get_balances(self) -> Dict[str, Dict[str, Balance]]:
        with self._balances_lock:
            return copy.deepcopy(self.balances)

    def update_balance(self, balance_type: str, balance: Balance):
        with self._balances_lock:
            self.balances[balance_type][balance.currency] = balance

    def get_orders(self) -> Dict[Hashable, Order]:
        with self._orders_lock:
            self.gc_orders()
            return self.orders.copy()

    def update_order(self, order: Order):
        with self._orders_lock:
            self.orders[order.order_id] = order

    def gc_orders(self):
        with self._orders_lock:
            now = time.time()
            for order_id, order in tuple(self.orders.items()):
                # noinspection PyProtectedMember
                mts_update = order._data['mts_update'] / 1000
                if order.state != OrderState.ACTIVE and (now - mts_update) > self.ttl:
                    self.orders.pop(order_id, None)

    def get_positions(self) -> Dict[str, Position]:
        with self._positions_lock:
            return self.positions.copy()

    def update_position(self, position: Position):
        with self._orders_lock:
            self.positions[position.instrument] = position

    def get_executions(self, *, key=lambda _: True) -> List[Execution]:
        with self._executions_lock:
            return list(filter(key, self.executions))

    def add_executions(self, execution: Execution):
        with self._executions_lock:
            self.executions.append(execution)

    def get_notifications(self, *, key=lambda _: True) -> List[Notification]:
        with self._notifications_lock:
            return list(filter(key, self.notifications))

    def add_notification(self, notification: Notification):
        with self._notifications_lock:
            self.notifications.append(notification)


class StreamClient(Client, StreamClientBase):
    STREAM_API_CLASS = StreamApi

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._channel_data_cache: DefaultDict[Hashable, dict] = defaultdict(dict)
        self._account_info: AccountInfo = AccountInfo()
        self._authentication_params = {}
        self._order_op_lock = threading.RLock()
        self._account_info_types = set()

    def wait_authentication(self, timeout: float = 30) -> bool:
        expired = time.time() + timeout
        while time.time() < expired:
            if self.is_authenticated():
                return True
        return False

    def is_authenticated(self) -> bool:
        return super().is_authenticated() and {'ws', 'os', 'ps'}.issubset(self._account_info_types)

    def on_open(self):
        with self._open_close_lock:
            self.send_message({
                'event': 'conf',
                'flags': 32768,  # timestamp
            })

    def request_subscribe(self, key: Tuple[str, Hashable]):
        self._channel_data_cache[key].clear()
        stream_type = key[0]
        if stream_type == StreamType.ORDER_BOOK:
            instrument: str = key[1]
            symbol = self.instruments[instrument].name_id
            params = {
                'event': 'subscribe',
                'channel': 'book',
                'symbol': symbol,
                'prec': 'P0',
                'freq': 'F0',
                'len': '25',
            }
            self.stream_api.subscribe((key, params))
            return

    def request_unsubscribe(self, key: Tuple[str, Hashable]):
        self.stream_api.unsubscribe(key)

    def convert_raw_data(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[str, Hashable] = data.key
        stream_type = key[0]
        if stream_type == StreamType.ORDER_BOOK:
            return self.convert_order_book(data)
        if stream_type == 'private_account':
            self.convert_account_info(data)
            return data

        return None

    def convert_order_book(self, data: StreamData) -> Optional[StreamData]:
        key: Tuple[str, Hashable] = data.key
        instrument: str = key[1]
        data: list = data.data
        assert isinstance(data, list), data
        body = data[1]
        if len(data) == 3:
            timestamp = data[2]
        else:
            timestamp = time.time()
        cache: dict = self._channel_data_cache[key]
        if body[0] != 'hb':
            if isinstance(body[0], list):
                # snapshot
                cache['snapshot'] = True
            else:
                # update
                body = [body]
            prices = cache.setdefault('prices', {})
            for price, count, amount in body:
                prices[price] = (amount, count)
                if count == 0:
                    cache.pop(price, None)
        if not cache.get('snapshot'):
            return
        asks = []
        bids = []
        for price, (amount, count) in cache['prices'].items():
            if amount > 0:
                asks.append((float(price), abs(float(amount)), int(count)))
            elif amount < 0:
                bids.append((float(price), abs(float(amount)), int(count)))
        order_book = OrderBook(timestamp=timestamp, instrument=instrument, asks=asks, bids=bids, _data=None)
        return StreamData(key, order_book)

    # private

    def convert_account_info(self, data: StreamData) -> Optional[StreamData]:
        orig_data = data
        try:
            _ = orig_data
            data: list = data.data
            if not isinstance(data, list) or not data:
                return
            message_type = data[1]
            if message_type == 'hb' or not data[2]:
                return

            if isinstance(data[2][0], list):
                values = data[2]
            else:
                values = [data[2]]
            self._account_info_types.add(message_type)
            if message_type in ('ws', 'wu',):
                if message_type == 'ws':
                    # noinspection PyProtectedMember
                    with self._account_info._balances_lock:
                        self._account_info.balances.clear()
                for v in values:
                    logger.debug(f'{message_type} {v}')
                    balance = self._convert_balance(v)
                    # noinspection PyProtectedMember
                    wallet_type = balance._data['wallet_type']
                    assert wallet_type in self._balance_type_map, data
                    balance_type = self._balance_type_map[wallet_type]
                    self._account_info.update_balance(balance_type, balance)
                return
            if message_type in ('os', 'on', 'ou', 'oc',):
                if message_type == 'os':
                    # noinspection PyProtectedMember
                    with self._account_info._orders_lock:
                        self._account_info.orders.clear()
                for v in values:
                    logger.debug(f'{message_type} {v}')
                    order = self._convert_order(v)
                    self._account_info.update_order(order)
                return
            if message_type in ('ps', 'pn', 'pu', 'pc',):
                if message_type == 'ps':
                    # noinspection PyProtectedMember
                    with self._account_info._positions_lock:
                        self._account_info.positions.clear()
                for v in values:
                    logger.debug(f'{message_type} {v}')
                    self._account_info.update_position(self._convert_position(v))
                return
            if message_type in ('te', 'tu',):
                for v in values:
                    logger.debug(f'{message_type} {v}')
                    self._account_info.add_executions(self._convert_private_execution(v))
                return
            if message_type in ('n',):
                for v in values:
                    logger.debug(f'{message_type} {v}')
                    self._account_info.add_notification(self._convert_notification(v))
                return
            logger.warning(f'unsupported message {data}')
        except Exception as e:
            logger.exception(e)
            raise

    def _convert_notification(self, data: list):
        keys = [
            'mts', 'type', 'message_id', '_0',
            'notify_info', 'code', 'status', 'text',
        ]
        data = OrderedDict(zip(keys, data), _extra=data[len(keys):])
        notify_info = data['notify_info']
        return Notification(timestamp=data['mts'] / 1000,
                            notify_type=data['type'],
                            notify_info=notify_info,
                            status=data['status'],
                            _data=data)

    _order_type_map = {
        # (type, margin)
        (OrderType.MARKET, False): 'EXCHANGE MARKET',
        (OrderType.LIMIT, False): 'EXCHANGE LIMIT',
        (OrderType.MARKET, True): 'MARKET',
        (OrderType.LIMIT, True): 'LIMIT',
    }

    def get_balances_list(self) -> List[Balance]:
        return self._account_info.get_balances_list()

    def get_orders(self, *, instrument: str = None, active_only: bool = True,
                   order_ids: Iterable[Hashable] = None) -> Iterator[Order]:
        if order_ids:
            raise NotSupportedError('order_ids not supported')
        if not active_only:
            raise NotSupportedError('active orders only')
        for order in self._account_info.get_orders().values():
            if not instrument or order.instrument == instrument:
                if order.state == OrderState.ACTIVE:
                    yield order

    def submit_order(self, instrument: str, order_type: str, side: str, price: Optional[float], qty: float,
                     *, margin: bool = False, leverage: float = None, params: dict = None,
                     gid: int = None, flags: int = None,
                     timeout: float = 30, async: bool = False) -> Order:
        order_type = self._order_type_map.get((order_type, margin), order_type)
        if margin:
            assert 'EXCHANGE' not in order_type, f'margin={margin}, but spot order'
        kwargs = dict(instrument=instrument, order_type=order_type, side=side,
                      price=price, qty=qty, gid=gid, flags=flags)
        kwargs.update(params or {})
        order_op = self._new_order_op(**kwargs)
        return self._submit_order_op(order_op, timeout=timeout, async=async)

    def cancel_order(self, order_id: Hashable, *, params: dict = None,
                     timeout: float = 30, async: bool = False) -> Order:
        order_op = self._cancel_order_op(order_id)
        return self._submit_order_op(order_op, timeout=timeout, async=async)

    def cancel_order_group(self, group_id: int, *, timeout: float = 30, async: bool = False):
        order_op = self._cancel_order_group_op(group_id)
        return self._submit_order_op(order_op, timeout=timeout, async=async)

    def update_order(self, order_id: Hashable, *, params: dict = None, timeout: float = 30, async: bool = False,
                     gid: int = None, price: float = None,
                     qty: float = None, qty_delta: float = None, flags: int = None) -> Order:
        kwargs = no_none_dict(gid=gid, price=price, qty=qty, qty_delta=qty_delta, flags=flags)
        kwargs.update(params or {})
        order_op = self._update_order_op(order_id, **kwargs)
        return self._submit_order_op(order_op, timeout=timeout, async=async)

    def get_positions(self, *, instrument: str = None, active_only: bool = True,
                      position_ids: Iterable[Hashable] = None) -> Iterator[Position]:
        if position_ids:
            raise NotSupportedError('position_ids not supported')
        if not active_only:
            raise NotSupportedError('active orders only')
        for position in self._account_info.get_positions().values():
            if not instrument or position.instrument == instrument:
                yield position

    def close_position(self, position_id: Hashable, *, qty: float = None) -> Optional[Position]:
        raise NotSupportedError('not supported')

    _cid_lock = threading.RLock()
    _cid = 0

    @classmethod
    def _get_cid(cls) -> int:
        with cls._cid_lock:
            cid = int(time.time() * 1000)
            if cid <= cls._cid:
                cid = cls._cid + 1
            cls._cid = cid
            return cid

    def _new_order_op(self, *, instrument: str, order_type: str, side: str, price: float = None, qty: float,
                      gid: int = None, flags: int = None):
        cid = self._get_cid()
        symbol = self.instruments[instrument].name_id
        side = side.upper()
        if side == 'BUY':
            amount = qty
        else:
            assert side == 'SELL'
            amount = -qty
        if price:
            price = str(price)
        params = no_none_dict(cid=cid, gid=gid, symbol=symbol, type=order_type,
                              amount=str(amount), price=price, flags=flags)
        return 'on', params

    def _update_order_op(self, order_id: Hashable, *,
                         gid: int = None, price: float = None,
                         qty: float = None, qty_delta: float = None, flags: int = None,
                         **kwargs):
        orders = self._account_info.get_orders()
        order = orders[order_id]
        if not order:
            raise NotFoundError(f'{order_id} order not found')
        if price:
            price = str(price)
        if qty:
            if order.side == OrderSide.SELL:
                qty = -qty
            qty = str(qty)
        if qty_delta:
            if order.side == OrderSide.SELL:
                qty_delta = -qty_delta
            qty_delta = str(qty_delta)
        params = no_none_dict(id=order_id, gid=gid, price=price,
                              amount=qty, delta=qty_delta,
                              flags=flags)
        return 'ou', params

    def _cancel_order_op(self, order_id: Hashable):
        _ = self
        return 'oc', {'id': order_id}

    def _cancel_order_group_op(self, gid: int):
        _ = self
        return 'oc_multi', {'gid': [[gid]]}

    def _submit_order_op(self, order_op: Tuple[str, dict],
                         *, timeout: float = 60, async: bool = False) -> Optional[Order]:
        with self._order_op_lock:
            op, params = order_op
            message = [
                0, op, None, params,
            ]
            timestamp = time.time()
            self.send_message(message)
            if async:
                return
            return self._wait_order_response(op, params, timestamp, timeout)

    def _wait_order_response(self, op: str, params, timestamp: float, timeout: float) -> Optional[Order]:
        expired = timestamp + timeout
        target_order_id: Hashable = None
        while time.time() < expired:
            # TODO: more efficient
            if target_order_id:
                if op == 'oc_multi':
                    return
                assert op in ('on', 'ou', 'oc',)
                orders = self._account_info.get_orders()
                order = orders[target_order_id]
                if order and timestamp < order.timestamp_update:
                    return order
            else:
                notifications = self._account_info.get_notifications(key=lambda x: timestamp < x.timestamp)
                for notification in notifications:
                    if notification.notify_type == f'{op}-req':
                        if op == 'oc_multi':
                            if notification.status != 'INFO':
                                # noinspection PyProtectedMember
                                text = notification._data['text']
                                raise CoinError(dict(message=f'order_op error. {text}'))
                            return
                        elif op == 'on':
                            cid = params['cid']
                            if notification.notify_info[2] == cid:
                                if notification.status != 'SUCCESS':
                                    # noinspection PyProtectedMember
                                    text = notification._data['text']
                                    raise CoinError(dict(message=f'order_op error. {text}'))
                                target_order_id = notification.notify_info[0]
                                break
                        elif op in ('oc', 'ou',):
                            _id = params['id']
                            if notification.notify_info[0] == _id:
                                if notification.status != 'SUCCESS':
                                    # noinspection PyProtectedMember
                                    text = notification._data['text']
                                    raise CoinError(dict(message=f'order_op error. {text}'))
                                target_order_id = notification.notify_info[0]
                                break
            time.sleep(0.2)
        raise CoinError('timeout')

    def send_message(self, message: Any):
        stream_api: StreamApi = self.stream_api
        stream_api.send_message(message)
