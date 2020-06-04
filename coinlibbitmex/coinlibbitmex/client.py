import json
import logging
import time
from typing import Hashable, Optional, Iterable, Iterator, List

import dateutil.parser
import pytz
from requests.structures import CaseInsensitiveDict

from coinlib.datatypes import Instrument, OrderBook, Execution, Order, Ticker, Balance, OrderState, OrderType
from coinlib.datatypes.balance import BalanceType
from coinlib.datatypes.position import Position
from coinlib.errors import NotSupportedError
from coinlib.trade.client import Client as ClientBase
from coinlib.utils.decorators import dedup
from coinlib.utils.funcs import no_none_dict
from .restapi import RestApi

logger = logging.getLogger(__name__)


class Client(ClientBase):
    CURRENCY_MAP = CaseInsensitiveDict({
        'XBT': 'BTC',
    })
    REST_API_CLASS = RestApi

    def get_instruments(self):
        instruments = {}
        for x in self.public_get('/instrument/active'):
            symbol = x['symbol']
            base_id = x['underlying']
            quote_id = x['quoteCurrency']
            base = self.CURRENCY_MAP.get(base_id, base_id)
            quote = self.CURRENCY_MAP.get(quote_id, quote_id)
            instruments[symbol] = Instrument(name=symbol, base=base, quote=quote,
                                             name_id=symbol, base_id=base_id, quote_id=quote_id,
                                             _data=x)
        return instruments

    def get_ticker(self, instrument: str) -> Ticker:
        symbol = self.instruments[instrument].name_id
        res = self.public_get('/instrument', symbol=symbol)
        x = res[0]
        return Ticker(timestamp=self._parse_time(x['timestamp']), instrument=instrument,
                      ask=float(x['askPrice']), bid=float(x['bidPrice']), last=float(x['lastPrice']),
                      volume_24h=float(x['volume24h']), _data=res)

    def _parse_time(self, time_str: str) -> float:
        dt = dateutil.parser.parse(time_str)
        if not dt.tzinfo:
            dt = pytz.UTC.localize(dt)
        return dt.timestamp()

    def get_order_book(self, instrument: str, *_, _depth: int = None) -> OrderBook:
        symbol = self.instruments[instrument].name_id
        timestamp = time.time()
        kwargs = no_none_dict(symbol=symbol, depth=_depth)
        res = self.public_get('/orderBook/L2', **kwargs)
        asks = []
        bids = []
        for x in res:
            if x['side'].upper() == 'BUY':
                bids.append((float(x['price']), float(x['size']), x['id']))
            else:
                asks.append((float(x['price']), float(x['size']), x['id']))
        return OrderBook(timestamp=timestamp, instrument=instrument, asks=asks, bids=bids, _data=res)

    def get_public_executions(self, instrument: str, **kwargs) -> Iterator[Execution]:
        pass

    def get_balances_list(self) -> List[Balance]:
        balances = []
        res = self.private_get('/user/margin')
        assert res['currency'] == 'XBt', res
        total = res['marginBalance'] / 1e8
        locked = (res['marginBalance'] - res['availableMargin']) / 1e8
        currency = self.rcurrencies[res['currency']]
        balance = Balance(balance_type=BalanceType.MARGIN, currency=currency, total=total, locked=locked, _data=res)
        balances.append(balance)
        return balances

    def _convert_order(self, data: dict) -> Order:
        #   {
        #     "orderID": "string",
        #     "clOrdID": "string",
        #     "clOrdLinkID": "string",
        #     "account": 0,
        #     "symbol": "string",
        #     "side": "string",
        #     "simpleOrderQty": 0,
        #     "orderQty": 0,
        #     "price": 0,
        #     "displayQty": 0,
        #     "stopPx": 0,
        #     "pegOffsetValue": 0,
        #     "pegPriceType": "string",
        #     "currency": "string",
        #     "settlCurrency": "string",
        #     "ordType": "string",
        #     "timeInForce": "string",
        #     "execInst": "string",
        #     "contingencyType": "string",
        #     "exDestination": "string",
        #     "ordStatus": "string",
        #     "triggered": "string",
        #     "workingIndicator": true,
        #     "ordRejReason": "string",
        #     "simpleLeavesQty": 0,
        #     "leavesQty": 0,
        #     "simpleCumQty": 0,
        #     "cumQty": 0,
        #     "avgPx": 0,
        #     "multiLegReportingType": "string",
        #     "text": "string",
        #     "transactTime": "2018-05-22T07:35:42.399Z",
        #     "timestamp": "2018-05-22T07:35:42.399Z"
        #   }
        instrument = self.rinstruments[data['symbol']]
        status = data['ordStatus'].lower()
        if status == 'new' or 'partially' in status:
            state = OrderState.ACTIVE
        elif status == 'filled':
            state = OrderState.FILLED
        elif status == 'canceled':
            state = OrderState.CANCELED
        else:
            state = OrderState.UNKNOWN
        qty_displayed = data['displayQty']
        is_hidden = False
        is_iceberg = False
        if qty_displayed == 0:
            is_hidden = True
            qty_displayed = None
        elif qty_displayed and qty_displayed > 0:
            is_iceberg = True
        return Order(order_id=data['orderID'],
                     timestamp=self._parse_time(data['timestamp']),
                     instrument=instrument, order_type=data['ordType'], side=data['side'], qty=float(data['orderQty']),
                     state=state, price=float(data['price']), price_executed_average=data['avgPx'],
                     qty_displayed=qty_displayed, qty_executed=float(data['cumQty']),
                     is_hidden=is_hidden, is_iceberg=is_iceberg, _data=data)

    @dedup(lambda x: x.order_id)
    def get_orders(self, *, instrument: str = None, active_only: bool = True,
                   order_ids: Iterable[Hashable] = None) -> Iterator[Order]:
        if order_ids:
            kwargs = dict(filter=json.dumps({'orderID': list(order_ids)}))
            res = self.private_get('/order', **kwargs)
            for x in res:
                yield self._convert_order(x)
        else:
            limit = 100
            kwargs = dict(count=limit)
            if instrument:
                kwargs.update(symbol=self.instruments[instrument].name_id)
            if active_only:
                kwargs.update(filter=json.dumps({'open': True}))
            start = 0
            while True:
                kwargs.update(start=start)
                start += limit
                res = self.private_get('/order', **kwargs)
                for x in res:
                    yield self._convert_order(x)
                if len(res) < limit:
                    break

    _order_type_map = {
        OrderType.MARKET: 'Market',
        OrderType.LIMIT: 'Limit',
    }

    def submit_order(self, instrument: str, order_type: str, side: str, price: float, qty: float,
                     *, margin: bool = False, params: dict = None, **__) -> Order:
        assert margin, 'margin only'
        side = side.lower().capitalize()
        kwargs = dict(symbol=self.instruments[instrument].name_id,
                      side=side, orderQty=qty, orderType=self._order_type_map.get(order_type, order_type))
        if price:
            kwargs.update(price=price)
        kwargs.update(params or {})
        res = self.private_post('/order', **kwargs)
        return self._convert_order(res)

    def cancel_order(self, order_id: Hashable) -> Optional[Order]:
        res = self.private_delete('/order', orderID=order_id)
        return self._convert_order(res[0])

    def update_order(self, order_id: Hashable, *, params: dict) -> Optional[Order]:
        raise NotSupportedError('not supported yet')

    @dedup(lambda x: x.execution_id)
    def get_private_executions(self, instrument: str) -> Iterator[Execution]:
        limit = 10
        kwargs = dict(count=limit, symbol=self.instruments[instrument].name_id,
                      reverse='true', filter=json.dumps(dict(execType='Trade')))
        start = 0
        while True:
            kwargs.update(start=start)
            start += limit
            res = self.private_get('/execution/tradeHistory', **kwargs)
            for x in res:
                yield Execution(execution_id=x['execID'],
                                timestamp=self._parse_time(x['timestamp']),
                                instrument=instrument,
                                side=x['side'],
                                price=x['price'],
                                qty=x['cumQty'],
                                _data=x)
            if len(res) < limit:
                break

    def get_positions(self, *, instrument: str = None, active_only: bool = True,
                      position_ids: Iterable[Hashable] = None) -> Iterator[Position]:
        pass

    def close_position(self, position_id: Hashable, *, qty: float = None) -> Optional[Position]:
        pass
