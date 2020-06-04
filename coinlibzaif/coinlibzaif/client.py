from collections import defaultdict, OrderedDict
from datetime import datetime

import pytz
import requests

from .apibase import ApiBase
from .errors import ApiError, RetryError


class Api(ApiBase):
    SPEC = {
        'name': 'zaif',
        'base_url': 'https://api.zaif.jp',
        'path_prefix': '',
    }
    """
      GET:
        spot: # /api/1
          - /currencies/{currency}
          - /currency_pairs/{currency_pair}
          - /last_price/{currency_pair}
          - /ticker/{currency_pair}
          - /trades/{currency_pair}
          - /depth/{currency_pair}
          - depth/{pair}
        leverage: # /fapi/1
          - /groups/{group_id}
          - /last_price/{group_id}/{currency_pair}
          - /ticker/{group_id}/{currency_pair}
          - /trades/{group_id}/{currency_pair}
          - /depth/{group_id}/{currency_pair}
    """

    def process_response(self, res: requests.Response, is_private: bool):
        if res.status_code != 200:
            if res.status_code // 100 == 5:
                raise RetryError('retry status_code={}'.format(res.status_code), interval=1)
        data = super().process_response(res, is_private)
        if 'success' not in data:
            return data
        if not data['success']:
            raise ApiError(data)
        return data['return']

    def public_spot_get(self, path: str, **kwargs):
        return self.request('GET', '/api/1' + path, False, kwargs)

    def public_get(self, path: str, **kwargs):
        return self.request('GET', '/api/1' + path, False, kwargs)

    def private_post(self, method: str, **kwargs):
        kwargs = kwargs.copy()
        kwargs.update(method=method)
        return self.request('POST', '/tapi', True, kwargs)

    def public_leverage_get(self, path: str, **kwargs):
        return self.request('GET', '/fapi/1' + path, False, kwargs)

    def private_leverage_post(self, method: str, **kwargs):
        kwargs = kwargs.copy()
        kwargs.update(method=method)
        return self.request('POST', '/tlapi', True, kwargs)

    def get_instruments(self):
        instruments = {}
        for pair in self.public_get('/currency_pairs/all'):
            currency_pair = pair['currency_pair']
            base_id, quote_id = currency_pair.split('_')
            name = pair['name']
            base, quote = name.upper().split('/')
            instruments['{}_{}'.format(base, quote)] = {
                'id': currency_pair,
                'base_id': base_id,
                'quote_id': quote_id,
                'base': base,
                'quote': quote,
                'data': pair,
            }
        return instruments

    def order_book(self, instrument: str):
        currency_pair = self.instruments[instrument]['id']
        data = self.public_get('/depth/{currency_pair}'.format(currency_pair=currency_pair))
        order_book = {}
        for k, reverse in [('asks', False), ('bids', True)]:
            order_book[k] = [(float(price), float(qty)) for price, qty in data[k]]
            order_book[k] = list(sorted(order_book[k], reverse=reverse))
        order_book['timestamp'] = pytz.UTC.localize(datetime.utcnow()).timestamp()
        return order_book

    def balances(self, with_details: bool = False):
        balances = defaultdict(lambda: dict(total=.0, used=.0, free=.0))
        res = self.private_post('get_info2')
        for k, v in res['deposit'].items():
            k = k.upper()
            d = balances[k]
            d['total'] = v
        for k, v in res['funds'].items():
            k = k.upper()
            d = balances[k]
            d['used'] = d['total'] - v
            d['free'] = v
        return OrderedDict(sorted(balances.items()))

    def leverage_positions(self, instrument: str = None):
        params = {}
        if instrument:
            params['currency_pair'] = self.instruments[instrument]['id']
        res = self.private_leverage_post('active_positions', type='margin', **params)
        positions = []
        for leverage_id, v in res.items():
            if 'amount_done' not in v:
                continue
            if v['amount_done'] == v.get('close_done'):
                continue
            qty = float(v['amount_done']) - float(v.get('close_done', 0))
            instrument = self.instruments_reversed[v['currency_pair']]
            positions.append(dict(id=int(leverage_id),
                                  instrument=instrument,
                                  side='BUY' if v['action'] == 'bid' else 'SELL',
                                  price=float(v['price_avg']),
                                  qty=qty,
                                  data=v))
        return positions
