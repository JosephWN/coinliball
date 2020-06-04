from coinlib.coinlib.errors import CoinError
from requests import Response

from coinlib.trade.restapi import RestApi as RestApiBase
from .auth import Auth


class RestApi(RestApiBase):
    BASE_URL = 'https://api.zaif.jp'
    AUTH_CLASS = Auth

    def get_url(self, path: str, is_private: bool):
        if is_private:
            url = ''.join(['https://api.bitbank.cc/v1', path])
        else:
            url = ''.join(['https://public.bitbank.cc', path])
        return url

    def public_get(self, path: str, **kwargs):
        assert path.startswith('/'), path
        return self.request('GET', '/api/1' + path, False, kwargs)

    def private_post(self, method: str, **kwargs):
        kwargs = dict(kwargs, method=method)
        return self.request('POST', '/tapi', True, kwargs)

    def public_leverage_get(self, path: str, **kwargs):
        assert path.startswith('/'), path
        return self.request('GET', '/fapi/1' + path, False, kwargs)

    def private_leverage_post(self, method: str, **kwargs):
        kwargs = dict(kwargs, method=method)
        return self.request('POST', '/tlapi', True, kwargs)

    def on_response(self, res: Response, is_private: bool):
        data = super().on_response(res, is_private)
        if 'success' not in data:
            return data
        if not data['success']:
            raise CoinError(data)
        return data['return']
