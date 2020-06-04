import requests

from coinlib.errors import CoinError
from coinlib.trade.restapi import RestApi as RestApiBase
from .auth import Auth

try:
    from python_bitbankcc.utils import ERROR_CODES
except ImportError:
    ERROR_CODES = {}


class RestApi(RestApiBase):
    NAME = 'bitbankcc'
    AUTH_CLASS = Auth
    CONTENT_TYPE = 'application/json'

    def get_url(self, path: str, is_private: bool):
        if is_private:
            url = ''.join(['https://api.bitbank.cc/v1', path])
        else:
            url = ''.join(['https://public.bitbank.cc', path])
        return url

    def on_response(self, res: requests.Response, is_private: bool):
        res.raise_for_status()
        data = res.json()
        if not data['success']:
            code = data['data']['code']
            message = ERROR_CODES.get(str(code))
            raise CoinError(dict(code=code, message=message))
        return data['data']
