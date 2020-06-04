from requests import HTTPError, Response

from coinlib.trade.restapi import RestApi as RestApiBase
from .auth import Auth


class RestApi(RestApiBase):
    NAME = 'bitflyer'
    AUTH_CLASS = Auth
    CONTENT_TYPE = 'application/json'

    def get_url(self, path: str, is_private: bool):
        if is_private:
            url = ''.join(['https://api.bitflyer.jp/v1/me', path])
        else:
            url = ''.join(['https://api.bitflyer.jp/v1', path])
        return url

    def on_error(self, exc: Exception):
        if isinstance(exc, HTTPError):
            res: Response = exc.response
            raise HTTPError(res.json()) from exc
        super().on_error(exc)
