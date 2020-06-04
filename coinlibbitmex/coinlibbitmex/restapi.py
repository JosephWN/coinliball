from requests import HTTPError, Response

from coinlib.trade.restapi import RestApi as RestApiBase
from .auth import Auth


class RestApi(RestApiBase):
    NAME = 'bitmex'
    BASE_URL = 'https://www.bitmex.com/api/v1'
    AUTH_CLASS = Auth
    CONTENT_TYPE = 'application/json'

    def on_error(self, exc: Exception):
        if isinstance(exc, HTTPError):
            res: Response = exc.response
            raise HTTPError(res.json()) from exc

        super().on_error(exc)
