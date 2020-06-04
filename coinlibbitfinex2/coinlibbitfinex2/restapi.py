from requests import HTTPError, Response

from coinlib.errors import RateLimitError, NotFoundError
from coinlib.trade.restapi import RestApi as RestApiBase
from .auth import Auth


class RestApi(RestApiBase):
    NAME = 'bitfinex2'
    BASE_URL = 'https://api.bitfinex.com/v2'
    AUTH_CLASS = Auth
    CONTENT_TYPE = 'application/json'
    RATE_LIMIT_WAIT = 15

    def on_error(self, exc: Exception):
        if isinstance(exc, HTTPError):
            res: Response = exc.response
            if res.status_code == 429:
                raise RateLimitError(res.json(), wait_seconds=self.RATE_LIMIT_WAIT) from exc
            elif res.status_code == 400:
                raise HTTPError(res.json()) from exc
            elif res.status_code == 404:
                raise NotFoundError(res.json()) from exc

        super().on_error(exc)
