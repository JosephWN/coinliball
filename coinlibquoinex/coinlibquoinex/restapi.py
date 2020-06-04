from requests import HTTPError, Response

from coinlib.errors import RateLimitError
from coinlib.trade.restapi import RestApi as RestApiBase
from .auth import Auth


class RestApi(RestApiBase):
    NAME = 'quoinex'
    BASE_URL = 'https://api.quoine.com'
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
            elif res.status_code == 422:
                raise HTTPError(res.json()) from exc

        super().on_error(exc)
