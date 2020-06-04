import hashlib
import hmac
import time
import urllib.parse

import requests

from coinlib.trade.auth import Auth as AuthBase


class Auth(AuthBase):
    EXPIRATION = 10

    def sign(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        method = req.method.upper()
        parsed = urllib.parse.urlsplit(req.url)
        path = parsed.path
        if parsed.query:
            path += '?' + parsed.query

        expiration = str(int(time.time() + self.EXPIRATION))
        message = method + path + expiration
        if req.body:
            message += req.body

        signature = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
        headers = {
            'api-expires': expiration,
            'api-key': self.api_key,
            'api-signature': signature,
            'Content-Type': 'application/json',
        }
        req.headers.update(headers)
        return req
