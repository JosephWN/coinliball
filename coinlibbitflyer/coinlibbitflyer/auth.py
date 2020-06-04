import hashlib
import hmac
import time
import urllib.parse

import requests

from coinlib.trade.auth import Auth as AuthBase


class Auth(AuthBase):
    def sign(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        method = req.method.upper()
        parsed = urllib.parse.urlsplit(req.url)
        path = parsed.path

        body = ''
        if method == 'GET':
            parsed = urllib.parse.urlsplit(req.url)
            if parsed.query:
                body = '?' + parsed.query
        else:
            if req.body:
                body = req.body

        nonce = str(self.get_nonce())
        text = (nonce + method + path + body).encode()
        secret = self.api_secret.encode()
        signature = hmac.new(secret, text, hashlib.sha256).hexdigest()
        headers = {
            'ACCESS-KEY': self.api_key,
            'ACCESS-TIMESTAMP': nonce,
            'ACCESS-SIGN': signature,
            'Content-Type': 'application/json',
        }
        req.headers.update(headers)
        return req

    @classmethod
    def get_nonce(cls):
        return time.time()
