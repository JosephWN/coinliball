import hashlib
import hmac
import urllib.parse

import requests

from coinlib.trade.auth import Auth as AuthBase


class Auth(AuthBase):
    def sign(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        nonce = str(self.get_nonce())

        data = nonce
        method = req.method.upper()
        if method == 'GET':
            parsed = urllib.parse.urlsplit(req.url)
            path = parsed.path
            if parsed.query:
                path += '?' + parsed.query
            data += path
        else:
            if req.body:
                data += req.body

        secret = self.api_secret.encode()
        signature = hmac.new(secret, data.encode(), hashlib.sha256).hexdigest()
        headers = {
            'ACCESS-KEY': self.api_key,
            'ACCESS-NONCE': nonce,
            'ACCESS-SIGNATURE': signature,
            'Content-Type': 'application/json',
        }
        req.headers.update(headers)
        return req
