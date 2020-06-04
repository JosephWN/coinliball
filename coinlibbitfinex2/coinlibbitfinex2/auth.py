import hashlib
import hmac
import json
import urllib.parse

import requests

from coinlib.trade.auth import Auth as AuthBase


class Auth(AuthBase):
    def sign(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        nonce = str(self.get_nonce())

        parsed = urllib.parse.urlsplit(req.url)

        method = req.method.upper()
        assert method == 'POST', 'private method only POST'
        data = {}
        if req.body:
            data = json.loads(req.body)
        payload = '/api' + parsed.path + nonce + json.dumps(data)
        secret = self.api_secret.encode()
        signature = hmac.new(secret, payload.encode(), hashlib.sha384).hexdigest()
        headers = {
            'bfx-nonce': nonce,
            'bfx-apikey': self.api_key,
            'bfx-signature': signature,
            'Content-Type': 'application/json',
        }
        req.headers.update(headers)
        return req
