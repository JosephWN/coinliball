import base64
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
        data.update({
            'nonce': nonce,
            'request': parsed.path,
        })
        payload = base64.b64encode(json.dumps(data).encode())
        secret = self.api_secret.encode()
        signature = hmac.new(secret, payload, hashlib.sha384).hexdigest()
        headers = {
            'X-BFX-APIKEY': self.api_key,
            'X-BFX-PAYLOAD': payload.decode(),
            'X-BFX-SIGNATURE': signature,
            'Content-Type': 'application/json',
        }
        req.headers.update(headers)
        return req
