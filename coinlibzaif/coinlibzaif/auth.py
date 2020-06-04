import hashlib
import hmac
import time
import urllib.parse

import requests

from coinlib.trade.auth import Auth as AuthBase


class Auth(AuthBase):
    def sign(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        method = req.method.upper()
        assert method == 'POST', 'POST method only, method={}'.format(method)

        body = req.body
        assert body
        data = dict(urllib.parse.parse_qsl(body))
        data['nonce'] = self.get_nonce()
        body = urllib.parse.urlencode(data)
        req.body = body

        secret = self.api_secret.encode()
        signature = hmac.new(secret, body.encode(), hashlib.sha512).hexdigest()
        headers = {
            'key': self.api_key,
            'sign': signature,
        }
        req.headers.update(headers)
        return req

    @classmethod
    def get_nonce(cls):
        return time.time()
