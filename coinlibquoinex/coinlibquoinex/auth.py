import urllib.parse

import jwt
import requests

from coinlib.trade.auth import Auth as AuthBase


class Auth(AuthBase):
    def sign(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        nonce = self.get_nonce()

        parsed = urllib.parse.urlsplit(req.url)
        path = parsed.path
        query = parsed.query

        if query:
            path += '?' + query
        auth_payload = {
            'path': path,
            'nonce': nonce,
            'token_id': self.api_key,
        }

        signature = jwt.encode(auth_payload, self.api_secret, 'HS256')

        headers = {
            'X-Quoine-API-Version': '2',
            'X-Quoine-Auth': signature,
            'Content-Type': 'application/json'
        }
        req.headers.update(headers)
        return req
