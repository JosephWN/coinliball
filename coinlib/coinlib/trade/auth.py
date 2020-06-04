import time
from abc import ABC, abstractmethod

import requests.auth


class Auth(requests.auth.AuthBase, ABC):
    """
    Authentication base class for REST API
    """

    def __init__(self, credential: dict):
        self.credential = credential

    @property
    def api_key(self) -> str:
        return self.credential.get('api_key')

    @property
    def api_secret(self) -> str:
        return self.credential.get('api_secret')

    def __call__(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        assert self.api_key and self.api_secret
        return self.sign(req)

    @abstractmethod
    def sign(self, req: requests.PreparedRequest) -> requests.PreparedRequest:
        pass

    @classmethod
    def get_nonce(cls):
        """Override if necessary"""
        return int(time.time() * 1000)
