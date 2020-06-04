from abc import ABC
import json
import logging
import threading
import time
from typing import Dict, Union, Type

import requests
from requests.structures import CaseInsensitiveDict

from coinlib.errors import RetryError, ApiTimeoutError
from coinlib.trade.auth import Auth
from coinlib.utils.sessiopool import SessionPool

logger = logging.getLogger(__name__)


class RestApi(ABC):
    NAME: str = ''
    BASE_URL: str = ''
    AUTH_CLASS: Type[Auth] = None
    CONTENT_TYPE: str = 'application/x-www-form-urlencoded'

    def __init__(self, credential: dict = None, *,
                 timeout: float = 60,
                 retry_timeout: float = 300,
                 proxies: Dict[str, str] = None,
                 session_pool_size: int = 4,
                 **kwargs):
        _ = kwargs
        credential = credential or {}
        self.credential = credential
        self.timeout = timeout
        self.retry_timeout = retry_timeout
        self.proxies = self.load_proxies()
        self.proxies.update(proxies or {})
        self.session_pool = SessionPool(session_pool_size, timeout=timeout)

        self._auth = self.AUTH_CLASS(credential)
        self._private_lock = threading.RLock()

    @classmethod
    def load_proxies(cls) -> Dict[str, str]:
        """
        environment variable name: 'http_proxy', 'https_proxy'
        """
        import os
        proxies = {}
        environ = CaseInsensitiveDict(os.environ)
        for k in ['http', 'https']:
            env_name = f'{k}_proxy'
            if env_name in environ:
                proxies[k] = environ[env_name]
        return proxies

    def request(self, method: str, path: str, is_private: bool, params: dict = None) -> Union[list, dict]:
        assert path.startswith('/'), f'path={path} must start with /'
        params = params or {}
        stop = time.time() + self.retry_timeout

        def _do_request():
            with self.session_pool.get() as s:
                req = self.prepare_request(method, path, is_private, params)
                prep = s.prepare_request(req)
                if is_private:
                    prep.prepare_auth(self._auth)
                return s.send(prep, timeout=self.timeout, proxies=self.proxies)

        while True:
            if stop <= time.time():
                raise ApiTimeoutError('retry timeout')
            try:
                if is_private:
                    with self._private_lock:
                        res = _do_request()
                else:
                    res = _do_request()
                try:
                    return self.on_response(res, is_private)
                except Exception as e:
                    return self.on_error(e)
            except RetryError as e:
                logger.warning(e)
                time.sleep(e.wait_seconds)

    def prepare_request(self, method: str, path: str, is_private: bool, params: dict) -> requests.Request:
        url = self.get_url(path, is_private)
        req_kwargs = {}
        if method == 'GET':
            req_kwargs['params'] = params
        else:
            if self.CONTENT_TYPE == 'application/json':
                req_kwargs['data'] = json.dumps(params)
            else:
                req_kwargs['data'] = params

        return requests.Request(method, url, **req_kwargs)

    def get_url(self, path: str, is_private: bool) -> str:
        _ = is_private
        url = ''.join([self.BASE_URL, path])
        return url

    def on_response(self, res: requests.Response, is_private: bool) -> Union[list, dict]:
        _ = self, is_private
        res.raise_for_status()
        if res.text and 'application/json' in res.headers['Content-Type']:
            return res.json()
        return {'response': res.text}

    def on_error(self, exc: Exception) -> dict:
        raise exc

    # public

    def public_get(self, path: str, **kwargs):
        return self.request('GET', path, False, kwargs)

    def public_post(self, path: str, **kwargs):
        return self.request('POST', path, False, kwargs)

    def public_put(self, path: str, **kwargs):
        return self.request('PUT', path, False, kwargs)

    def public_patch(self, path: str, **kwargs):
        return self.request('PATCH', path, False, kwargs)

    def public_delete(self, path: str, **kwargs):
        return self.request('DELETE', path, False, kwargs)

    # private

    def private_get(self, path: str, **kwargs):
        return self.request('GET', path, True, kwargs)

    def private_post(self, path: str, **kwargs):
        return self.request('POST', path, True, kwargs)

    def private_put(self, path: str, **kwargs):
        return self.request('PUT', path, True, kwargs)

    def private_patch(self, path: str, **kwargs):
        return self.request('PATCH', path, True, kwargs)

    def private_delete(self, path: str, **kwargs):
        return self.request('DELETE', path, True, kwargs)
