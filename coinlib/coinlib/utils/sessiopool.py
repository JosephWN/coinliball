import contextlib
from queue import LifoQueue

from requests import Session


class SessionPool:
    def __init__(self, pool_size: int, *, timeout: float = 60):
        self.pool_size = pool_size
        self.timeout = timeout
        self.q = LifoQueue()
        for _ in range(pool_size):
            self.q.put(Session())

    @contextlib.contextmanager
    def get(self) -> Session:
        if not self.pool_size:
            with Session() as s:
                yield s
        else:
            s = None
            try:
                s = self.q.get(timeout=self.timeout)
                yield s
            finally:
                if s:
                    self.q.put(s)
