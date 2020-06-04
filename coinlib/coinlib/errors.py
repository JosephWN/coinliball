from typing import Any


class CoinError(Exception):
    @property
    def info(self) -> Any:
        return self.args[0]


class NotSupportedError(CoinError):
    pass


class ApiError(CoinError):
    def __init__(self, code: int, message: str):
        super().__init__(dict(code=code, message=message))
        self.code = code
        self.message = message


class ApiTimeoutError(CoinError):
    pass


class RetryError(CoinError):
    def __init__(self, *args, wait_seconds: float = 0):
        super().__init__(*args)
        self.wait_seconds = wait_seconds


class RateLimitError(RetryError):
    pass


class NotFoundError(CoinError):
    pass
