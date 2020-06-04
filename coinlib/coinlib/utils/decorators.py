import functools
from collections import OrderedDict
from typing import Hashable, Any, Callable


def dedup(key: Callable[[Any], Hashable], cache_limit: int = 5000):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            cache = OrderedDict()
            for x in f(*args, **kwargs):
                _id = key(x)
                if _id not in cache:
                    yield x
                    cache[_id] = True
                if len(cache) > cache_limit:
                    cache.popitem(last=False)

        return wrapper

    return decorator
