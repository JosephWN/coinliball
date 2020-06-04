from typing import Iterable, Union


def no_none_dict(_d: Union[dict, Iterable] = None, **kwargs) -> dict:
    _d = dict(_d or {})
    _d.update(kwargs)
    return {k: v for k, v in _d.items() if v is not None}
