from queue import Empty

import pytest

from coinlib.utils.sessiopool import SessionPool


def test_session_pool():
    pool = SessionPool(0)
    with pool.get() as s1:
        pass
    with pool.get() as s2:
        pass
    assert s1 is not s2

    pool = SessionPool(1)
    with pool.get() as s1:
        pass
    with pool.get() as s2:
        pass
    assert s1 is s2

    pool = SessionPool(1, timeout=0.2)
    with pool.get():
        with pytest.raises(Empty):
            with pool.get():
                pass

    pool = SessionPool(10)
    ss = set()
    for _ in range(10):
        ss.add(pool.get())
    assert len(ss) == 10
