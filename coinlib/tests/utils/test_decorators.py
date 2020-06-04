from coinlib.utils.decorators import dedup


def test_dedup():
    @dedup(lambda x: x)
    def gen(it):
        yield from it

    assert list(range(3)) == list(gen(range(3)))
    assert list(range(3)) == list(gen(list(range(3)) * 2))

    @dedup(lambda x: x, cache_limit=0)
    def gen(it):
        yield from it

    assert list(range(3)) == list(gen(range(3)))
    assert list(range(3)) * 2 == list(gen(list(range(3)) * 2))

    @dedup(lambda x: x, cache_limit=2)
    def gen(it):
        yield from it

    assert [1, 2, 3, 1] == list(gen([1, 2, 3, 2, 1]))
