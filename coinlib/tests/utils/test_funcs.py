from coinlib.utils.funcs import no_none_dict


def test_no_none_dict():
    assert no_none_dict(dict(a=None, b=2)) == dict(b=2)
    assert no_none_dict(a=None, b=2) == dict(b=2)
    assert no_none_dict(dict(a=None, b=2), a=1, b=None) == dict(a=1)
