import os
from tempfile import mkstemp

import yaml

from coinlib.utils import config


def test_config():
    data = dict(x=1)
    _, path = mkstemp()
    try:
        with open(path, 'w') as f:
            yaml.dump(data, f)
        assert config.load(path=path) == data
    finally:
        os.remove(path)
