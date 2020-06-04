from pathlib import Path
from typing import Union

import yaml

DEFAULT_PATH = Path.home() / '.coinconf.yaml'


def load(path: Union[str, Path] = None) -> dict:
    path = Path(path or DEFAULT_PATH)
    with path.open() as f:
        return yaml.load(f)
