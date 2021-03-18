import json
import sys
from pyfonycore.bootstrap.config import config_factory as pyfony_config_factory
from pyfonycore.bootstrap.config.Config import Config

if sys.version_info >= (3, 8):
    from importlib import metadata as importlib_metadata
else:
    import importlib_metadata


def read() -> Config:
    entry_points = importlib_metadata.entry_points().get("pyfony.bootstrap", ())

    if not entry_points:
        raise Exception("pyfony.bootstrap entry points is missing in the master package, try rebuilding the package")

    raw_config = json.loads(tuple(entry_points)[0].dist.read_text("bootstrap_config.json"))

    return pyfony_config_factory.create(raw_config, "pyfony.bootstrap entry point")


def entry_point_exists():
    return "pyfony.bootstrap" in importlib_metadata.entry_points()
