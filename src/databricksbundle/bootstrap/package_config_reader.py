import json
import sys
from pyfonycore.bootstrap.config import config_factory as pyfony_config_factory
from pyfonycore.bootstrap.config.Config import Config
from databricksbundle.detector import is_databricks_workspace

if sys.version_info >= (3, 8):
    from importlib import metadata as importlib_metadata
else:
    import importlib_metadata

# pylint: disable=wrong-import-position
from importlib_metadata import files


def read() -> Config:
    if is_databricks_workspace():
        return read_config_from_master_package()

    # pylint: disable=import-outside-toplevel
    from pyfonycore.bootstrap.config import config_reader

    return config_reader.read()


def read_config_from_master_package() -> Config:
    entry_points = importlib_metadata.entry_points().get("pyfony.bootstrap", ())

    if not entry_points:
        raise Exception("pyfony.bootstrap entry points is missing in the master package, try rebuilding the package")

    if hasattr(tuple(entry_points)[0], "dist"):
        dist_path = tuple(entry_points)[0].dist
    else:
        package_name = entry_points.value
        dist_path = files(package_name)[0].dist

    raw_config = json.loads(dist_path.read_text("bootstrap_config.json"))

    return pyfony_config_factory.create(raw_config, "pyfony.bootstrap entry point")


def entry_point_exists() -> bool:
    return "pyfony.bootstrap" in importlib_metadata.entry_points()
