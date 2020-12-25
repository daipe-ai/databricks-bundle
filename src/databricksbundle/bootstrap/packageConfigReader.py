import json
import sys
from pyfonycore.bootstrap.config import configFactory as pyfonyConfigFactory
from pyfonycore.bootstrap.config.Config import Config

if sys.version_info >= (3, 8):
    from importlib import metadata as importlib_metadata # pylint: disable = no-name-in-module
else:
    import importlib_metadata

def read() -> Config:
    entryPoints = importlib_metadata.entry_points().get('pyfony.bootstrap', ())

    if not entryPoints:
        raise Exception('pyfony.bootstrap entry points is missing in the master package, try rebuilding the package')

    rawConfig = json.loads(entryPoints[0].dist.read_text('bootstrap_config.json'))

    return pyfonyConfigFactory.create(rawConfig, 'pyfony.bootstrap entry point')

def entryPointExists():
    return 'pyfony.bootstrap' in importlib_metadata.entry_points()
