# pylint: disable = import-outside-toplevel, unused-import
from databricksbundle.bootstrap.packageConfigReader import entryPointExists

if entryPointExists():
    from databricksbundle.bootstrap.packageConfigReader import read
else:
    from pyfonycore.bootstrap.config.configReader import read
