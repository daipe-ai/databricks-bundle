from databricksbundle.bootstrap.package_config_reader import entry_point_exists

if entry_point_exists():
    from databricksbundle.bootstrap.package_config_reader import read  # noqa: F401
else:
    from pyfonycore.bootstrap.config.config_reader import read  # noqa: F401
