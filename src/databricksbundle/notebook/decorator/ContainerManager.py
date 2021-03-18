import os
from databricksbundle.bootstrap import bootstrap_config_reader
from injecta.container.ContainerInterface import ContainerInterface


class ContainerManager:

    _container: ContainerInterface

    @classmethod
    def set_container(cls, container: ContainerInterface):
        cls._container = container

    @classmethod
    def get_container(cls):
        if not hasattr(cls, "_container"):
            cls._container = cls._create_container()

        return cls._container

    @staticmethod
    def _create_container():
        bootstrap_config = bootstrap_config_reader.read()
        return bootstrap_config.container_init_function(os.environ["APP_ENV"], bootstrap_config)
