from pathlib import Path
from typing import Dict
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.DType import DType
from injecta.dtype.classLoader import loadClass
from databricksbundle.pipeline.function.service.ServiceResolverInterface import ServiceResolverInterface

class ServiceResolver:

    def __init__(
        self,
        serviceResolvers: Dict[str, str],
        container: ContainerInterface
    ):
        self.__serviceResolversMapping = serviceResolvers
        self.__container = container

    def resolve(self, dtype: DType, pipelinePath: Path):
        argumentType = str(dtype)

        if argumentType in self.__serviceResolversMapping:
            if self.__serviceResolversMapping[argumentType][0:1] != '@':
                raise Exception('Service name must start with @')

            serviceResolverName = self.__serviceResolversMapping[argumentType][1:]
            serviceResolver = self.__container.get(serviceResolverName) # type: ServiceResolverInterface

            return serviceResolver.resolve(pipelinePath)

        class_ = loadClass(dtype.moduleName, dtype.className) # pylint: disable = invalid-name

        return self.__container.get(class_)
