from pathlib import Path
from typing import Dict
from injecta.container.ContainerInterface import ContainerInterface
from injecta.dtype.classLoader import loadClass
from injecta.service.class_.InspectedArgument import InspectedArgument
from databricksbundle.pipeline.function.service.ServiceResolverInterface import ServiceResolverInterface

class ServiceResolver:

    def __init__(
        self,
        serviceResolvers: Dict[str, str],
        container: ContainerInterface
    ):
        self.__serviceResolversMapping = serviceResolvers
        self.__container = container

    def resolve(self, inspectedArgument: InspectedArgument, pipelinePath: Path):
        argumentType = str(inspectedArgument.dtype)

        if argumentType in self.__serviceResolversMapping:
            if self.__serviceResolversMapping[argumentType][0:1] != '@':
                raise Exception('Service name must start with @')

            serviceResolverName = self.__serviceResolversMapping[argumentType][1:]
            serviceResolver = self.__container.get(serviceResolverName) # type: ServiceResolverInterface

            return serviceResolver.resolve(pipelinePath)

        class_ = loadClass(inspectedArgument.dtype.moduleName, inspectedArgument.dtype.className) # pylint: disable = invalid-name

        return self.__container.get(class_)
