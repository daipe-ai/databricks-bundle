import os
from injecta.dtype.classLoader import loadClass
from injecta.container.ContainerInterface import ContainerInterface

def loadContainerUsingEnvVar(appEnv: str) -> ContainerInterface:
    if 'CONTAINER_INIT_FUNCTION' not in os.environ:
        raise Exception('CONTAINER_INIT_FUNCTION environment variable must be set on cluster')

    containerInitFunctionPath = os.environ['CONTAINER_INIT_FUNCTION']
    containerInitModuleName = containerInitFunctionPath[0:containerInitFunctionPath.rfind('.')]
    containerInitFunctionName = containerInitFunctionPath[containerInitFunctionPath.rfind('.') + 1:]

    containerInitFunction = loadClass(containerInitModuleName, containerInitFunctionName)
    return containerInitFunction(appEnv)

def containerInitEnvVarDefined():
    return 'CONTAINER_INIT_FUNCTION' in os.environ
