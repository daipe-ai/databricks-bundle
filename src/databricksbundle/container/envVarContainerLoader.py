import os
from databricksbundle.pipeline.decorator.containerLoader import loadContainerUsingEnvVar

container = loadContainerUsingEnvVar(os.environ['APP_ENV'])
