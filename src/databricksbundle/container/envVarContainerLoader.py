import os
from databricksbundle.notebook.decorator.containerLoader import loadContainerUsingEnvVar

container = loadContainerUsingEnvVar(os.environ['APP_ENV'])
