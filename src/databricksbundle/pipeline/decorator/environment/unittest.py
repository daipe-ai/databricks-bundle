# pylint: disable = invalid-name
from databricksbundle.pipeline.decorator.argsChecker import checkArgs

class dataFrameLoader:

    def __init__(self, *args, **kwargs):  # pylint: disable = unused-argument
        checkArgs(args)

    def __call__(self, fun, *args, **kwargs):
        return fun

class transformation:

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, fun, *args, **kwargs):
        return fun

class dataFrameSaver:

    def __init__(self, *args):
        pass

    def __call__(self, fun, *args, **kwargs):
        return fun
