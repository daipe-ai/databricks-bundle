# pylint: disable = invalid-name
import sys
from typing import Tuple
from types import FunctionType
from databricksbundle.detector import isDatabricks
from databricksbundle.display import display

# ---------------------------------------------------------------------------------------------------------------------

def checkArgs(a):
    # called as @dataFrameLoader (without brackets and any arguments)
    if a and isinstance(a[0], FunctionType):
        raise Exception('Use @dataFrameLoader() instead of @dataFrameLoader please')

if isDatabricks():
    class dataFrameLoader:

        def __init__(self, *args, **kwargs):
            self._displayEnabled = kwargs.get('display', False)
            checkArgs(args)

        def __call__(self, fun, *args, **kwargs):
            g = fun.__globals__
            df = fun()
            g[fun.__name__ + '_df'] = df

            if self._displayEnabled:
                display(df)

            return fun

    class transformation:

        def __init__(self, *args: Tuple[callable], **kwargs):
            self._sources = args
            self._displayEnabled = kwargs.get('display', False)

        def __call__(self, fun, *args, **kwargs):
            g = fun.__globals__

            def transformSource(source: callable):
                return g[source.__name__ + '_df']

            dataframesToUse = list(map(transformSource, self._sources))

            df = fun(*dataframesToUse)
            g[fun.__name__ + '_df'] = df

            if self._displayEnabled:
                display(df)

            return fun

    class dataFrameSaver:

        def __init__(self, *args: Tuple[callable]):
            self._sources = args

        def __call__(self, fun, *args, **kwargs):
            g = fun.__globals__

            def transformSource(source: callable):
                return g[source.__name__ + '_df']

            dataframesToUse = list(map(transformSource, self._sources))

            fun(*dataframesToUse)

            return fun

# ---------------------------------------------------------------------------------------------------------------------

elif 'unittest' in sys.modules:
    class dataFrameLoader:

        def __init__(self, *args, **kwargs): # pylint: disable = unused-argument
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

# ---------------------------------------------------------------------------------------------------------------------

else:
    from databricksbundle.pipeline.pipelineInvoker import invokePipeline
    from databricksbundle.pipeline.Pipeline import Pipeline

    pipeline = Pipeline()

    class dataFrameLoader:

        def __init__(self, *args, **kwargs): # pylint: disable = unused-argument
            checkArgs(args)

        def __call__(self, fun, *args, **kwargs):
            pipeline.addDataFrameLoader(fun)
            return fun

    class transformation:

        def __init__(self, *args: Tuple[callable], **kwargs): # pylint: disable = unused-argument
            self._sources = args

        def __call__(self, fun, *args, **kwargs):
            pipeline.addTransformation(fun, self._sources)
            return fun

    class dataFrameSaver:

        def __init__(self, *args: Tuple[callable]):
            self._sources = args

        def __call__(self, fun, *args, **kwargs):
            dataFramesByName = invokePipeline(pipeline)

            def transformSource(source: callable):
                return dataFramesByName[source.__name__ + '_df']

            dataframesToUse = list(map(transformSource, self._sources))

            fun(*dataframesToUse)

            return fun
