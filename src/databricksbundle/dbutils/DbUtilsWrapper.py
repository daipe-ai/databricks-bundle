import types
import platform

class DbUtilsWrapper:

    def __init__(self, factoryCallback: callable):
        self._factoryCallback = factoryCallback
        self._dbUtils = None

    def __getattr__(self, attributeName):
        if self._dbUtils is None:
            if platform.system() == 'Windows':
                from databricksbundle.spark.hadoopHomeSetter import setHadoopHomeEnvVar # pylint: disable = import-outside-toplevel
                setHadoopHomeEnvVar()

            self._dbUtils = self._factoryCallback()

        if hasattr(self._dbUtils, attributeName) is False:
            raise AttributeError(attributeName)

        attr = getattr(self._dbUtils, attributeName)

        if isinstance(attr, types.FunctionType) is False:
            return attr

        def wrapper(*args, **kw):
            return attr(*args, **kw)

        return wrapper
