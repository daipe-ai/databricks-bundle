import platform

class SparkSessionLazy:

    def __init__(self, factoryCallback: callable):
        self._factoryCallback = factoryCallback
        self._sparkSession = None

    def __getattr__(self, attributeName):
        if self._sparkSession is None:
            if platform.system() == 'Windows':
                from databricksbundle.spark.hadoopHomeSetter import setHadoopHomeEnvVar # pylint: disable = import-outside-toplevel
                setHadoopHomeEnvVar()

            self._sparkSession = self._factoryCallback()

        return getattr(self._sparkSession, attributeName)
