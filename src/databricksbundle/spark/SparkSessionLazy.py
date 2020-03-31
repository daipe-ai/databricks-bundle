import platform
from databricksbundle.detector import isDatabricks

class SparkSessionLazy:

    def __init__(self, factoryCallback: callable):
        self._factoryCallback = factoryCallback
        self._sparkSession = None

    def __getattr__(self, attributeName):
        if self._sparkSession is None:
            if not isDatabricks():
                from databricksbundle.spark.javaHomeSetter import setJavaHome # pylint: disable = import-outside-toplevel
                from databricksbundle.spark.existingSparkCleanup import cleanExistingSparkConfig  # pylint: disable = import-outside-toplevel
                setJavaHome()
                cleanExistingSparkConfig()

                if platform.system() == 'Windows':
                    from databricksbundle.spark.hadoopHomeSetter import setHadoopHomeEnvVar # pylint: disable = import-outside-toplevel
                    setHadoopHomeEnvVar()

            self._sparkSession = self._factoryCallback()

        return getattr(self._sparkSession, attributeName)
