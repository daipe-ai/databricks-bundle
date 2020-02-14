import os
import IPython
from pyfonybundles.appContainerInit import initAppContainer
from databricksbundle.detector import isDatabricks

container = initAppContainer(os.environ['APP_ENV'])

if isDatabricks():
    IPython.get_ipython().user_ns['spark'] = container.get('pyspark.sql.session.SparkSession')
