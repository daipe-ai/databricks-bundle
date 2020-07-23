# pylint: disable = unused-import
import sys
from databricksbundle.detector import isDatabricks

if isDatabricks():
    from databricksbundle.pipeline.decorator.environment.databricks import pipelineFunction, dataFrameLoader, transformation, dataFrameSaver
elif 'unittest' in sys.modules:
    from databricksbundle.pipeline.decorator.environment.unittest import pipelineFunction, dataFrameLoader, transformation, dataFrameSaver
else:
    from databricksbundle.pipeline.decorator.environment.pyscript import pipelineFunction, dataFrameLoader, transformation, dataFrameSaver
