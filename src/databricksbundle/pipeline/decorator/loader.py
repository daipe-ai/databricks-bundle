from databricksbundle.detector import isDatabricks
from databricksbundle.notebook.helpers import isNotebookEnvironment

if isDatabricks() and isNotebookEnvironment():
    from databricksbundle.pipeline.decorator.environment.databricksNotebook import pipelineFunction, dataFrameLoader, transformation, dataFrameSaver # pylint: disable = unused-import
else:
    from databricksbundle.pipeline.decorator.environment.pyscript import pipelineFunction, dataFrameLoader, transformation, dataFrameSaver # pylint: disable = unused-import
