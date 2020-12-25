# pylint: disable = invalid-name, not-callable
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.display import display as displayFunction
from databricksbundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass

class dataFrameLoader(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, display=False): # pylint: disable = unused-argument
        self._display = display

    def afterExecution(self, container: ContainerInterface):
        if self._display:
            displayFunction(self._result)
