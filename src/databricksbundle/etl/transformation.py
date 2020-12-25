# pylint: disable = invalid-name, not-callable
from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.display import display as displayFunction
from databricksbundle.notebook.decorator.DataFrameReturningDecorator import DataFrameReturningDecorator
from databricksbundle.notebook.decorator.DuplicateColumnsChecker import DuplicateColumnsChecker
from databricksbundle.notebook.decorator.DecoratorMetaclass import DecoratorMetaclass

class transformation(DataFrameReturningDecorator, metaclass=DecoratorMetaclass):

    def __init__(self, *args, display=False, checkDuplicateColumns=True): # pylint: disable = unused-argument
        self._display = display
        self._checkDuplicateColumns = checkDuplicateColumns

    def afterExecution(self, container: ContainerInterface):
        if self._checkDuplicateColumns:
            duplicateColumnsChecker: DuplicateColumnsChecker = container.get(DuplicateColumnsChecker)

            dataFrameDecorators = tuple(decoratorArg for decoratorArg in self._decoratorArgs if isinstance(decoratorArg, DataFrameReturningDecorator))
            duplicateColumnsChecker.check(self._result, dataFrameDecorators)

        if self._display:
            displayFunction(self._result)
