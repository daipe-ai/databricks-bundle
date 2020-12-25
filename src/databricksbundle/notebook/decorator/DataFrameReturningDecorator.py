from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.notebook.decorator.AbstractDecorator import AbstractDecorator

class DataFrameReturningDecorator(AbstractDecorator):

    def onExecution(self, container: ContainerInterface):
        result = super().onExecution(container)
        self._function.__globals__[self._function.__name__ + '_df'] = result

        return result
