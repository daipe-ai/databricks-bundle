from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.notebook.function.ArgumentsResolver import ArgumentsResolver
from databricksbundle.notebook.function.functionInspector import inspectFunction

class AbstractDecorator:

    _isDecorator = True # use this instead of isinstance(decoratorArgument, AbstractDecorator) which does not work probably due to some cyclic import
    _decoratorArgs: tuple = tuple()
    _function: callable = lambda: None
    _result = None

    @property
    def function(self):
        return self._function

    @property
    def result(self):
        return self._result

    def onExecution(self, container: ContainerInterface):
        argumentsResolver: ArgumentsResolver = container.get(ArgumentsResolver)
        arguments = argumentsResolver.resolve(inspectFunction(self._function), self._decoratorArgs)

        return self._function(*arguments)

    def afterExecution(self, container: ContainerInterface):
        pass

    def __call__(self, *args, **kwargs):
        pass
