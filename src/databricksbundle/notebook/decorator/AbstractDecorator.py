from injecta.container.ContainerInterface import ContainerInterface
from databricksbundle.notebook.function.ArgumentsResolver import ArgumentsResolver
from databricksbundle.notebook.function.function_inspector import inspect_function


class AbstractDecorator:

    _is_decorator = (
        True  # use this instead of isinstance(decorator_argument, AbstractDecorator) which does not work probably due to some cyclic import
    )
    _decorator_args: tuple = tuple()
    _function: callable = lambda: None
    _result = None

    @property
    def function(self):
        return self._function

    @property
    def result(self):
        return self._result

    def on_execution(self, container: ContainerInterface):
        arguments_resolver: ArgumentsResolver = container.get(ArgumentsResolver)
        arguments = arguments_resolver.resolve(inspect_function(self._function), self._decorator_args)

        return self._function(*arguments)

    def after_execution(self, container: ContainerInterface):
        pass

    def __call__(self, *args, **kwargs):
        pass
