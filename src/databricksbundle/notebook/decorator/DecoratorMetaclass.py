# pylint: disable = protected-access
import re
import inspect
from types import FunctionType
from databricksbundle.notebook.decorator.AbstractDecorator import AbstractDecorator
from databricksbundle.notebook.decorator.ContainerManager import ContainerManager

class DecoratorMetaclass(type):

    def __new__(cls, name, bases, attrs):
        originalInit = attrs['__init__']

        def decorateNew(metaclass: DecoratorMetaclass, *args, **kwargs): # pylint: disable = unused-argument
            if args and isinstance(args[0], FunctionType):
                code = inspect.getsource(args[0])

                if not re.match(r'^@[a-zA-Z]+\(', code):
                    decoratorName = metaclass.__name__
                    raise Exception(f'Use @{decoratorName}() instead of @{decoratorName} please')

            return object.__new__(metaclass)

        def decorateInit(decorator: AbstractDecorator, *args, **kwargs):
            decorator._decoratorArgs = args
            originalInit(decorator, *args, **kwargs)

        def decorateCall(decorator: AbstractDecorator, fun):
            if cls._notebookFunctionExecuted(fun):
                decorator._function = fun

                container = ContainerManager.getContainer()
                decorator._result = decorator.onExecution(container)
                decorator.afterExecution(container)

                return decorator

            return fun

        attrs['__new__'] = decorateNew
        attrs['__init__'] = decorateInit
        attrs['__call__'] = decorateCall

        return super().__new__(cls, name, bases, attrs)

    @staticmethod
    def _notebookFunctionExecuted(fun):
        return fun.__module__ == '__main__'
