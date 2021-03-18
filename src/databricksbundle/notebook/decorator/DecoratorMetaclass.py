import re
import inspect
from types import FunctionType
from databricksbundle.notebook.decorator.AbstractDecorator import AbstractDecorator
from databricksbundle.notebook.decorator.ContainerManager import ContainerManager


class DecoratorMetaclass(type):
    def __new__(cls, name, bases, attrs):
        original_init = attrs["__init__"]

        def decorate_new(metaclass: DecoratorMetaclass, *args, **kwargs):
            if args and isinstance(args[0], FunctionType):
                code = inspect.getsource(args[0])

                if not re.match(r"^@[a-z_]+\(", code):
                    decorator_name = metaclass.__name__
                    raise Exception(f"Use @{decorator_name}() instead of @{decorator_name} please")

            return object.__new__(metaclass)

        def decorate_init(decorator: AbstractDecorator, *args, **kwargs):
            decorator._decorator_args = args
            original_init(decorator, *args, **kwargs)

        def decorate_call(decorator: AbstractDecorator, fun):
            if cls._notebook_function_executed(fun):
                decorator._function = fun

                container = ContainerManager.get_container()
                decorator._result = decorator.on_execution(container)
                decorator.after_execution(container)

                return decorator

            return fun

        attrs["__new__"] = decorate_new
        attrs["__init__"] = decorate_init
        attrs["__call__"] = decorate_call

        return super().__new__(cls, name, bases, attrs)

    @staticmethod
    def _notebook_function_executed(fun):
        return fun.__module__ == "__main__"
