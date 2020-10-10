from types import FunctionType

def checkArgs(a, decoratorName: str):
    # called as @[decoratorName] (without brackets and any arguments)
    if a and isinstance(a[0], FunctionType):
        raise Exception(f'Use @{decoratorName}() instead of @{decoratorName} please')
