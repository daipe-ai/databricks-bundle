from types import FunctionType

def checkArgs(a):
    # called as @dataFrameLoader (without brackets and any arguments)
    if a and isinstance(a[0], FunctionType):
        raise Exception('Use @dataFrameLoader() instead of @dataFrameLoader please')
