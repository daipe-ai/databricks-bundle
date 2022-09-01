import sys


def resolve() -> str:
    return min([path for path in sys.path if path.startswith("/Workspace/Repos")], key=len)
