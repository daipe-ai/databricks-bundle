import os
import sys


def resolve() -> str:
    return min([path for path in sys.path if path in os.getcwd()], key=len)
