import time

from .string import *

try:
    pypy = True
    import __pypy__
except ImportError:
    pypy = False


def identity(x):
    return x


default_timer = time.monotonic
as_iterator = identity
