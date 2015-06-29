import time

from .string import *   # noqa

try:
    pypy = True
    import __pypy__     # noqa
except ImportError:     # pragma    nocover
    pypy = False


def identity(x):
    return x


default_timer = time.monotonic
as_iterator = identity
