from types import CoroutineType
from collections.abc import Awaitable


cdef tuple AWAITABLES = (CoroutineType, Awaitable)


cpdef object isawaitable(object object):
    """Return true if object can be passed to an ``await`` expression."""
    return isinstance(object, AWAITABLES)
