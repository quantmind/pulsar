
_PENDING = 'PENDING'
_CANCELLED = 'CANCELLED'
_FINISHED = 'FINISHED'


cdef class Deferred:

    def __init__(self):
        self._result = None
        self._state = _PENDING
        self._callbacks = []
