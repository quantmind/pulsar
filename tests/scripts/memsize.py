from pympler.asizeof import asizeof


class Deferred(object):
    _paused = 0
    _runningCallbacks = False
    _suppressAlreadyCalled = False
    _timeout = None
    _callbacks = None

    def __init__(self, loop=None):
        self._loop = loop or 1
        self._state = 0


class DeferredSlots(object):
    __slots__ = ('_paused', '_runningCallbacks', '_suppressAlreadyCalled',
                 '_timeout', '_callbacks', '_state', '_loop')

    def __init__(self, loop=None):
        self._loop = loop or 1
        self._paused = 0
        self._runningCallbacks = False
        self._suppressAlreadyCalled = False
        self._timeout = None
        self._callbacks = None
        self._state = 0


d1 = Deferred()
d2 = DeferredSlots()

print('Deferred size, dict: %s, slots: %s ' % (asizeof(d1), asizeof(d2)))
