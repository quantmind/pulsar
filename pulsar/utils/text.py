from .httpurl import native_str

class _lazy:

    def __init__(self, f, args, kwargs):
        self._value = None
        self._f = f
        self.args = args
        self.kwargs = kwargs
    
    def __str__(self):
        if self._value is None:
            self._value = native_str(self._f(*self.args, **self.kwargs) or '')
        return self._value
    __repr__ = __str__


def lazy_string(f):
    def _(*args, **kwargs):
        return _lazy(f, args, kwargs)
    return _
    