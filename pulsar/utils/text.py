from .pep import native_str, to_string, ispy3k

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
    
def capfirst(x):
    x = to_string(x).strip()
    if x:
        return x[0].upper() + x[1:]
    else:
        return x
    
def nicename(name):
    name = to_string(name)
    return capfirst(' '.join(name.replace('-',' ').replace('_',' ').split()))

if ispy3k:

    class UnicodeMixin(object):

        def __unicode__(self):
            return '%s object' % self.__class__.__name__

        def __str__(self):
            return self.__unicode__()

        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__, self)

else: # Python 2    # pragma nocover

    class UnicodeMixin(object):

        def __unicode__(self):
            return unicode('%s object' % self.__class__.__name__)

        def __str__(self):
            return self.__unicode__().encode()

        def __repr__(self):
            return '%s: %s' % (self.__class__.__name__, self)