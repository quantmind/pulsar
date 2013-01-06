'''Decorator for RPC functions operating with users and model instances
'''
from pulsar.utils.tools import checkarity

from .exceptions import InvalidParams


def wrap_object_call(fname, namefunc):
    def _(self,*args,**kwargs):
        f = getattr(self, fname)
        return f(*args,**kwargs)
    _.__name__ = namefunc
    return _
    
def rpcerror(func, args, kwargs, discount=0):
    msg = checkarity(func, args, kwargs, discount=discount)
    if msg:
        raise InvalidParams('Invalid Parameters. %s' % msg)
    else:
        raise
    
def FromApi(func, doc=None, format='json', request_handler=None):
    '''A decorator which exposes a function ``func`` as an rpc function.

:parameter func: The function to expose.
:parameter doc: Optional doc string. If not provided the doc string of
    ``func`` will be used.
:parameter format: Optional output format.
:parameter request_handler: function which takes ``request``, ``format`` and
     ``kwargs`` and return a new ``kwargs`` to be passed to
     ``func``. It can be used to add additional parameters based
     on request and format.'''
    def _(self, *args, **kwargs):
        request = args[0]
        if request_handler:
            kwargs = request_handler(request, format, kwargs)
        request.format = kwargs.pop('format', format)
        try:
            return func(*args, **kwargs)
        except TypeError:
            rpcerror(func, args, kwargs)
        
    _.__doc__ = doc or func.__doc__
    _.__name__ = func.__name__
    _.FromApi = True
    return _
