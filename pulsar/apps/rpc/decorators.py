'''Decorator for RPC functions operating with users and model instances
'''
from pulsar.utils.tools import checkarity

from .exceptions import InvalidParams


class AuthenticationException(Exception):
    pass


class InstanceNotAvailable(Exception):
    pass


def wrap_object_call(fname, namefunc):
    def _(self,*args,**kwargs):
        f = getattr(self,fname)
        return f(*args,**kwargs)
    _.__name__ = namefunc
    return _

def requires_authentication(f):
    '''Decorator for class view functions requiring authentication.'''
    def wrapper(rpc, request, **kwargs):
        if not rpc.http.is_authenticated(request):
            raise AuthenticationException('Not authenticated')
        return f(rpc, request, **kwargs)
    return wrapper


class requires_instance(object):
    def __init__(self, getter):
        self.get = getter
    
    def __call__(self, f):
        
        def wrapper(rpc, request, *args, **kwargs):
            obj,user = self.get(**kwargs)
            if not obj:
                raise InstanceNotAvailable('Object does not exists')
            return f(request,obj,user,**kwargs)
        
        return wrapper    
    
    
class requires_owner(requires_instance):
    '''Decorator for class view used to check if an authenticated request
can manipulate an instance obtained from the getter function.
For example::

    def getinstance(id=None,**kwargs):
        try:
            instance = ...
            user = get_instance_owner(instance)
            return instance,user
        except:
            return None,None
        
    @requires_owner(getinstance)
    def jsonrpc_rename(request, instance, user, **kwargs):
        ...
'''
    def __call__(self, f):
        @requires_authentication
        def wrapper(rpc, request, **kwargs):
            obj,user = self.get(kwargs)
            if not obj:
                raise InstanceNotAvailable('Object does not exists')
            if user == request.user:
                return f(rpc,request,obj,user,**kwargs)
            else:
                raise AuthenticationException('user {0} does not own {1}'.format(user,obj))
        
        return wrapper
    
def rpcerror(func, args, kwargs, discount=0):
    msg = checkarity(func, args, kwargs, discount=discount)
    if msg:
        raise InvalidParams('Invalid Parameters. %s' % msg)
    else:
        raise
    
def FromApi(func, doc=None, format='json', request_handler=None):
    '''\
A decorator which exposes a function ``func`` as an rpc function.

:parameter func: The function to expose.
:parameter doc: Optional doc string. If not provided the doc string of
    ``func`` will be used.
:parameter format: Optional output format. Only used if ``request_handler``
    is specified.
:parameter request_handler: function which takes ``request``, ``format`` and
     ``kwargs`` and return a new ``kwargs`` to be passed to
     ``func``. It can be used to add additional parameters based
     on request and format.'''
    def _(self, request, *args, **kwargs):
        if request_handler:
            kwargs = request_handler(request, format, kwargs)
        try:
            return func(*args, **kwargs)
        except TypeError:
            rpcerror(func, args, kwargs)
        
    _.__doc__ = doc or func.__doc__
    _.__name__ = func.__name__
    _.FromApi = True
    return _

def callrpc(func, handler, self, args, kwargs):
    try:
        return func(self.handler, self, *self.args, **self.kwargs)
    except TypeError as e:
        if not getattr(func, 'FromApi', False):
            rpcerror(func, args, kwargs, discount=2)
        else:
            raise