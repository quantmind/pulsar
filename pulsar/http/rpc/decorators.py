'''Decorator for RPC functions operating with users and model instances
'''

class AuthenticationException(Exception):
    pass


class InstanceNotAvailable(Exception):
    pass


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
    
    
    
def FromApi(func, doc = None, format = 'json'):
    
    def _(self,request,**kwargs):
        res = func(**kwargs)
        return res
        
    _.__doc__ = doc or func.__doc__
    
    return _
