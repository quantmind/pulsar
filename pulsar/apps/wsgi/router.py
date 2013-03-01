from inspect import isfunction, ismethod

from pulsar import HttpException
from pulsar.utils.httpurl import ENCODE_URL_METHODS, ENCODE_BODY_METHODS

from .route import Route
from .wsgi import WsgiRequest

__all__ = ['Router', 'route']


class RouterType(type):
    
    def __new__(cls, name, bases, attrs):
        routes = []
        for name, callable in attrs.items():
            rule_method = getattr(callable, 'rule_method', None)
            if isinstance(rule_method, tuple):
                rule, method = rule_method
                router = Router(rule, **{method: callable})
                routes.append(router)
        attrs['routes'] = routes
        return super(RouterType, cls).__new__(cls, name, bases, attrs)
    
    
class Router(RouterType('RouterBase', (object,), {})):
    '''A WSGI application which handle multiple routes.'''
    default_content_type=None
    request_class = WsgiRequest
    routes = []
    def __init__(self, rule, *routes, **handlers):
        self.route = Route(rule)
        self.routes = list(self.routes)
        self.routes.extend(routes)
        for handle, callable in handlers.items():
            if not hasattr(self, handle) and hasattr(callable, '__call__'):
                setattr(self, handle, callable)
        
    def __repr__(self):
        return self.route.__repr__()
        
    def __call__(self, environ, start_response):
        path = environ.get('PATH_INFO') or '/'
        path = path[1:]
        router_args = self.resolve(path)
        if router_args:
            router, args = router_args
            request = self.request_class(environ, start_response, args)
            method = request.method
            callable = getattr(router, method, None)
            if callable is None:
                raise HttpException(status=405,
                                    msg='Method "%s" not allowed' % method)
            return callable(request)
        
    def resolve(self, path, urlargs=None):
        urlargs = urlargs if urlargs is not None else {}
        match = self.route.match(path)
        if match is None:
            return
        if '__remaining__' in match:
            for handler in self.routes:
                view_args = handler.resolve(path, urlargs)
                if view_args is None:
                    continue
                #remaining_path = match.pop('__remaining__','')
                #urlargs.update(match)
                return view_args
        else:
            return self, match
        
    def add_method(self, method, func):
        setattr(method, func)
    

class route(object):
    
    def __init__(self, rule=None, method=None):
        '''Create a new Router'''
        self.rule = rule
        self.method = method
        
    def __call__(self, func):
        '''func could be an unbound method of a Router class or a standard
python function.'''
        bits = func.__name__.split('_')
        method = None
        if len(bits) > 1:
            m = bits[0].upper()
            if m in ENCODE_URL_METHODS or method in ENCODE_BODY_METHODS:
                method = m
                bits = bits[1:]
        method = (self.method or method or 'get').lower()
        rule = self.rule or '_'.join(bits)
        func.rule_method = (rule, method)
        return func
            
    def _get_router(self, cls, route):
        for router in cls.routers:
            if router.path == route:
                return router
        router = Router(route)
        cls.routers.append(router)
        return router