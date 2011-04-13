import sys
import logging
import inspect
from datetime import datetime

from .exceptions import NoSuchFunction

__all__ = ['Handler']


class RpcResponse(object):
    __slots__ = ('handler','path','func')
    
    def __init__(self, handler, path, func):
        self.handler = handler
        self.path = path
        self.func = func
    
    @property    
    def name(self):
        return self.func.__name__
    
    def __repr__(self):
        return self.func.__name__
        
    def __call__(self, request, *args, **kwargs):
        log = self.handler.log
        log.info('Requesting function "{0}"'.format(self.path))
        try:
            deco = self.handler.wrap_function_decorator
            return deco(self, request, *args, **kwargs)
        except Exception as e:
            log.critical('Unhandled exception %s: %s' % (e.__class__.__name__,e),
                         exc_info=sys.exc_info)
            raise
    

class MetaRpcHandler(type):
    '''A metaclass for rpc handlers'''
    def __new__(cls, name, bases, attrs):
        make = super(MetaRpcHandler, cls).__new__
        if attrs.pop('virtual',None):
            return make(cls,name,bases,attrs)
        
        funcprefix = attrs.get('serve_as',None)
        if not funcprefix:
            for base in bases[::-1]:
                if isinstance(base, MetaRpcHandler):
                    funcprefix = base.serve_as
                    if funcprefix:
                        break
        
        rpc = {}
        if funcprefix:
            fprefix = '%s_' % funcprefix
            for key, method in list(attrs.items()):
                if hasattr(method,'__call__') and key.startswith(fprefix):
                    namefunc = key[len(fprefix):]
                    func = attrs.pop(key)
                    func.__name__ = namefunc
                    rpc[namefunc] = func
            for base in bases[::-1]:
                if hasattr(base, 'rpcfunctions'):
                    rpcbase = base.rpcfunctions
                    for key,method in rpcbase.items():
                        if not rpc.has_key(key):
                            rpc[key] = method
                        
        attrs['rpcfunctions'] = rpc
        return make(cls, name, bases, attrs)


BaseHandler = MetaRpcHandler('BaseRpcHandler',(object,),{'virtual':True})


class Handler(BaseHandler):
    '''Server Handler.
Sub-handlers for prefixed methods (e.g., system.listMethods)
can be added with putSubHandler. By default, prefixes are
separated with a '.'. Override self.separator to change this.
    '''
    serve_as   = 'rpc'
    '''Type of server and prefix to functions providing services'''
    separator  = '.'
    '''Separator between subhandlers.'''
    RESPONSE   = RpcResponse

    def __init__(self,
                 subhandlers = None,
                 http = None,
                 attrs = None,
                 **kwargs):
        self.subHandlers = {}
        self.http = http
        self.started = datetime.now()
        logger = kwargs.pop('logger',None)            
        if logger:
            self.log = logger
        else:
            self.log = logging.getLogger(self.__class__.__name__)
        if subhandlers:
            for route,handler in subhandlers.items():
                if inspect.isclass(handler):
                    handler = handler(http = self.http)
                self.putSubHandler(route, handler)
    
    def __getitem__(self, path):
        return self._getFunction(path)
    
    def get_handler(self, path):
        prefixes = path.split(self.separator)
        return self._get_handler(prefixes)
    
    def _get_handler(self, prefixes):
        handler = self
        for path in prefixes:
            handler = handler.getSubHandler(path)
            if not handler:
                raise NoSuchFunction('Could not find path {0}'.format(path))
        return handler
            
    def putSubHandler(self, prefix, handler):
        '''Add a subhandler with prefix prefix
        
:keyword prefix: a string defining the url of the subhandler
:keyword handler: the sub-handler, an instance of :class:`Handler` 
        '''
        self.subHandlers[prefix] = handler

    def getSubHandler(self, prefix):
        '''Get a subhandler at ``prefix``
        '''
        return self.subHandlers.get(prefix, None)
    
    def wrap_function_decorator(self, rpc, *args, **kwargs):
        return rpc.func(self, *args,**kwargs)
    
    def _getFunction(self, method):
        prefix_method = method.split(self.separator, 1)
        if len(prefix_method) > 1:
            # Found prefixes, get the subhandler
            method = prefix_method[-1]
            handler = self._get_handler(prefix_method[:-1])
        else:
            handler = self
        try:
            func = handler.rpcfunctions[method]
        except:
            raise NoSuchFunction("function %s not found" % method)
        return self.RESPONSE(handler,method,func)

    def invokeServiceEndpoint(self, meth, args):
        return meth(*args)

    def listFunctions(self):
        return self.rpcfunctions.keys()
    
    def wsgi(self, environ, start_response):
        '''The WSGI Handler'''
        path = environ['PATH_INFO']
        handler = self.get_handler(path)
        if handler:
            request = self.http.Request(environ)
            response = handler.serve(request)
        else:
            response = self.http.HttpResponse(status = 500)
        try:
            status_text = self.http.STATUS_CODE_TEXT[response.status_code]
        except KeyError:
            status_text = 'UNKNOWN STATUS CODE'
        status = '%s %s' % (response.status_code, status_text)
        response_headers = [(str(k), str(v)) for k, v in response.items()]
        for c in response.cookies.values():
            response_headers.append(('Set-Cookie', str(c.output(header=''))))
        start_response(status, response_headers)
        return response
        
    def serve(self, request):
        return ['<h1>Not Found</h1>'] 
        