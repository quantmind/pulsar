import sys
import inspect

from pulsar import make_async, net, NOT_DONE
from pulsar.utils.tools import checkarity

from .exceptions import NoSuchFunction, InvalidParams, InternalError


__all__ = ['RpcMiddleware']


def wrap_object_call(fname,namefunc):
    
    def _(self,*args,**kwargs):
        f = getattr(self,fname)
        return f(*args,**kwargs)
    
    _.__name__ = namefunc
    return _


def wrap_function_call(func,namefunc):
    
    def _(self,*args,**kwargs):
        return func(self,*args,**kwargs)
    
    _.__name__ = namefunc
    return _


class RpcResponse(object):
    __slots__ = ('handler','path','func')
    
    def __init__(self, handler, path, func):
        self.handler = handler
        self.path = path
        self.func = func
    
    @property
    def name(self):
        return self.func.__name__
    
    @property
    def content_type(self):
        return self.handler.content_type
    
    def __repr__(self):
        return self.func.__name__
        
    def info(self, request, id, msg, ok = True):
        '''Do something with the message and request'''
        self.handler.log.info(msg)
        
    def critical(self, request, id, e):
        msg = 'Unhandled server exception %s: %s' % (e.__class__.__name__,e)
        self.handler.log.critical(msg,exc_info=sys.exc_info)
        raise InternalError(msg)
    
    def __call__(self, request, start_response, *args, **kwargs):
        try:
            id = request.environ['pulsar.rpc.id']
            if not self.func:
                msg = 'Function "{0}" not available.'.format(self.path)
                self.info(request,id,msg,False)
                raise NoSuchFunction(msg)
            try:
                deco = self.handler.wrap_function_decorator
                result = deco(self, request, *args, **kwargs)
            except TypeError as e:
                msg = checkarity(self.func,args,kwargs,discount=2)
                if msg:
                    self.info(request,id,'Invalid Parameters in rpc function: {0}'.format(msg),False)
                    raise InvalidParams(msg)
                else:
                    self.critical(request,id,e)
            except Exception as e:
                self.critical(request,id,e)
        except Exception as e:
            result = e
            
        result = make_async(result)
        while not result.called:
            yield b''
        yield self._end(request,start_response,result.result)
            
    def _end(self, request, start_response, result):
        id = request.environ['pulsar.rpc.id']
        version = request.environ['pulsar.rpc.version']
        #status = '400 Bad Request'
        status = '200 OK'
        try:
            if isinstance(result,Exception):
                self.handler.log.error(str(result),exc_info=True)
                result = self.handler.dumps(id,version,error=result)
            else:
                result = self.handler.dumps(id,version,result=result)
                self.info(request,id,'Successfully handled rpc function "{0}"'\
                                .format(self.path))
        except Exception as e:
            result = self.handler.dumps(id,version,error=e)
        
        response_headers = (
                            ('Content-type', self.content_type),
                            ('Content-Length', str(len(result)))
                            )
        start_response(status, response_headers)
        return result
        

class MetaRpcHandler(type):
    '''A metaclass for rpc handlers.
Add a limited ammount of magic to RPC handlers.'''
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
                    if not inspect.isfunction(func):
                        key = '_{0}'.format(key)
                        attrs[key] = func
                        func = wrap_object_call(key,namefunc)
                    else:
                        func = wrap_function_call(func,namefunc)
                    rpc[namefunc] = func
            for base in bases[::-1]:
                if hasattr(base, 'rpcfunctions'):
                    rpcbase = base.rpcfunctions
                    for key,method in rpcbase.items():
                        if key not in rpc:
                            rpc[key] = method
                        
        attrs['rpcfunctions'] = rpc
        return make(cls, name, bases, attrs)


BaseHandler = MetaRpcHandler('BaseRpcHandler',(object,),{'virtual':True})


class RpcMiddleware(BaseHandler):
    '''A WSGI middleware for serving RPC.
Sub-handlers for prefixed methods (e.g., system.listMethods)
can be added with putSubHandler. By default, prefixes are
separated with a '.'. Override self.separator to change this.

.. attribute:: request_middleware
    
    instance of :class:`pulsar.utils.Middleware` or ``None``. It available
    the request will be processed by the middleware before
    processing.
    
.. attribute:: response_middleware
    
    instance of :class:`pulsar.utils.Middleware` or ``None``. It available
    the response will be processed by the middleware before returning.
    '''
    serve_as     = 'rpc'
    '''Type of server and prefix to functions providing services'''
    separator    = '.'
    content_type = 'text/plain'
    '''Separator between subhandlers.'''
    RESPONSE     = RpcResponse

    def __init__(self, subhandlers = None,
                 title = None,
                 documentation = None,
                 **kwargs):
        self.subHandlers = {}
        self.title = title or self.__class__.__name__
        self.documentation = documentation or ''
        self.log = self.getLogger(**kwargs)
        if subhandlers:
            for prefix,handler in subhandlers.items():
                if inspect.isclass(handler):
                    handler = handler(http = self.http)
                self.putSubHandler(prefix, handler)
    
    def get_method_and_args(self, data):
        raise NotImplementedError
    
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
        return self

    def getSubHandler(self, prefix):
        '''Get a subhandler at ``prefix``
        '''
        return self.subHandlers.get(prefix, None)
    
    def wrap_function_decorator(self, rpc, *args, **kwargs):
        return rpc.func(rpc.handler,*args,**kwargs)
    
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
            func = None
        return self.RESPONSE(handler,method,func)

    def invokeServiceEndpoint(self, meth, args):
        return meth(*args)

    def listFunctions(self, prefix = ''):
        for name,func in self.rpcfunctions.items():
            doc = {'doc':func.__doc__ or 'No docs','section':prefix}
            yield '{0}{1}'.format(prefix,name),doc
        for name,handler in self.subHandlers.items():
            pfx = '{0}{1}{2}'.format(prefix,name,self.separator) 
            for f,doc in handler.listFunctions(pfx):
                yield f,doc
                
    def _docs(self):
        for name, data in self.listFunctions():
            link = '.. _functions-{0}:'.format(name)
            title = name
            under = (2+len(title))*'-'
            yield '\n'.join((link,'',title,under,'',data['doc'],'\n')) 

    def docs(self):
        return '\n'.join(self._docs()) 
    
    def __call__(self, environ, start_response):
        '''The WSGI handler which consume the remote procedure call'''
        request = self.request(environ)
        data = request.data
        if data is None:
            request.read()
            yield b''
            while request.data is None:
                yield b''
            data = request.data
        data = data[0]
        method, args, kwargs, id, version = self.get_method_and_args(data)
        rpc_handler = self._getFunction(method)
        environ['pulsar.rpc.id'] = id
        environ['pulsar.rpc.version'] = version
        for data in rpc_handler(request, start_response, *args, **kwargs):
            yield data
        
        