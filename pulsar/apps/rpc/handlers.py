import sys
import inspect

from pulsar import make_async, net, NOT_DONE, PickableMixin, to_bytestring
from pulsar.utils.tools import checkarity

from .exceptions import NoSuchFunction, InvalidParams,\
                        InternalError


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


class RpcRequest(object):
    
    def __init__(self, environ, handler, method, func, args,
                kwargs, id, version):
        self.environ = environ
        self.log = handler.log
        self.handler = handler
        self.method = method
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.version = version
        self.id = id
    
    def __repr__(self):
        return self.method
    
    @property
    def actor(self):
        return self.environ.get('pulsar.actor')
    
    @property
    def content_type(self):
        return self.handler.content_type
    
    def info(self, msg):
        '''Do something with the message and request'''
        self.log.debug(msg)
        
    def critical(self, e):
        msg = 'Unhandled server exception %s: %s' % (e.__class__.__name__,e)
        self.log.critical(msg,exc_info=True)
        raise InternalError(msg)
    

class RpcResponse(object):
    __slots__ = ('request','start_response')
    
    def __init__(self, request, start_response):
        self.request = request
        self.start_response = start_response
    
    @property
    def name(self):
        return self.request.method
    
    def __repr__(self):
        return self.name
        
    def critical(self, request, id, e):
        msg = 'Unhandled server exception %s: %s' % (e.__class__.__name__,e)
        self.handler.log.critical(msg,exc_info=sys.exc_info)
        raise InternalError(msg)
    
    def __iter__(self):
        request = self.request
        handler = request.handler
        status = '200 OK'
        try:
            if not request.func:
                msg = 'Function "{0}" not available.'.format(request.method)
                raise NoSuchFunction(msg)
            try:
                result = request.func(request.handler, request, *request.args,
                                      **request.kwargs)
            except TypeError as e:
                msg = checkarity(request.func,
                                 request.args,
                                 request.kwargs,
                                 discount=2)
                if msg:
                    msg = 'Invalid Parameters in rpc function: {0}'.format(msg)
                    raise InvalidParams(msg)
                else:
                    raise
        except Exception as e:
            status = '400 Bad Request'
            result = e
            
        result = make_async(result)
        while not result.called:
            yield b''
        result=  result.result
        try:
            if isinstance(result,Exception):
                handler.log.error(str(result),exc_info=True)
                result = handler.dumps(request.id,
                                       request.version,
                                       error=result)
            else:
                result = handler.dumps(request.id,
                                       request.version,
                                       result=result)
                request.info('Successfully handled rpc function "{0}"'\
                                .format(request.method))
        except Exception as e:
            status = '500 Internal Server Error'
            result = handler.dumps(request.id,
                                   request.version,
                                   error=e)
        
        response_headers = (
                            ('Content-type', request.content_type),
                            ('Content-Length', str(len(result)))
                            )
        self.start_response(status, response_headers)
        yield to_bytestring(result)
        

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


BaseHandler = MetaRpcHandler('BaseRpcHandler',(PickableMixin,),{'virtual':True})


class RpcMiddleware(BaseHandler):
    '''A WSGI middleware for serving Remote procedure calls (RPC).

Sub-handlers for prefixed methods (e.g., system.listMethods)
can be added with :meth:`putSubHandler`. By default, prefixes are
separated with a dot. Override :attr:`separator` to change this.
    '''
    serve_as     = 'rpc'
    '''Prefix to callable providing services.'''
    separator    = '.'
    '''Separator between subhandlers.'''
    content_type = 'text/plain'
    '''Default content type.'''

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
        '''Obtain function information form ``wsgi.input``. Needs to be
implemented by subclasses. It should return a five elements tuple containing::

    method, args, kwargs, id, version
    
where ``method`` is the function name, ``args`` are non-keyworded
to pass to the function, ``kwargs`` are keyworded parameters, ``id`` is an
identifier for the client, ``version`` is the version of the RPC protocol.
    '''
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
    
    def wrap_function_decorator(self, request, *args, **kwargs):
        return request.func(rpc.handler, request, *args,**kwargs)
    
    def request(self, environ, method, args, kwargs, id, version):
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
        return RpcRequest(environ, handler, method, func, args,
                          kwargs, id, version)
        #return RpcResponse(handler,method,func)

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
        data = environ['wsgi.input'].read()
        method, args, kwargs, id, version = self.get_method_and_args(data)
        request = self.request(environ, method, args, kwargs, id, version)
        return RpcResponse(request, start_response)
        
        