import sys
import inspect

from pulsar import LogginMixin, to_bytes, is_failure, log_failure, is_async,\
                    as_failure, maybe_async, HttpException
from pulsar.utils.tools import checkarity
from pulsar.apps.wsgi import WsgiResponse, WsgiResponseGenerator

from .decorators import callrpc, wrap_object_call
from .exceptions import *


__all__ = ['RpcHandler', 'RpcMiddleware']


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
    def user(self):
        return self.environ.get('user')

    @property
    def content_type(self):
        return self.handler.content_type

    def process(self):
        if not self.func:
            raise NoSuchFunction('Function "%s" not available.' % self.method)
        return callrpc(self.func, self.handler, self, self.args, self.kwargs)


class ResponseGenerator(WsgiResponseGenerator):
    '''Asynchronous response generator invoked by the djpcms WSGI middleware'''
    def __init__(self, request, start_response):
        self.request = request
        super(ResponseGenerator, self).__init__(request.environ, start_response)

    def __iter__(self):
        request = self.request
        status_code = 200
        try:
            result = request.process()
        except Exception as e:
            result = as_failure(e)
        handler = request.handler
        result = maybe_async(result)
        while is_async(result):
            yield b''
            result = maybe_async(result)
        try:
            if is_failure(result):
                e = result.trace[1]
                status_code = getattr(e, 'status', 400)
                log_failure(result)
                result = handler.dumps(request.id,
                                       request.version,
                                       error=e)
            else:
                result = handler.dumps(request.id,
                                       request.version,
                                       result=result)
        except Exception as e:
            handler.log.error('Could not serialize', exc_info=True)
            status_code = 500
            result = handler.dumps(request.id,
                                   request.version,
                                   error=e)
        content = to_bytes(result)
        response = WsgiResponse(status_code, content,
                                content_type=request.content_type)
        for c in self.start(response):
            yield c


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
                        func = wrap_object_call(key, namefunc)
                    rpc[namefunc] = func
            for base in bases[::-1]:
                if hasattr(base, 'rpcfunctions'):
                    rpcbase = base.rpcfunctions
                    for key,method in rpcbase.items():
                        if key not in rpc:
                            rpc[key] = method

        attrs['rpcfunctions'] = rpc
        return make(cls, name, bases, attrs)


BaseHandler = MetaRpcHandler('BaseRpcHandler',(LogginMixin,),{'virtual':True})


class RpcHandler(BaseHandler):
    '''The base class for rpc handlers.
Sub-handlers for prefixed methods (e.g., system.listMethods)
can be added with :meth:`putSubHandler`. By default, prefixes are
separated with a dot. Override :attr:`separator` to change this.

.. attribute:: content_type
    Default content type.

    Default: ``"text/plain"``.

.. attribute:: charset

    The default chrset for this handler. Can be overritten by function
    charset attribute.

    Default: 'utf-8'.
'''
    serve_as     = 'rpc'
    '''Prefix for class methods providing remote services. Default: ``rpc``.'''
    separator    = '.'
    '''Separator between subhandlers.'''
    content_type = 'text/plain'
    '''charset'''
    charset = 'utf-8'
    '''Default content type. Default: ``"text/plain"``.'''
    _info_exceptions = (Fault,)

    def __init__(self, subhandlers=None, title=None, documentation=None,
                 **kwargs):
        self._parent = None
        self.subHandlers = {}
        self.title = title or self.__class__.__name__
        self.documentation = documentation or ''
        self.setlog(**kwargs)
        if subhandlers:
            for prefix,handler in subhandlers.items():
                if inspect.isclass(handler):
                    handler = handler()
                self.putSubHandler(prefix, handler)

    @property
    def parent(self):
        '''The parent :class:`RpcHandler` or ``None`` if this
is the root handler.'''
        return self._parent

    @property
    def root(self):
        '''The root :class:`RpcHandler` or ``self`` if this
is the root handler.'''
        return self._parent.root if self._parent is not None else self

    def isroot(self):
        '''``True`` if this is the root handler.'''
        return self._parent == None

    def get_method_and_args(self, data):
        '''Obtain function information form ``wsgi.input``. Needs to be
implemented by subclasses. It should return a five elements tuple containing::

    method, args, kwargs, id, version

where ``method`` is the function name, ``args`` are positional parameters
for ``method``, ``kwargs`` are keyworded parameters for ``method``,
``id`` is an identifier for the client,
``version`` is the version of the RPC protocol.
    '''
        raise NotImplementedError()

    def __getstate__(self):
        d = super(RpcHandler,self).__getstate__()
        if not self.isroot():
            # Avoid duplicating handlers
            d['_parent'] = True
        return d

    def __setstate__(self, state):
        super(RpcHandler,self).__setstate__(state)
        for handler in self.subHandlers.values():
            handler._parent = self

    def putSubHandler(self, prefix, handler):
        '''Add a sub :class:`RpcHandler` with prefix ``prefix``.

:keyword prefix: a string defining the prefix of the subhandler
:keyword handler: the sub-handler.
        '''
        self.subHandlers[prefix] = handler
        handler._parent = self
        return self

    def getSubHandler(self, prefix):
        '''Get a sub :class:`RpcHandler` at ``prefix``.'''
        return self.subHandlers.get(prefix)

    def wrap_function_decorator(self, request, *args, **kwargs):
        return request.func(rpc.handler, request, *args,**kwargs)

    def request(self, environ, method, args, kwargs, id, version):
        bits = method.split(self.separator, 1)
        handler = self
        method_name = bits[-1]
        for bit in bits[:-1]:
            subhandler = handler.getSubHandler(bit)
            if subhandler is None:
                method_name = method
                break
            else:
                handler = subhandler
        try:
            func = handler.rpcfunctions[method_name]
        except:
            func = None
        return RpcRequest(environ, handler, method, func, args,
                          kwargs, id, version)

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


class RpcMiddleware(object):
    '''A WSGI_ middleware for serving an :class:`RpcHandler`.

.. attribute:: handler

    The :class:`RpcHandler` to serve.

.. attribute:: path

    The path where the RPC is located

    Default ``None``

.. _WSGI: http://www.wsgi.org/
'''
    methods = ('get','post','put','head','delete','trace','connect')

    def __init__(self, handler, path=None, methods=None):
        self.handler = handler
        self.path = path or '/'
        self.methods = methods or self.methods

    def __str__(self):
        return self.path

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,self)

    @property
    def route(self):
        return self.path

    def __call__(self, environ, start_response):
        '''The WSGI handler which consume the remote procedure call'''
        path = environ['PATH_INFO'] or '/'
        if path == self.path:
            method = environ['REQUEST_METHOD'].lower()
            if method not in self.methods:
                raise HttpException(status=405)
            data = environ['wsgi.input'].read()
            hnd = self.handler
            method, args, kwargs, id, version = hnd.get_method_and_args(data)
            request = hnd.request(environ, method, args, kwargs, id, version)
            return ResponseGenerator(request, start_response)

