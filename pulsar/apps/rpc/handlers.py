import sys
import inspect
import logging

from pulsar import log_failure, is_async, is_failure,\
                    maybe_failure, maybe_async, HttpException
from pulsar.utils.pep import to_bytes
from pulsar.utils.tools import checkarity
from pulsar.utils.structures import AttributeDictionary
from pulsar.apps.wsgi import WsgiResponse, WsgiRequest

from .decorators import rpcerror, wrap_object_call
from .exceptions import *


__all__ = ['RpcHandler']

LOGGER = logging.getLogger('pulsar.rpc')


class RPC:
    
    def __init__(self, handler, method, func, args, kwargs,
                 id=None, version=None):
        self.handler = handler
        self.method = method
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.id = id
        self.version = version
        
    def process(self, request):
        func = self.func
        if not func:
            raise NoSuchFunction('Function "%s" not available.' % self.method)
        try:
            return func(self.handler, request, *self.args, **self.kwargs)
        except TypeError as e:
            if not getattr(func, 'FromApi', False):
                rpcerror(func, self.args, self.kwargs, discount=2)
            else:
                raise
    

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


class RpcHandler(MetaRpcHandler('_RpcHandler', (object,), {'virtual': True})):
    '''The base class for rpc handlers.

.. attribute:: content_type

    Default content type. Default: ``text/plain``.

.. attribute:: charset

    The default charset for this handler. Default: ``utf-8``.
'''
    serve_as     = 'rpc'
    '''Prefix for class methods providing remote services. Default: ``rpc``.'''
    separator    = '.'
    '''Separator between :attr:`subHandlers`.'''
    content_type = 'text/plain'
    methods = ('get','post','put','head','delete','trace','connect')
    '''HTTP method allowed by this handler.'''
    charset = 'utf-8'
    _info_exceptions = (Fault,)

    def __init__(self, subhandlers=None, title=None, documentation=None):
        self._parent = None
        self.subHandlers = {}
        self.title = title or self.__class__.__name__
        self.documentation = documentation or ''
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
        raise NotImplementedError

    def __getstate__(self):
        d = self.__dict__.copy()
        if not self.isroot():
            # Avoid duplicating handlers
            d['_parent'] = True
        return d

    def __setstate__(self, state):
        self.__dict__ = state
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
        except Exception:
            func = None
        environ['rpc'] = RPC(handler, method, func, args, kwargs, id, version)

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
