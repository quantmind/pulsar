import inspect

from pulsar import HttpException
from pulsar.utils.tools import checkarity

__all__ = ['RpcHandler', 'rpc_method', 'InvalidRequest', 'InvalidParams',
           'NoSuchFunction', 'InternalError']

_exceptions = {}


def rpc_exception(cls):
    global _exceptions
    code = cls.fault_code
    _exceptions[code] = cls
    return cls


@rpc_exception
class InvalidRequest(HttpException):
    status = 400
    fault_code = -32600
    msg = 'Invalid RPC request'

    def __init__(self, msg=None, data=None):
        self.msg = msg or self.msg
        self.data = data


@rpc_exception
class ParseExcetion(InvalidRequest):
    fault_code = -32700
    msg = 'Parse error'


@rpc_exception
class NoSuchFunction(InvalidRequest):
    fault_code = -32601
    msg = 'The method does not exist'


@rpc_exception
class InvalidParams(InvalidRequest):
    fault_code = -32602
    msg = 'Invalid method parameters'


@rpc_exception
class InternalError(InvalidRequest):
    fault_code = -32603
    msg = 'Internal error'


def exception(code, msg):
    global _exceptions
    cls = _exceptions.get(code, Exception)
    raise cls(msg)


def rpc_method(func, doc=None, format='json', request_handler=None):
    '''A decorator which exposes a function ``func`` as an rpc function.

    :param func: The function to expose.
    :param doc: Optional doc string. If not provided the doc string of
        ``func`` will be used.
    :param format: Optional output format.
    :param request_handler: function which takes ``request``, ``format``
        and ``kwargs`` and return a new ``kwargs`` to be passed to ``func``.
        It can be used to add additional parameters based on request and
        format.
    '''
    def _(self, *args, **kwargs):
        request = args[0]
        if request_handler:
            kwargs = request_handler(request, format, kwargs)
        request.format = kwargs.pop('format', format)
        try:
            return func(*args, **kwargs)
        except TypeError:
            msg = checkarity(func, args, kwargs)
            if msg:
                raise InvalidParams('Invalid Parameters. %s' % msg)
            else:
                raise

    _.__doc__ = doc or func.__doc__
    _.__name__ = func.__name__
    _.FromApi = True
    return _


class MetaRpcHandler(type):
    '''A metaclass for rpc handlers.

    Add a limited ammount of magic to RPC handlers.
    '''
    def __new__(cls, name, bases, attrs):
        make = super().__new__
        if attrs.pop('virtual', None):
            return make(cls, name, bases, attrs)
        funcprefix = attrs.get('serve_as')
        if not funcprefix:
            for base in bases[::-1]:
                if isinstance(base, MetaRpcHandler):
                    funcprefix = base.serve_as
                    if funcprefix:
                        break
        if funcprefix:
            rpc = set()
            fprefix = '%s_' % funcprefix
            n = len(fprefix)
            for key, method in list(attrs.items()):
                if hasattr(method, '__call__') and key.startswith(fprefix):
                    method_name = key[n:]
                    rpc.add(method_name)
            for base in bases[::-1]:
                if hasattr(base, 'rpc_methods'):
                    rpc.update(base.rpc_methods)
            attrs['rpc_methods'] = frozenset(rpc)
        return make(cls, name, bases, attrs)


class RpcHandler(metaclass=MetaRpcHandler):
    '''Base class for classes to handle remote procedure calls.
    '''
    serve_as = 'rpc'
    '''Prefix for class methods providing remote services. Default: ``rpc``.'''
    separator = '.'
    '''HTTP method allowed by this handler.'''
    virtual = True

    def __init__(self, subhandlers=None, title=None, documentation=None):
        self._parent = None
        self.subHandlers = {}
        self.title = title or self.__class__.__name__
        self.documentation = documentation or self.__doc__
        if subhandlers:
            for prefix, handler in subhandlers.items():
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
        return self._parent is None

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

    def get_handler(self, method):
        if not method:
            raise NoSuchFunction('RPC method not supplied')
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
        if method_name in handler.rpc_methods:
            return getattr(handler, '%s_%s' % (self.serve_as, method_name))
        else:
            raise NoSuchFunction('RPC method "%s" not available.' % method)

    def listFunctions(self, prefix=''):
        for name in sorted(self.rpc_methods):
            method = getattr(self, '%s_%s' % (self.serve_as, name))
            docs = method.__doc__ or 'No docs'
            doc = {'doc': ' '.join(docs.split('\n')), 'section': prefix}
            yield '%s%s' % (prefix, name), doc
        for name, handler in self.subHandlers.items():
            pfx = '%s%s%s' % (prefix, name, self.separator)
            for f, doc in handler.listFunctions(pfx):
                yield f, doc

    def _docs(self):
        for name, data in self.listFunctions():
            link = '.. _functions-{0}:'.format(name)
            title = name
            under = (2+len(title))*'-'
            yield '\n'.join((link, '', title, under, '', data['doc'], '\n'))

    def docs(self):
        return '\n'.join(self._docs())
