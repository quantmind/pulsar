'''\
A Python implementation of JSON-RPC 2.0 Specification

JSON-RPC is a lightweight remote procedure call protocol.
It's designed to be simple.

A remote method is invoked by sending a request to a remote service.
The request is a single object serialized using JSON.

The specification is at http://groups.google.com/group/json-rpc/web/json-rpc-2-0
'''
import logging
import json
from timeit import default_timer

import pulsar
from pulsar import HttpClient, is_async
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils.security import gen_unique_id
from pulsar.utils.httpurl import to_string, range
from pulsar.utils.jsontools import DefaultJSONEncoder, DefaultJSONHook

from .handlers import RpcHandler, RpcRequest
from .exceptions import exception, INTERNAL_ERROR, REQUIRES_AUTHENTICATION

__all__ = ['JSONRPC', 'JsonProxy', 'LocalJsonProxy']


class JsonToolkit(object):

    @classmethod
    def dumps(cls, data, **kwargs):
        return json.dumps(data, cls=DefaultJSONEncoder, **kwargs)

    @classmethod
    def loads(cls, content):
        return json.loads(to_string(content), object_hook=DefaultJSONHook)


class JSONRPC(RpcHandler):
    '''A :class:`RpcHandler` for class for JSON-RPC services.
Design to comply with the `JSON-RPC 2.0`_ Specification.

.. _`JSON-RPC 2.0`: http://groups.google.com/group/json-rpc/web/json-rpc-2-0'''
    content_type = 'application/json'
    methods = ('post',)
    _json = JsonToolkit

    def get_method_and_args(self, data):
        '''Overrides the :meth:`RpcHandler:get_method_and_args` to obtain
method data from the JSON *data* string.'''
        if not isinstance(data, dict):
            data = self._json.loads(data)
        method  = data.get('method',None)
        params  = data.get('params',None)
        id      = data.get('id',None)
        version = data.get('jsonrpc',None)
        kwargs  = {}
        args    = ()
        if isinstance(params, dict):
            for k, v in params.items():
                kwargs[str(k)] = v
        elif params:
            args = tuple(params)
        return method, args, kwargs, id, version

    def dumps(self, id, version, result=None, error=None):
        '''Modify JSON dumps method to comply with JSON-RPC Specification 2.0'''
        res = {'id': id, "jsonrpc": version}
        if error:
            if hasattr(error, 'faultCode'):
                code = error.faultCode
                msg = getattr(error, 'faultString', str(error))
            else:
                msg = str(error)
                if getattr(error, 'status', None) == 403:
                    code = REQUIRES_AUTHENTICATION
                else:
                    code = INTERNAL_ERROR
            res['error'] =  {'code': code,
                             'message': msg,
                             'data': getattr(error,'data','')}
        else:
            res['result'] = result
        return self._json.dumps(res)


class JsonProxy(object):
    '''A python Proxy class for :class:`JSONRPC` Servers.

:param url: server location
:param version: JSONRPC server version. Default ``2.0``
:param id: optional request id, generated if not provided. Default ``None``.
:param data: Extra data to include in all requests. Default ``None``.
:param http: optional http opener. If provided it must have the ``request``
    method available which must be of the form::

        http.request(url, body=..., method=...)

    Default ``None``.

Lets say your RPC server is running at ``http://domain.name.com/``::

    >>> a = JsonProxy('http://domain.name.com/')
    >>> a.add(3,4)
    7
    >>> a.ping()
    'pong'

'''
    separator  = '.'
    rawprefix  = 'raw'
    default_version = '2.0'
    default_timeout = 3
    _json = JsonToolkit

    def __init__(self, url, name=None, version=None, id=None, data=None,
                 **kwargs):
        self.__url = url
        self.__name = name
        self.__version = version or self.__class__.default_version
        self.__id = id
        self.__data = data if data is not None else {}
        self.local = AttributeDictionary()
        self.setup(**kwargs)

    def setup(self, http=None, timeout=None, **kwargs):
        if not http:
            timeout = timeout if timeout is not None else self.default_timeout
            http = HttpClient(timeout=timeout, **kwargs)
        self.local.http = http

    @property
    def url(self):
        return self.__url

    @property
    def http(self):
        return self.local.http

    @property
    def path(self):
        return self.__name

    def makeid(self):
        '''Can be re-implemented by your own Proxy'''
        return gen_unique_id()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        if self.__id:
            d = '%s - %s' % (self.__url,self.__id)
        else:
            d = self.__url
        return 'JSONRPCProxy(%s)' % d

    def __getattr__(self, name):
        if self.__name != None:
            name = "%s%s%s" % (self.__name, self.separator, name)
        id = self.makeid()
        return self.__class__(self.__url, name=name, version=self.__version,
                              id=id, data=self.__data, **self.local.all())

    def timeit(self, func, times, *args, **kwargs):
        '''Usefull little utility for timing responses from server. The
usage is simple::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://127.0.0.1:8060')
    >>> p.timeit('ping',10)
    0.56...
    >>> _
    '''
        r = range(times)
        func = getattr(self,func)
        start = default_timer()
        for t in r:
            func(*args, **kwargs)
        return default_timer() - start

    def _get_data(self, *args, **kwargs):
        func_name = self.__name
        fs = func_name.split('_')
        raw = False
        if len(fs) > 1 and fs[0] == self.rawprefix:
            raw = True
            fs.pop(0)
            func_name = '_'.join(fs)
        params = self.get_params(*args, **kwargs)
        data = {'method': func_name, 'params': params, 'id': self.__id,
                'jsonrpc': self.__version}
        return data, raw

    def __call__(self, *args, **kwargs):
        data, raw = self._get_data(*args, **kwargs)
        body = self._json.dumps(data).encode('latin-1')
        # Always make sure the content-type is application/json
        self.http.headers['content-type'] = 'application/json'
        resp = self.http.post(self.__url, data=body)
        if is_async(resp):
            return resp.add_callback(lambda r: self._end_call(r, raw))
        else:
            return self._end_call(resp, raw)
    
    def _end_call(self, resp, raw):
        content = resp.content.decode('utf-8')
        if resp.is_error:
            if 'error' in content:
                return self.loads(content)
            else:
                resp.raise_for_status()
        else:
            if raw:
                return content
            else:
                return self.loads(content)

    def get_params(self, *args, **kwargs):
        '''
        Create an array or positional or named parameters
        Mixing positional and named parameters in one
        call is not possible.
        '''
        kwargs.update(self.__data)
        if args and kwargs:
            raise ValueError('Cannot mix positional and named parameters')
        if args:
            return list(args)
        else:
            return kwargs

    def loads(self, obj):
        res = self._json.loads(obj)
        if isinstance(res, dict):
            if 'error' in res:
                error = res['error']
                code    = error['code']
                message = error['message']
                raise exception(code, message)
            else:
                return res.get('result',None)
        return res


class LocalJsonProxy(JsonProxy):
    '''A proxy class to use when accessing the rpc within the rpc application
domain.'''
    def setup(self, handler=None, environ=None, **kwargs):
        self.local.handler = handler
        self.local.environ = environ

    def __call__(self, *args, **kwargs):
        data, raw = self._get_data(*args, **kwargs)
        hnd = self.local.handler
        environ = self.local.environ
        method, args, kwargs, id, version = hnd.get_method_and_args(data)
        hnd.request(environ, method, args, kwargs, id, version)
        return environ['rpc'].process(RpcRequest(environ))
