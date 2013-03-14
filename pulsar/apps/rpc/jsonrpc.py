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
from functools import partial
from timeit import default_timer

import pulsar
from pulsar import is_async, multi_async
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils.security import gen_unique_id
from pulsar.utils.pep import to_string, range
from pulsar.utils.jsontools import DefaultJSONEncoder, DefaultJSONHook
from pulsar.apps.http import HttpClient

from .handlers import RpcHandler
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

    def __call__(self, request):
        data = request.body_data
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
    
    def _call(self, request):
        if request.method.lower() not in self.handler.methods:
            raise HttpException(status=405, msg='Method "%s" not allowed' %\
                                method)
        
        data = environ['wsgi.input'].read()
        
        hnd = self.handler
        method, args, kwargs, id, version = hnd.get_method_and_args(request)
        hnd.request(environ, method, args, kwargs, id, version)
        rpc = environ['rpc']
        status_code = 200
        try:
            result = rpc.process(request)
        except Exception as e:
            result = maybe_failure(e)
        handler = rpc.handler
        result = maybe_async(result)
        while is_async(result):
            yield b''
            result = maybe_async(result)
        try:
            if is_failure(result):
                e = result.trace[1]
                status_code = getattr(e, 'status', 400)
                log_failure(result)
                result = handler.dumps(rpc.id, rpc.version, error=e)
            else:
                result = handler.dumps(rpc.id, rpc.version, result=result)
        except Exception as e:
            LOGGER.error('Could not serialize', exc_info=True)
            status_code = 500
            result = handler.dumps(rpc.id, rpc.version, error=e)
        response = request.response
        response.status_code = status_code
        response.content = result
        response.content_type = handler.content_type
        for c in response.start():
            yield c


class JsonCall:
    
    dlots = ('_client', '_name')
    
    def __init__(self, client, name):
        self._client = client
        self._name = name
        
    def __repr__(self):
        return self._name
    __str__ = __repr__
    
    @property
    def url(self):
        return self._client.url
    
    @property
    def name(self):
        return self._name
    
    def __getattr__(self, name):
        name = "%s%s%s" % (self._name, self._client.separator, name)
        return self.__class__(self._client, name)
        
    def __call__(self, *args, **kwargs):
        return self._client._call(self._name, *args, **kwargs)
    
        
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

    def __init__(self, url, version=None, data=None, full_response=False, **kw):
        self.__url = url
        self.__version = version or self.__class__.default_version
        self._full_response = full_response
        self.__data = data if data is not None else {}
        self.local = AttributeDictionary()
        self.setup(**kw)

    def setup(self, http=None, timeout=None, **kw):
        if not http:
            timeout = timeout if timeout is not None else self.default_timeout
            http = HttpClient(timeout=timeout, **kw)
        self.local.http = http

    @property
    def url(self):
        return self.__url

    @property
    def http(self):
        return self.local.http

    def makeid(self):
        '''Can be re-implemented by your own Proxy'''
        return gen_unique_id()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__url)

    def __str__(self):
        return self.__repr__()
    
    def __getattr__(self, name):
        return JsonCall(self, name)

    def timeit(self, func, times, *args, **kwargs):
        '''Usefull little utility for timing responses from server. The
usage is simple::

    >>> from pulsar.apps import rpc
    >>> p = rpc.JsonProxy('http://127.0.0.1:8060')
    >>> p.timeit('ping',10)
    0.56...
    >>> _
    '''
        func = getattr(self, func)
        return multi_async((func(*args, **kwargs) for t in range(times)))

    def _call(self, name, *args, **kwargs):
        data, raw = self._get_data(name, *args, **kwargs)
        body = self._json.dumps(data).encode('utf-8')
        # Always make sure the content-type is application/json
        self.http.headers['content-type'] = 'application/json'
        resp = self.http.post(self.url, data=body)
        if self._full_response:
            return resp
        elif hasattr(resp, 'on_finished'):
            return resp.on_finished.add_callback(partial(self._end_call, raw))
        else:
            return self._end_call(raw, resp)
        
    def _end_call(self, raw, resp):
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
        
    def _get_data(self, func_name, *args, **kwargs):
        id = self.makeid()
        fs = func_name.split('_')
        raw = False
        if len(fs) > 1 and fs[0] == self.rawprefix:
            raw = True
            fs.pop(0)
            func_name = '_'.join(fs)
        params = self.get_params(*args, **kwargs)
        data = {'method': func_name, 'params': params, 'id': id,
                'jsonrpc': self.__version}
        return data, raw

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

    def _call(self, name, *args, **kwargs):
        data, raw = self._get_data(name, *args, **kwargs)
        hnd = self.local.handler
        environ = self.local.environ
        method, args, kwargs, id, version = hnd.get_method_and_args(data)
        hnd.request(environ, method, args, kwargs, id, version)
        return environ['rpc'].process(Request(environ))