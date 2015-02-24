import sys
import json
import logging

from pulsar import AsyncObject, task, as_coroutine, new_event_loop
from pulsar.utils.string import gen_unique_id
from pulsar.utils.tools import checkarity
from pulsar.apps.wsgi import Json
from pulsar.apps.http import HttpClient

from .handlers import RpcHandler, InvalidRequest, exception


__all__ = ['JSONRPC', 'JsonProxy']


logger = logging.getLogger('pulsar.jsonrpc')


class JSONRPC(RpcHandler):
    '''An :class:`.RpcHandler` for JSON-RPC services.

    Design to comply with the `JSON-RPC 2.0`_ Specification.

    JSON-RPC is a lightweight remote procedure call protocol
    designed to be simple.
    A remote method is invoked by sending a request to a remote service,
    the request is a single object serialised using JSON.

    .. _`JSON-RPC 2.0`: http://www.jsonrpc.org/specification
    '''
    version = '2.0'

    def __call__(self, request):
        return Json(self._call(request)).http_response(request)

    @task
    def _call(self, request):
        response = request.response
        data = {}
        exc_info = None
        callable = None
        try:
            try:
                data = yield from as_coroutine(request.body_data())
            except ValueError:
                raise InvalidRequest(
                    status=415, msg='Content-Type must be application/json')
            if data.get('jsonrpc') != self.version:
                raise InvalidRequest(
                    'jsonrpc must be supplied and equal to "%s"' %
                    self.version)
            params = data.get('params')
            if isinstance(params, dict):
                args, kwargs = (), params
            else:
                args, kwargs = tuple(params or ()), {}
            #
            callable = self.get_handler(data.get('method'))
            result = yield from as_coroutine(
                callable(request, *args, **kwargs))
        except Exception as exc:
            result = exc
            exc_info = sys.exc_info()
        else:
            try:
                json.dumps(result)
            except Exception as exc:
                result = exc
                exc_info = sys.exc_info()
        #
        res = {'id': data.get('id'), "jsonrpc": self.version}
        if exc_info:
            msg = None
            code = getattr(result, 'fault_code', None)
            if not code:
                if isinstance(result, TypeError) and callable:
                    msg = checkarity(callable, args, kwargs, discount=1)
                code = -32602 if msg else -32603
            msg = msg or str(result) or 'JSON RPC exception'
            code = getattr(result, 'fault_code', code)
            if code == -32603:
                logger.error(msg, exc_info=exc_info)
            else:
                logger.warning(msg)
            error = {'code': code,
                     'message': msg,
                     'data': getattr(result, 'data', '')}
            response.status_code = getattr(result, 'status', 400)
            res['error'] = error
        else:
            res['result'] = result
        return res


class JsonCall:
    slots = ('_client', '_name')

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
        result = self._client._call(self._name, *args, **kwargs)
        if self._client.sync:
            return self._client._loop.run_until_complete(result)
        else:
            return result


class JsonProxy(AsyncObject):
    '''A python Proxy class for :class:`.JSONRPC` Servers.

    :param url: server location
    :param version: JSON-RPC server version. Default ``2.0``
    :param id: optional request id, generated if not provided.
        Default ``None``.
    :param data: Extra data to include in all requests. Default ``None``.
    :param full_response: return the full Http response rather than
        just the content.
    :param http: optional http client. If provided it must have the ``request``
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
    separator = '.'
    default_version = '2.0'
    default_timeout = 30

    def __init__(self, url, version=None, data=None,
                 full_response=False, http=None, timeout=None, sync=False,
                 loop=None, **kw):
        self.sync = sync
        self._url = url
        self._version = version or self.__class__.default_version
        self._full_response = full_response
        self._data = data if data is not None else {}
        if not http:
            timeout = timeout if timeout is not None else self.default_timeout
            if sync and not loop:
                loop = new_event_loop()
            http = HttpClient(timeout=timeout, loop=loop, **kw)
        http.headers['accept'] = 'application/json, text/*; q=0.5'
        http.headers['content-type'] = 'application/json'
        self._http = http

    @property
    def url(self):
        return self._url

    @property
    def version(self):
        return self._version

    @property
    def _loop(self):
        return self._http._loop

    def makeid(self):
        '''Can be re-implemented by your own Proxy'''
        return gen_unique_id()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__url)

    def __str__(self):
        return self.__repr__()

    def __getattr__(self, name):
        return JsonCall(self, name)

    def _call(self, name, *args, **kwargs):
        data = self._get_data(name, *args, **kwargs)
        body = json.dumps(data).encode('utf-8')
        resp = yield from self._http.post(self._url, data=body)
        if self._full_response:
            return resp
        else:
            content = resp.decode_content()
            if resp.is_error:
                if 'error' not in content:
                    resp.raise_for_status()
            return self.loads(content)

    def _get_data(self, func_name, *args, **kwargs):
        id = self.makeid()
        params = self.get_params(*args, **kwargs)
        data = {'method': func_name, 'params': params, 'id': id,
                'jsonrpc': self._version}
        return data

    def get_params(self, *args, **kwargs):
        '''
        Create an array or positional or named parameters
        Mixing positional and named parameters in one
        call is not possible.
        '''
        kwargs.update(self._data)
        if args and kwargs:
            raise ValueError('Cannot mix positional and named parameters')
        if args:
            return list(args)
        else:
            return kwargs

    def loads(self, obj):
        if isinstance(obj, dict):
            if 'error' in obj:
                error = obj['error']
                raise exception(error.get('code'), error.get('message'))
            else:
                return obj.get('result')
        return obj
