import logging
import asyncio
from collections import namedtuple

from pulsar.api import AsyncObject
from pulsar.utils.string import gen_unique_id
from pulsar.utils.tools import checkarity
from pulsar.utils.system import json
from pulsar.apps.http import HttpClient

from .handlers import RpcHandler, InvalidRequest, exception


logger = logging.getLogger('pulsar.jsonrpc')

BatchResponse = namedtuple('BatchResponse', 'id result exception')


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

    async def __call__(self, request):
        response = request.response

        try:
            data = request.body_data()
            try:
                data = await data
            except TypeError:
                pass
        except ValueError:
            res, status = self._get_error_and_status(InvalidRequest(
                status=415, msg='Content-Type must be application/json'))
        else:
            # if it's batch request
            if isinstance(data, list):
                status = 200

                tasks = [self._call(request, each) for each in data]
                result = await asyncio.gather(*tasks)
                res = [r[0] for r in result]
            else:
                res, status = await self._call(request, data)

        response.status_code = status
        return request.json_response(res)

    async def _call(self, request, data):
        proc = None
        try:
            if (not isinstance(data, dict) or
                    data.get('jsonrpc') != self.version or
                    'id' not in data):
                raise InvalidRequest(
                    'jsonrpc must be supplied and equal to "%s"' %
                    self.version
                )
            params = data.get('params')
            if isinstance(params, dict):
                args, kwargs = (), params
            else:
                args, kwargs = tuple(params or ()), {}
            #
            proc = self.get_handler(data.get('method'))
            result = proc(request, *args, **kwargs)
            try:
                result = await result
            except TypeError:
                pass
        except Exception as exc:
            return self._get_error_and_status(exc, data, proc)

        return {
            'id': data.get('id'),
            'jsonrpc': self.version,
            'result': result
        }, 200

    def _get_error_and_status(self, exc, data, proc):
        if isinstance(exc, TypeError) and proc:
            params = data.get('params')
            if isinstance(params, dict):
                args, kwargs = (), params
            else:
                args, kwargs = tuple(params or ()), {}
            msg = checkarity(proc, args, kwargs, discount=1)
        else:
            msg = None

        rpc_id = data.get('id') if isinstance(data, dict) else None
        res = {'id': rpc_id, 'jsonrpc': self.version}
        code = getattr(exc, 'fault_code', None)
        if not code:
            code = -32602 if msg else -32603
        msg = msg or str(exc) or 'JSON RPC exception'
        if code == -32603:
            logger.exception(msg)
        else:
            logger.warning(msg)
        res['error'] = {
            'code': code,
            'message': msg,
            'data': getattr(exc, 'data', '')
        }

        return res, getattr(exc, 'status', 400)


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
    :param data: Extra data to include in all requests. Default ``None``
    :param headers: optional HTTP headers to send with requests
    :param full_response: return the full Http response rather than
        just the content.
    :param http: optional http client. If provided it must have the ``request``
        method available which must be of the form::

            http.post(url, data=..., headers=...)

        Default ``None``.
    :param encoding: encoding of the request. Default ``ascii``.

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
    default_headers = {
        'accept': 'application/json, text/*; q=0.5',
        'content-type': 'application/json'
    }

    def __init__(self, url, version=None, data=None, headers=None,
                 full_response=False, http=None, timeout=None, sync=False,
                 loop=None, encoding='ascii', **kw):
        self.sync = sync
        self.headers = headers
        self._url = url
        self._version = version or self.__class__.default_version
        self._full_response = full_response
        self._data = data if data is not None else {}
        if not http:
            timeout = timeout if timeout is not None else self.default_timeout
            if sync and not loop:
                loop = asyncio.new_event_loop()
            http = HttpClient(timeout=timeout, loop=loop, **kw)
        self.http = http
        self._encoding = encoding

    @property
    def url(self):
        return self._url

    @property
    def version(self):
        return self._version

    @property
    def _loop(self):
        return self.http._loop

    def makeid(self):
        '''Can be re-implemented by your own Proxy'''
        return gen_unique_id()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__url)

    def __str__(self):
        return self.__repr__()

    def __getattr__(self, name):
        return JsonCall(self, name)

    async def _call(self, name, *args, **kwargs):
        data = self._get_data(name, *args, **kwargs)
        is_ascii = self._encoding == 'ascii'
        data = json.dumps(data, ensure_ascii=is_ascii).encode(self._encoding)
        return await self._send(data)

    async def _send(self, data):
        headers = self.default_headers.copy()
        headers.update(self.headers or ())
        resp = await self.http.post(self._url, data=data, headers=headers)
        if self._full_response:
            return resp
        else:
            content = resp.json()
            if not resp.ok:
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

    @classmethod
    def loads(cls, obj):
        if isinstance(obj, dict):
            if 'error' in obj:
                error = obj['error']
                raise cls.exception(error.get('code'), error.get('message'))
            else:
                return obj.get('result')
        return obj

    @classmethod
    def exception(cls, code, msg):
        return exception(code, msg)


class JsonBatchProxy(JsonProxy):
    """A python Proxy class for :class:`.JSONRPC` Servers
    implementing batch protocol.

    :param url: server location
    :param version: JSON-RPC server version. Default ``2.0``
    :param id: optional request id, generated if not provided.
        Default ``None``.
    :param data: Extra data to include in all requests. Default ``None``.
    :param full_response: return the full Http response rather than
        just the response generator.
    :param http: optional http client. If provided it must have the ``request``
        method available which must be of the form::

            http.request(url, body=..., method=...)

        Default ``None``.
    :return: generator that returns batch response
        (a named tuple 'id result exception'). If ``full_response`` is True,
        then returns Http response.

    Lets say your RPC server is running at ``http://domain.name.com/``::

        >>> a = JsonBatchProxy('http://domain.name.com/')
        >>> a.add(3,4)
        'i86863002653c42278d7c5ff7506d84c7'
        >>> a.ping()
        'i71a9b79eef9b48eea2fb9d691c8e897e'

        >>> for each in (await a()):
        >>>     print(each.id, each.result, each.exception)

    """
    _batch = None

    def __len__(self):
        return len(self._batch or ())

    async def __call__(self):
        if not self._batch:
            return ()
        data = b'[' + b','.join(self._batch) + b']'
        self._batch = None
        return await self._send(data)

    def _call(self, name, *args, **kwargs):
        data = self._get_data(name, *args, **kwargs)
        is_ascii = self._encoding == 'ascii'
        body = json.dumps(data, ensure_ascii=is_ascii).encode(self._encoding)
        if not self._batch:
            self._batch = []
        self._batch.append(body)
        return data['id']

    @classmethod
    def loads(cls, obj):
        if not isinstance(obj, list):
            obj = [obj]

        for resp in obj:
            try:
                yield BatchResponse(
                    id=resp['id'],
                    result=JsonProxy.loads(resp),
                    exception=None
                )
            except Exception as err:
                yield BatchResponse(
                    id=resp['id'], result=None, exception=err
                )
