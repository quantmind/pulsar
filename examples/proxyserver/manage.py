'''An asynchronous multi-process `HTTP proxy server`_. It works for both
``http`` and ``https`` (tunneled) requests.


Managing Headers
=====================
It is possible to add middleware to manipulate the original request headers.
If the header middleware is
an empty list, the proxy passes requests and responses unmodified.
This is an implementation for a forward-proxy which can be used
to retrieve any type of source from the Internet.

To run the server::

    python manage.py

An header middleware is a callable which receives the wsgi *environ* and
the list of request *headers*. By default the example uses:

.. autofunction:: x_forwarded_for

To run with different headers middleware create a new script and do::

    from proxyserver.manage import server

    if __name__ == '__main__':
        server(headers_middleware=[...]).start()

Implemenation
===========================

.. autoclass:: ProxyServerWsgiHandler
   :members:
   :member-order:


.. _`HTTP proxy server`: http://en.wikipedia.org/wiki/Proxy_server
'''
import logging
from functools import partial
import asyncio

import pulsar
from pulsar import HttpException, ensure_future
from pulsar.apps import wsgi, http
from pulsar.apps.http.plugins import noerror
from pulsar.utils.httpurl import Headers, ENCODE_BODY_METHODS
from pulsar.utils.log import LocalMixin, local_property


SERVER_SOFTWARE = 'Pulsar-proxy-server/%s' % pulsar.version
ENVIRON_HEADERS = ('content-type', 'content-length')
USER_AGENT = SERVER_SOFTWARE
logger = logging.getLogger('pulsar.proxyserver')


def x_forwarded_for(environ, headers):
    '''Add *x-forwarded-for* header'''
    headers.add_header('x-forwarded-for', environ['REMOTE_ADDR'])


class ProxyServerWsgiHandler(LocalMixin):
    '''WSGI middleware for an asynchronous proxy server.

    To perform processing on headers you can pass a list of
    ``headers_middleware``.
    An headers middleware is a callable which accepts two parameters, the wsgi
    *environ* dictionary and the *headers* container.
    '''
    def __init__(self, headers_middleware=None):
        self.headers_middleware = headers_middleware or []

    @local_property
    def http_client(self):
        '''The :class:`.HttpClient` used by this proxy middleware for
        accessing upstream resources'''
        client = http.HttpClient(decompress=False, store_cookies=False)
        client.headers.clear()
        return client

    def __call__(self, environ, start_response):
        uri = environ['RAW_URI']
        logger.debug('new request for %r' % uri)
        if not uri or uri.startswith('/'):  # No proper uri, raise 404
            raise HttpException(status=404)
        response = TunnelResponse(self, environ, start_response)
        ensure_future(response.request())
        return response.future


############################################################################
#    RESPONSE OBJECTS
class TunnelResponse:
    '''Base WSGI Response Iterator for the Proxy server
    '''
    def __init__(self, wsgi, environ, start_response):
        self.wsgi = wsgi
        self.environ = environ
        self.start_response = start_response
        self.future = asyncio.Future()

    async def request(self):
        '''Perform the Http request to the upstream server
        '''
        request_headers = self.request_headers()
        environ = self.environ
        method = environ['REQUEST_METHOD']
        data = None
        if method in ENCODE_BODY_METHODS:
            data = DataIterator(self)
        http = self.wsgi.http_client
        try:
            await http.request(method,
                               environ['RAW_URI'],
                               data=data,
                               headers=request_headers,
                               version=environ['SERVER_PROTOCOL'],
                               pre_request=self.pre_request)
        except Exception as exc:
            self.error(exc)

    def request_headers(self):
        '''Fill request headers from the environ dictionary and
        modify them via the list of :attr:`headers_middleware`.
        The returned headers will be sent to the target uri.
        '''
        headers = Headers(kind='client')
        for k in self.environ:
            if k.startswith('HTTP_'):
                head = k[5:].replace('_', '-')
                headers[head] = self.environ[k]
        for head in ENVIRON_HEADERS:
            k = head.replace('-', '_').upper()
            v = self.environ.get(k)
            if v:
                headers[head] = v
        for middleware in self.wsgi.headers_middleware:
            middleware(self.environ, headers)
        return headers

    def error(self, exc):
        if self.future.done():
            self.future.set_exception(exc)
        else:
            logger.error(str(exc))

    @noerror
    def pre_request(self, response, exc=None):
        '''Start the tunnel.

        This is a callback fired once a connection with upstream server is
        established.
        '''
        if response.request.method == 'CONNECT':
            # proxy - server connection
            upstream = response.connection
            # client - proxy connection
            dostream = self.environ['pulsar.connection']
            # Upgrade downstream connection
            dostream.upgrade(partial(StreamTunnel, upstream))
            # Upgrade upstream connection
            upstream.upgrade(partial(StreamTunnel, dostream))
            self.start_response('200 Connection established', [])
            # send empty byte so that headers are sent
            self.future.set_result([b''])
            response.abort_request()
        else:
            response.bind_event('data_processed', self.data_processed)
            response.bind_event('post_request', self.post_request)

    def data_processed(self, response, data=None, **kw):
        self.environ['pulsar.connection'].write(data)

    def post_request(self, _, exc=None):
        self.future.set_exception(wsgi.AbortWsgi())


class DataIterator:

    def __init__(self, response):
        self.response = response
        self.stream = response.environ.get('wsgi.input')

    def __iter__(self):
        yield self.stream.reader.read()


class StreamTunnel(pulsar.ProtocolConsumer):
    ''':class:`.ProtocolConsumer` handling encrypted messages from
    downstream client and upstream server.

    This consumer is created as an upgrade of the standard Http protocol
    consumer.

    .. attribute:: tunnel

        Connection to the downstream client or upstream server.
    '''
    headers = None
    status_code = None
    http_request = None

    def __init__(self, tunnel, loop=None):
        super().__init__(loop)
        self.tunnel = tunnel

    def connection_made(self, connection):
        self.logger.debug('Tunnel connection %s made', connection)
        connection.bind_event('connection_lost', self._close_tunnel)
        if self.http_request:
            self.start(self.http_request)

    def data_received(self, data):
        try:
            return self.tunnel.write(data)
        except Exception:
            if not self.tunnel.closed:
                raise

    def _close_tunnel(self, arg, exc=None):
        if not self.tunnel.closed:
            self._loop.call_soon(self.tunnel.close)


def server(name='proxy-server', headers_middleware=None,
           server_software=None, **kwargs):
    '''Function to Create a WSGI Proxy Server.'''
    if headers_middleware is None:
        headers_middleware = [x_forwarded_for]
    wsgi_proxy = ProxyServerWsgiHandler(headers_middleware)
    kwargs['server_software'] = server_software or SERVER_SOFTWARE
    return wsgi.WSGIServer(wsgi_proxy, name=name, **kwargs)


if __name__ == '__main__':
    server().start()
