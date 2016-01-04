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
import io
import logging
from functools import partial
import asyncio

import pulsar
from pulsar import HttpException, task, async, add_errback, as_coroutine
from pulsar.apps import wsgi, http
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
        return http.HttpClient(decompress=False, store_cookies=False)

    def __call__(self, environ, start_response):
        uri = environ['RAW_URI']
        logger.debug('new request for %r' % uri)
        if not uri or uri.startswith('/'):  # No proper uri, raise 404
            raise HttpException(status=404)
        request_headers = self.request_headers(environ)
        method = environ['REQUEST_METHOD']
        data = None
        response = TunnelResponse(environ, start_response)
        if method in ENCODE_BODY_METHODS:
            data = DataIterator(response)
        res = self.http_client.request(method,
                                       uri,
                                       data=data,
                                       headers=request_headers,
                                       version=environ['SERVER_PROTOCOL'],
                                       pre_request=response.pre_request,
                                       stream=True)
        add_errback(async(res), response.error)
        return response

    def request_headers(self, environ):
        '''Fill request headers from the environ dictionary and
        modify them via the list of :attr:`headers_middleware`.
        The returned headers will be sent to the target uri.
        '''
        headers = Headers(kind='client')
        for k in environ:
            if k.startswith('HTTP_'):
                head = k[5:].replace('_', '-')
                headers[head] = environ[k]
        for head in ENVIRON_HEADERS:
            k = head.replace('-', '_').upper()
            v = environ.get(k)
            if v:
                headers[head] = v
        for middleware in self.headers_middleware:
            middleware(environ, headers)
        return headers


class DataIterator:

    def __init__(self, response):
        self.response = response
        self.stream = response.environ.get('wsgi.input')

    def __iter__(self):
        yield self.stream.reader.read()


############################################################################
#    RESPONSE OBJECTS
class TunnelResponse:
    '''Base WSGI Response Iterator for the Proxy server
    '''

    _started = False
    _headers = None
    _done = False

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start_response = start_response
        self.queue = asyncio.Queue()

    '''Asynchronous wsgi response for http requests
    '''

    def __iter__(self):
        while True:
            if self._done:
                try:
                    yield self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            else:
                yield async(self.queue.get())

    def error(self, exc):
        if not self._started:
            logger.error(str(exc))
            request = wsgi.WsgiRequest(self.environ)
            content_type = request.content_types.best_match(
                ('text/html', 'text/plain'))
            uri = self.environ['RAW_URI']
            msg = 'Could not find %s' % uri
            logger.info(msg=msg)
            if content_type == 'text/html':
                html = wsgi.HtmlDocument(title=msg)
                html.body.append('<h1>%s</h1>' % msg)
                data = html.render()
                resp = wsgi.WsgiResponse(504, data, content_type='text/html')
            elif content_type == 'text/plain':
                resp = wsgi.WsgiResponse(504, msg, content_type='text/html')
            else:
                resp = wsgi.WsgiResponse(504, '')
            self.start_response(resp.status, resp.get_headers())
            self._done = True
            self.queue.put_nowait(resp.content[0])

    def pre_request(self, response, exc=None):
        '''Start the tunnel.

        This is a callback fired once a connection with upstream server is
        established.

        Write back to the client the 200 Connection established message.
        After this the downstream connection consumer will upgrade to the
        DownStreamTunnel.
        '''
        self._started = True
        #
        upstream = response.connection
        dostream = self.environ['pulsar.connection']
        #
        dostream.upgrade(partial(StreamTunnel, upstream))
        upstream.upgrade(partial(StreamTunnel, dostream))
        self.start_response('200 Connection established', [])
        # send empty byte so that headers are sent
        self.queue.put_nowait(b'')
        # Done with this wsgi response, the rest of the communication is done
        # by the StreamTunnel consumer
        self._done = True
        response.abort_request()


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

    def __init__(self, tunnel, loop=None):
        super().__init__(loop)
        self.tunnel = tunnel

    def connection_made(self, connection):
        self.logger.debug('Tunnel connection %s made', connection)
        connection.bind_event('connection_lost', self._close_tunnel)

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
