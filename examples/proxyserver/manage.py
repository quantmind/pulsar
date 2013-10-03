'''An asynchronous multi-process `HTTP proxy server`_


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
import sys
from functools import partial

try:
    import pulsar
except ImportError:
    sys.path.append('../../')
    import pulsar

from pulsar import HttpException, Queue, Empty, Deferred
from pulsar.apps import wsgi, http
from pulsar.utils.httpurl import Headers
from pulsar.utils.log import LocalMixin, local_property


SERVER_SOFTWARE = 'Pulsar-proxy-server/%s' % pulsar.version
ENVIRON_HEADERS = ('content-type', 'content-length')
USER_AGENT = SERVER_SOFTWARE


def x_forwarded_for(environ, headers):
    '''Add *x-forwarded-for* header'''
    headers.add_header('x-forwarded-for', environ['REMOTE_ADDR'])


class user_agent:
    '''Override user-agent header'''
    def __init__(self, agent):
        self.agent = agent

    def __call__(self, environ, headers):
        headers['user-agent'] = self.agent


class ProxyServerWsgiHandler(LocalMixin):
    '''WSGI middleware for an asynchronous proxy server. To perform
processing on headers you can pass a list of ``headers_middleware``.
An headers middleware is a callable which accepts two parameters, the wsgi
*environ* dictionary and the *headers* container.'''
    def __init__(self, headers_middleware=None):
        self.headers_middleware = headers_middleware or []

    @local_property
    def http_client(self):
        '''The :ref:`HttpClient <'''
        return http.HttpClient(decompress=False, store_cookies=False)

    def __call__(self, environ, start_response):
        # The WSGI thing
        if environ.get('HTTP_EXPECT') == '100-continue':
            return self._call(environ, start_response)
        stream = environ.get('wsgi.input') or io.BytesIO()
        data = stream.read()
        # handle asynchronous body data
        if isinstance(data, Deferred):
            return data.add_callback(partial(self._call, environ,
                                             start_response))
        else:
            return self._call(environ, start_response, data)

    def _call(self, environ, start_response, data=None):
        uri = environ['RAW_URI']
        if not uri or uri.startswith('/'):  # No proper uri, raise 404
            raise HttpException(status=404)
        request_headers = self.request_headers(environ)
        method = environ['REQUEST_METHOD']
        response = self.http_client.request(method, uri,
                                            data=data,
                                            headers=request_headers,
                                            version=environ['SERVER_PROTOCOL'])
        #
        if method == 'CONNECT':
            return ProxyTunnel(environ, start_response, response)
        else:
            return ProxyResponse(environ, start_response, response)

    def request_headers(self, environ):
        '''Fill request headers from the environ dictionary and
modify the headers via the list of :attr:`headers_middleware`.
The returned headers will be sent to the target uri.'''
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


class ProxyResponse(object):
    '''Asynchronous wsgi response.
    '''
    _headers = None
    _done = False

    def __init__(self, environ, start_response, response):
        self.environ = environ
        self.start_response = start_response
        self.queue = Queue()
        response.on_finished.add_errback(self.error)
        self.setup(response)

    def setup(self, response):
        response.bind_event('data_processed', self.data_processed)

    def __iter__(self):
        while True:
            try:
                yield self.queue.get(wait=not self._done)
            except Empty:
                break

    def data_processed(self, response, **kw):
        '''Receive data from the requesting HTTP client.'''
        status = response.get_status()
        if status == '100 Continue':
            stream = self.environ.get('wsgi.input') or io.BytesIO()
            body = yield stream.read()
            response.transport.write(body)
        if response.parser.is_headers_complete():
            if self._headers is None:
                headers = self.remove_hop_headers(response.headers)
                self._headers = Headers(headers, kind='server')
                # start the response
                self.start_response(status, list(self._headers))
            body = response.recv_body()
            if response.parser.is_message_complete():
                self._done = True
            self.queue.put(body)

    def error(self, failure):
        '''Handle a failure.'''
        if not self._done:
            uri = self.environ['RAW_URI']
            msg = 'Oops! Could not find %s' % uri
            html = wsgi.HtmlDocument(title=msg)
            html.body.append('<h1>%s</h1>' % msg)
            data = html.render()
            resp = wsgi.WsgiResponse(504, data, content_type='text/html')
            self.start_response(resp.status, resp.get_headers(),
                                failure.exc_info)
            self._done = True
            self.queue.put(resp.content[0])

    def remove_hop_headers(self, headers):
        for header, value in headers:
            if header.lower() not in wsgi.HOP_HEADERS:
                yield header, value


class ProxyTunnel(ProxyResponse):

    def setup(self, response):
        response.bind_event('pre_request', self.connection_made)

    def connection_made(self, response):
        '''Start the tunnel.

        This is a callback fired once a connection with target server is
        established and this proxy is acting as tunnel for a TSL server.

        Write back to the client the 200 Connection established message.
        After this the downstream connection consumer will upgrade to the
        DownStreamTunnel.
        '''
        # Upgrade downstream protocol consumer
        upstream = response.connection
        downstream = self.environ['pulsar.connection']
        downstream.upgrade(partial(DownStreamTunnel, upstream))
        self.start_response('200 Connection established', [])
        self._done = True
        # send empty byte so that headers are sent
        self.queue.put(b'')
        return response


class DownStreamTunnel(pulsar.ProtocolConsumer):
    ''':class:`ProtocolConsumer` handling encrypted messages from
    downstream client.

    This consumer is created as an upgrade of the standard Http protocol
    consumer, once encrypted data arrives from the downstream client.

    .. attribute:: upstream

        Client :class:`pulsar.Connection` with the upstream server.
    '''
    def __init__(self, upstream):
        super(DownStreamTunnel, self).__init__()
        self.upstream = upstream

    def connection_made(self, connection):
        '''Upgrade the consumer of :attr:`upstream` connection.

        The upstream (proxy - endpoint) connection consumer changes from HTTP
        to the :class:`UpstreamTunnel`.
        '''
        self.upstream.upgrade(partial(UpstreamTunnel, connection), True)

    def data_received(self, data):
        # Received data from the downstream part of the tunnel.
        # Send the data to the upstream server
        self.upstream.transport.write(data)


class UpstreamTunnel(pulsar.ProtocolConsumer):
    headers = None
    status_code = None

    def __init__(self, downstream):
        super(UpstreamTunnel, self).__init__()
        self.downstream = downstream

    def data_received(self, data):
        # Got data from the upstream server.
        # Send it back to the downstream client
        self.downstream.transport.write(data)


def server(name='proxy-server', headers_middleware=None, server_software=None,
           **kwargs):
    '''Function to Create a WSGI Proxy Server.'''
    if headers_middleware is None:
        #headers_middleware = [user_agent(USER_AGENT), x_forwarded_for]
        headers_middleware = [x_forwarded_for]
    wsgi_proxy = ProxyServerWsgiHandler(headers_middleware)
    kwargs['server_software'] = server_software or SERVER_SOFTWARE
    return wsgi.WSGIServer(wsgi_proxy, name=name, **kwargs)


if __name__ == '__main__':
    server().start()
