"""Classes for testing WSGI servers using the HttpClient
"""
from asyncio import Transport

from pulsar.apps import http
from pulsar.apps.wsgi import HttpServerResponse
from pulsar.utils.httpurl import http_parser


__all__ = ['HttpTestClient']


class DummyTransport(Transport):

    @property
    def transport(self):
        return self

    def close(self):
        pass

    def abort(self):
        pass


class DummyConnection(DummyTransport):
    """A class simulating a :class:`pulsar.Transport` to a :attr:`connection`

    .. attribute:: client

        The :class:`pulsar.Client` using this :class:`DummyTransport`

    .. attribute:: connection

        The *server* connection for this :attr:`client`
    """
    def __init__(self, producer, address, ssl=None):
        super().__init__()
        self.address = address
        self.ssl = ssl
        self._producer = producer
        self._processed = 0
        self._current_consumer = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    @property
    def _loop(self):
        return self._producer._loop

    def detach(self, discard=True):
        pass

    def current_consumer(self):
        consumer = http.HttpResponse(self._loop)
        consumer._connection = self
        consumer.server_side = None

        if self._producer.wsgi:
            server_side = HttpServerResponse(
                self._producer.wsgi, self._producer.test.cfg, loop=self._loop
            )
            server_side._connection = DummyServerConnection(
                server_side, consumer, self.address
            )
            consumer.server_side = server_side

        else:
            consumer.server_side = None
            consumer.message = b''
            consumer.in_parser = http_parser(kind=0)

        consumer.connection_made(self)
        self._current_consumer = consumer
        return consumer

    def write(self, chunk):
        consumer = self._current_consumer
        server_side = consumer.server_side
        if server_side:
            server_side.data_received(chunk)
        else:
            consumer.message += chunk
            assert consumer.in_parser.execute(chunk, len(chunk)) == len(chunk)
            if consumer.in_parser.is_message_complete():
                consumer.finished()


class DummyServerConnection(DummyTransport):

    def __init__(self, server, response, address):
        super().__init__({'sockname': ('127.0.0.1', 1234)})
        self.address = address
        self._current_consumer = server
        self.response = response

    def write(self, chunk):
        self.response.data_received(chunk)


class DummyConnectionPool:
    """A class for simulating a client connection with a server
    """
    def __init__(self, connector, **kwargs):
        self.connector = connector

    async def connect(self):
        return await self.connector()


class HttpTestClient(http.HttpClient):
    """A test client for http requests to a WSGI server handlers.

    .. attribute:: wsgi

        The WSGI server handler to test
    """
    client_version = 'Pulsar-Http-Test-Client'
    connection_pool = DummyConnectionPool

    def __init__(self, test=None, wsgi=None, **kwargs):
        self.test = test
        self.wsgi = wsgi
        super().__init__(**kwargs)

    async def create_connection(self, address, ssl=None):
        return DummyConnection(self, address)

    def get_headers(self, request, headers):
        headers = super().get_headers(request, headers)
        headers['X-Forwarded-Proto'] = request.key[0]
        return headers
