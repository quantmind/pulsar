"""Classes for testing WSGI servers using the HttpClient
"""
from asyncio import Transport

from pulsar.apps import http
from pulsar.apps.wsgi import HttpServerResponse


__all__ = ['HttpTestClient']


class DummyConnection(Transport):
    """A class simulating a :class:`pulsar.Transport` to a :attr:`connection`

    .. attribute:: client

        The :class:`pulsar.Client` using this :class:`DummyTransport`

    .. attribute:: connection

        The *server* connection for this :attr:`client`
    """
    def __init__(self, producer, address):
        super().__init__()
        self.address = address
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

    @property
    def transport(self):
        return self

    def detach(self, discard=True):
        pass

    def current_consumer(self):
        consumer = http.HttpResponse(self._loop)
        consumer._connection = self
        server_side = HttpServerResponse(
            self._producer.wsgi, self._producer.test.cfg, loop=self._loop
        )
        server_side._connection = DummyServerConnection(
            server_side, consumer, self.address
        )
        consumer.server_side = server_side
        consumer.connection_made(self)
        self._current_consumer = consumer
        return consumer

    def write(self, chunk):
        server_side = self._current_consumer.server_side
        server_side.data_received(chunk)


class DummyServerConnection(Transport):

    def __init__(self, server, response, address):
        super().__init__({'sockname': ('127.0.0.1', 1234)})
        self.address = address
        self._current_consumer = server
        self.response = response

    @property
    def transport(self):
        return self

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

    def __init__(self, test, wsgi, **kwargs):
        self.test = test
        self.wsgi = wsgi
        super().__init__(**kwargs)

    async def create_connection(self, address, ssl=None):
        return DummyConnection(self, address)
