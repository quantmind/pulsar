"""Classes for testing WSGI servers using the HttpClient
"""
from asyncio import Transport, gather

import pulsar
from pulsar.apps import http
from pulsar.utils.http import HttpRequestParser
from pulsar.api import Protocol, Producer
from pulsar.apps.wsgi import HttpServerResponse
from pulsar.async.access import cfg
from pulsar.async.mixins import Pipeline


class DummyTransport(Transport):
    """A Dummy Transport

    data is not sent through the wire, instead it is passed to
    the connection other side object.
    """
    def __init__(self, connection, address, ssl=None):
        super().__init__()
        self.connection = connection
        self._is_closing = False
        self._extra['sockname'] = address

    def can_write_eof(self):
        return False

    def is_closing(self):
        return self._is_closing

    def close(self):
        self._is_closing = True
        self.connection.connection_lost(None)

    def abort(self):
        self.connection.connection_lost(None)

    def write(self, chunk):
        consumer = self.connection.current_consumer()
        if self.connection.other_side:
            self.connection.other_side.data_received(chunk)
        else:
            consumer.message.extend(chunk)
            consumer.in_parser.feed_data(chunk)
            if consumer.in_parser.is_message_complete():
                consumer.finished()


class DummyConnection(Pipeline):
    other_side = None

    def write(self, data):
        self.transport.write(data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, type, value, traceback):
        await self.detach()

    async def detach(self, discard=True):
        waiter = self.close()
        if waiter:
            await waiter

    def close(self, other_side=True):
        """Close the connection

        Close the pipeline if open and make sure the other connection
        is closed too.

        :param other_side: if True close the other_side connection too.
            used to avoid infinite loop
        """
        waiters = [self.close_pipeline()]
        if self.other_side and other_side:
            waiters.append(self.other_side.close(False))
        if self.transport:
            self.transport.close()
        waiters = [w for w in waiters if w]
        if waiters:
            return waiters[0] if len(waiters) == 1 else gather(*waiters)


class DummyClientConnection(DummyConnection, Protocol):
    """A class simulating a :class:`pulsar.Transport` to a :attr:`connection`

    .. attribute:: client

        The :class:`pulsar.Client` using this :class:`DummyTransport`

    .. attribute:: connection

        The *server* connection for this :attr:`client`
    """
    @classmethod
    def create(cls, producer, address, ssl=None):
        connection = cls(cls.consumer_factory, producer)
        connection.connection_made(DummyTransport(connection, address, ssl))
        return connection

    @classmethod
    def consumer_factory(cls, connection):
        consumer = http.HttpResponse(connection)

        if connection.producer.wsgi_callable:
            connection.other_side = WsgiProducer(connection).create_protocol()
            consumer.server_side = connection.other_side.current_consumer()
        else:
            consumer.message = bytearray()
            consumer.in_parser = HttpRequestParser(consumer)

        return consumer


class DummyServerConnection(DummyConnection, Protocol):

    @classmethod
    def create(cls, producer):
        client_connection = producer.client_connection
        server_connection = cls(HttpServerResponse, producer)
        server_transport = DummyTransport(
            server_connection,
            client_connection.transport.get_extra_info('sockname')
        )
        server_connection.connection_made(server_transport)
        server_connection.other_side = client_connection
        return server_connection


class DummyConnectionPool:
    """A class for simulating a client connection with a server
    """
    def __init__(self, connector, **kwargs):
        self.connector = connector

    async def connect(self):
        return await self.connector()


class HttpWsgiClient(http.HttpClient):
    """A test client for http requests to a WSGI server handlers.

    .. attribute:: wsgi

        The WSGI server handler to test
    """
    client_version = 'Pulsar-Http-Wsgi-Client'
    connection_pool = DummyConnectionPool

    def __init__(self, wsgi_callable=None, **kwargs):
        self.wsgi_callable = wsgi_callable
        self.cfg = getattr(wsgi_callable, 'cfg', None) or cfg()
        super().__init__(**kwargs)
        self.headers['X-Http-Local'] = 'local'

    async def create_connection(self, address, ssl=None):
        return DummyClientConnection.create(self, address)

    def get_headers(self, request, headers):
        headers = super().get_headers(request, headers)
        headers['X-Forwarded-Proto'] = request.key[0]
        return headers


class WsgiProducer(Producer):
    """Dummy producers of Server protocols
    """
    server_software = 'Local/%s' % pulsar.SERVER_SOFTWARE

    def __init__(self, connection):
        super().__init__(DummyServerConnection.create, loop=connection._loop)
        self.client_connection = connection
        self.cfg = connection.producer.cfg
        self.wsgi_callable = connection.producer.wsgi_callable
        self.keep_alive = self.cfg.http_keep_alive or 0
