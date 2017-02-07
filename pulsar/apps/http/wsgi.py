"""Classes for testing WSGI servers using the HttpClient
"""
from asyncio import Transport

import pulsar
from pulsar.apps import http
from pulsar.utils.http import HttpRequestParser
from pulsar.api import Protocol, Producer
from pulsar.apps.wsgi import HttpServerResponse
from pulsar.async.access import cfg


__all__ = ['HttpWsgiClient']


class DummyTransport(Transport):

    def __init__(self, connection, address, ssl=None):
        super().__init__()
        self.connection = connection
        self._extra['sockname'] = address

    def can_write_eof(self):
        return False

    def close(self):
        pass

    def abort(self):
        pass

    def write(self, chunk):
        consumer = self.connection.current_consumer()
        if self.connection.other_side:
            self.connection.other_side.data_received(chunk)
        else:
            consumer.message.extend(chunk)
            consumer.in_parser.feed_data(chunk)
            if consumer.in_parser.is_message_complete():
                consumer.finished()


class DymmyConnection:
    other_side = None

    def write(self, data):
        self.transport.write(data)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def detach(self, discard=True):
        pass

    def close(self):
        pass


class DummyClientConnection(Protocol, DymmyConnection):
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
        else:
            consumer.message = bytearray()
            consumer.in_parser = HttpRequestParser(consumer)

        return consumer


class DummyServerConnection(Protocol, DymmyConnection):

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
