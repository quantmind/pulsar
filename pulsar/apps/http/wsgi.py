"""Classes for testing WSGI servers using the HttpClient
"""
from asyncio import Transport

import pulsar
from pulsar.apps import http
from pulsar.utils.http import HttpRequestParser
from pulsar.api import Protocol
from pulsar.apps.wsgi import HttpServerResponse
from pulsar.async.access import cfg


__all__ = ['HttpWsgiClient']


class DummyTransport(Transport):

    def __init__(self, connection, address, ssl):
        super().__init__()
        self.connection = connection
        self._extra['sockname'] = address

    def close(self):
        pass

    def abort(self):
        pass

    def write(self, chunk):
        consumer = self.connection.current_consumer()
        server_side = consumer.server_side
        if server_side:
            server_side.feed_data(chunk)
        else:
            consumer.message.extend(chunk)
            consumer.in_parser.feed_data(chunk)
            if consumer.in_parser.is_message_complete():
                consumer.finished()


class DummyConnection(Protocol):
    """A class simulating a :class:`pulsar.Transport` to a :attr:`connection`

    .. attribute:: client

        The :class:`pulsar.Client` using this :class:`DummyTransport`

    .. attribute:: connection

        The *server* connection for this :attr:`client`
    """
    @classmethod
    def create(cls, producer, address, ssl=None):
        connection = DummyConnection(cls.consumer_factory, producer)
        connection.connection_made(DummyTransport(connection, address, ssl))
        return connection

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def detach(self, discard=True):
        pass

    @classmethod
    def consumer_factory(cls, connection):
        consumer = http.HttpResponse(connection)
        consumer.server_side = None
        producer = consumer.producer

        if producer.wsgi:
            consumer.server_side = HttpServerResponse(connection)
            consumer.server_side.start()
        else:
            consumer.server_side = None
            consumer.message = bytearray()
            consumer.in_parser = HttpRequestParser(consumer)

        return consumer


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


class HttpWsgiClient(http.HttpClient):
    """A test client for http requests to a WSGI server handlers.

    .. attribute:: wsgi

        The WSGI server handler to test
    """
    client_version = 'Pulsar-Http-Wsgi-Client'
    server_software = 'Local/%s' % pulsar.SERVER_SOFTWARE
    connection_pool = DummyConnectionPool

    def __init__(self, wsgi=None, **kwargs):
        self.wsgi = wsgi
        self.cfg = getattr(wsgi, 'cfg', None) or cfg()
        super().__init__(**kwargs)
        self.headers['X-Http-Local'] = 'local'

    async def create_connection(self, address, ssl=None):
        return DummyConnection.create(self, address)

    def get_headers(self, request, headers):
        headers = super().get_headers(request, headers)
        headers['X-Forwarded-Proto'] = request.key[0]
        return headers
