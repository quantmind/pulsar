"""Classes for testing WSGI servers using the HttpClient
"""
import asyncio
from functools import partial

from pulsar.apps import http
from pulsar.apps.wsgi import HttpServerResponse

__all__ = ['HttpTestClient']


class DummyTransport(asyncio.Transport):
    """A class simulating a :class:`pulsar.Transport` to a :attr:`connection`

    .. attribute:: client

        The :class:`pulsar.Client` using this :class:`DummyTransport`

    .. attribute:: connection

        The *server* connection for this :attr:`client`
    """
    def __init__(self, client, connnection):
        self.client = client
        self.connection = connnection

    def write(self, data):
        """Writing data means calling ``data_received`` on the
        server :attr:`connection`
        """
        self.connection.data_received(data)

    @property
    def address(self):
        return self.connection.address


class DummyConnectionPool:
    """A class for simulating a client connection with a server
    """
    def get_or_create_connection(self, producer):
        client = self.connection_factory(self.address, 1, 0,
                                         producer.consumer_factory,
                                         producer)
        server = self.connection_factory(('127.0.0.1', 46387), 1, 0,
                                         producer.server_consumer,
                                         producer)
        client.connection_made(DummyTransport(producer, server))
        server.connection_made(DummyTransport(producer, client))
        return client


class HttpTestClient(http.HttpClient):
    """Useful :class:`pulsar.apps.http.HttpClient` for wsgi server
    handlers.

    .. attribute:: wsgi_handler

        The WSGI server handler to test
    """
    client_version = 'Pulsar-Http-Test-Client'
    connection_pool = DummyConnectionPool

    def __init__(self, test, wsgi_handler, **kwargs):
        self.test = test
        self.wsgi_handler = wsgi_handler
        self.server_consumer = partial(HttpServerResponse, wsgi_handler,
                                       test.cfg)
        super().__init__(**kwargs)

    def data_received(self, connnection, data):
        pass

    def response(self, request):
        conn = self.get_connection(request)
        # build the protocol consumer
        consumer = conn.consumer_factory(conn)
        # start the request
        consumer.new_request(request)
        return consumer
