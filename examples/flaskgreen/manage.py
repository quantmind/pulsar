"""Demonstrate the use of

* Pulsar multi app
* Asynchronous flask with greenlet
"""
from flask import Flask, make_response

from pulsar.apps import wsgi

import asyncio
from functools import partial

import pulsar
from pulsar import Pool, Connection, AbstractClient, ProtocolError
from pulsar.apps.socket import SocketServer

from pulsar import MultiApp, Config
from pulsar.apps.wsgi import WSGIServer

from pulsar.apps.greenio import GreenPool
from pulsar.apps.greenio.wsgi import GreenWSGI


class EchoProtocol(pulsar.ProtocolConsumer):
    '''An echo :class:`~.ProtocolConsumer` for client and servers.

    The only difference between client and server is the implementation
    of the :meth:`response` method.
    '''
    separator = b'\r\n\r\n'
    '''A separator for messages.'''
    buffer = b''
    '''The buffer for long messages'''

    def data_received(self, data):
        '''Implements the :meth:`~.ProtocolConsumer.data_received` method.

        It simply search for the :attr:`separator` and, if found, it invokes
        the :meth:`response` method with the value of the message.
        '''
        if self.buffer:
            data = self.buffer + data

        idx = data.find(self.separator)
        if idx >= 0:    # we have a full message
            idx += len(self.separator)
            data, rest = data[:idx], data[idx:]
            self.buffer = self.response(data, rest)
            self.finished()
            return rest
        else:
            self.buffer = data

    def start_request(self):
        '''Override :meth:`~.ProtocolConsumer.start_request` to write
        the message ended by the :attr:`separator` into the transport.
        '''
        self.transport.write(self._request + self.separator)

    def response(self, data, rest):
        '''Clients return the message so that the
        :attr:`.ProtocolConsumer.on_finished` is called back with the
        message value, while servers sends the message back to the client.
        '''
        if rest:
            raise ProtocolError
        return data[:-len(self.separator)]


class EchoServerProtocol(EchoProtocol):
    '''The :class:`EchoProtocol` used by the echo :func:`server`.
    '''
    def response(self, data, rest):
        '''Override :meth:`~EchoProtocol.response` method by writing the
        ``data`` received back to the client.
        '''
        self.transport.write(data)
        data = data[:-len(self.separator)]
        # If we get a QUIT message, close the transport.
        # Used by the test suite.
        if data == b'QUIT':
            self.transport.close()
        return data


class EchoGreen(AbstractClient):
    """A client for the echo server.
    """
    protocol_factory = partial(Connection, EchoProtocol)

    def __init__(self, address, wait, pool_size=10, loop=None):
        super().__init__(loop)
        self.address = address
        self.wait = wait
        self.pool = Pool(self.connect, pool_size, self._loop)

    def connect(self):
        return self.create_connection(self.address)

    def __call__(self, message):
        return self.wait(self._call(message))

    @asyncio.coroutine
    def _call(self, message):
        connection = yield from self.pool.connect()
        with connection:
            consumer = connection.current_consumer()
            consumer.start(message)
            yield from consumer.on_finished
            return consumer.buffer


def FlaskApp(echo):
    app = Flask(__name__)

    @app.errorhandler(404)
    def not_found(e):
        return make_response("404 Page", 404)

    @app.route('/<path>', methods=['GET'])
    def add_org(path):
        a = echo(path.encode('utf-8'))
        return "Flask Url : %s" % a.decode('utf-8')

    return app


class Site(wsgi.LazyWsgi):

    def setup(self, environ=None):
        green_pool = GreenPool()
        echo = EchoGreen(('localhost', 8060), green_pool.wait)
        app = FlaskApp(echo)
        return wsgi.WsgiHandler([GreenWSGI(app, green_pool)], async=True)


def server(**kwargs):
    return wsgi.WSGIServer(Site(), **kwargs)


class Server(MultiApp):
    cfg = Config(bind=':8080', echo_bind=':8060')

    def build(self):
        yield self.new_app(WSGIServer, callable=Site())
        yield self.new_app(SocketServer, 'echo', callable=EchoServerProtocol)


if __name__ == '__main__':  # pragma    nocover
    app = Server()
    app.start()
