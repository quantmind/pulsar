"""This script demonstrates the use synchronous web frameworks such as flask_
in the context of asynchronous responses. It uses pulsar the :mod:`.greenio`
module to run flask on a pool of greenlet.

This configuration allows the use of the greenlet-friendly :class:`EchoGreen`
client within a flask response function.
The Echo server starts at the same time as the Flask WSGI server.

To run the script::

    python manage.py

and point to http://localhost:8080 or any other sub urls.

Main Classes
==============

.. autoclass:: FlaskSite

.. autoclass:: EchoGreen

.. autoclass:: FlaskSite

Components
=============

In this snippet the following components are used:

* Pulsar :class:`.MultiApp`
* Pulsar :class:`.WSGIServer`
* Pulsar :class:`.SocketServer`
* Pulsar :class:`.GreenWSGI`

For more information about writing server and clients check the
:ref:`writing clients tutorial <tutorials-writing-clients>`.

.. _flask: http://flask.pocoo.org/
"""
from functools import partial

from flask import Flask, make_response

from pulsar.apps import wsgi

import pulsar
from pulsar import Pool, Connection, AbstractClient, ProtocolError
from pulsar.apps.socket import SocketServer

from pulsar import MultiApp, Config
from pulsar.apps.wsgi import WSGIServer

from pulsar.apps.greenio import GreenPool
from pulsar.apps.greenio.wsgi import GreenWSGI


class EchoProtocol(pulsar.ProtocolConsumer):
    separator = b'\r\n\r\n'
    '''A separator for messages.'''
    buffer = b''
    '''The buffer for long messages'''

    def data_received(self, data):
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
        self.transport.write(self._request + self.separator)

    def response(self, data, rest):
        if rest:
            raise ProtocolError
        return data[:-len(self.separator)]


class EchoServerProtocol(EchoProtocol):
    '''The :class:`EchoProtocol` used by the echo :func:`server`.
    '''
    def response(self, data, rest):
        self.transport.write(data)
        data = data[:-len(self.separator)]
        # If we get a QUIT message, close the transport.
        # Used by the test suite.
        if data == b'QUIT':
            self.transport.close()
        return data


class EchoGreen(AbstractClient):
    """A client for the echo server to be used by the Flask application
    """
    protocol_factory = partial(Connection, EchoProtocol)
    address = None

    def __init__(self, appname, wait, pool_size=5, loop=None):
        super().__init__(loop)
        self.app_name = appname
        self.wait = wait
        self.pool = Pool(self.connect, pool_size, self._loop)

    def connect(self):
        return self.create_connection(self.address)

    def __call__(self, message):
        return self.wait(self._call(message))

    async def _call(self, message):
        # get the address of the echo application
        if not self.address:
            app = await pulsar.get_application(self.app_name)
            self.address = app.cfg.addresses[0]
        connection = await self.pool.connect()
        with connection:
            consumer = connection.current_consumer()
            consumer.start(message)
            await consumer.on_finished
            return consumer.buffer


def FlaskApp(echo):
    app = Flask(__name__)

    @app.errorhandler(404)
    def not_found(e):
        return make_response("404 Page", 404)

    @app.route('/', methods=['GET'])
    def home():
        return "Try any other url for an echo"

    @app.route('/<path>', methods=['GET'])
    def add_org(path):
        a = echo(path.encode('utf-8'))
        return "Flask Url : %s" % a.decode('utf-8')

    return app


class FlaskSite(wsgi.LazyWsgi):
    """Configure a flask_ application to run on a :class:`.GreenPool`
    """
    def setup(self, environ=None):
        green_pool = GreenPool()
        cfg = environ['pulsar.cfg']
        echo_app_name = 'echo_%s' % cfg.name
        echo = EchoGreen(echo_app_name, green_pool.wait)
        app = FlaskApp(echo)
        return wsgi.WsgiHandler([GreenWSGI(app, green_pool)])


class FlaskGreen(MultiApp):
    """Multiapp Server configurator
    """
    cfg = Config(bind=':8080', echo_bind=':8060')

    def build(self):
        yield self.new_app(WSGIServer, callable=FlaskSite())
        yield self.new_app(SocketServer, 'echo', callable=EchoServerProtocol,
                           echo_connection_made=log_connection)


def log_connection(connection, exc=None):
    if not exc:
        connection.logger.debug('Got a new connection to echo server!')


if __name__ == '__main__':  # pragma    nocover
    FlaskGreen().start()
