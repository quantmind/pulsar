'''Asynchronous application for serving requests
on sockets. This is the base class of :class:`.WSGIServer`.
All is needed by a :class:`SocketServer` is a callable which build a
:class:`.ProtocolConsumer` for each new client request received.
This is an example of a script for an Echo server::

    import pulsar
    from pulsar.apps.socket import SocketServer

    class EchoServerProtocol(pulsar.ProtocolConsumer):
        ...

    if __name__ == '__main__':
        SocketServer(EchoServerProtocol).start()

Check the :ref:`echo server example <tutorials-writing-clients>` for detailed
implementation of the ``EchoServerProtocol`` class.

.. _socket-server-settings:

Socket Server Settings
==============================
All standard :ref:`application settings <settings>` can be applied to a
:class:`SocketServer`. In addition, the following are
specific to sockets and can be used to fine tune your application:

bind
------
To specify the address to bind the server to::

    python script.py --bind 127.0.0.1:8070

This will listen for both ipv4 and ipv6 sockets on all hosts on port 8080::

    python script.py --bind :8080

backlog
---------
To specify the maximum number of queued connections you can use the
:ref:`backlog <setting-backlog>` settings. For example::

    python script.py --backlog 1000

rarely used.

keep_alive
---------------
To control how long a server :class:`.Connection` is kept alive after the
last read from the remote client, one can use the
:ref:`keep-alive <setting-keep_alive>` setting::

    python script.py --keep-alive 10

will close client connections which have been idle for 10 seconds.

.. _socket-server-ssl:

TLS/SSL support
------------------------
Transport Layer Security (often known as Secure Sockets Layer) is handled by
the :ref:`cert-file <setting-cert_file>` and :ref:`key-file <setting-key_file>`
settings::

    python script.py --cert-file server.crt --key-file server.key


.. _socket-server-concurrency:

Concurrency
==================

When running a :class:`SocketServer` in multi-process mode (default),
the application, create a listening socket in the parent (Arbiter) process
and then spawn several process-based actors which listen on the
same shared socket.
This is how pre-forking servers operate.

When running a :class:`SocketServer` in threading mode::

    python script.py --concurrency thread

the number of :class:`.Actor` serving the application is set
to ``0`` so that the application is actually served by the
:class:`.Arbiter` event-loop (we refer this to a single process server).
This configuration is used when debugging, testing, benchmarking or on small
load servers.

In addition, a :class:`SocketServer` in multi-process mode is only available
for:

* Posix systems.
* Windows running python 3.2 or above (python 2 on windows does not support
  the creation of sockets from file descriptors).

Check the :meth:`SocketServer.monitor_start` method for implementation details.
'''
import os
from functools import partial

import pulsar
from pulsar import TcpServer, Connection
from pulsar.utils.internet import (parse_address, SSLContext, WrapSocket,
                                   format_address)
from pulsar.utils.config import pass_through


class SocketSetting(pulsar.Setting):
    virtual = True
    app = 'socket'
    section = "Socket Servers"


class Bind(SocketSetting):
    name = "bind"
    flags = ["-b", "--bind"]
    meta = "ADDRESS"
    default = "127.0.0.1:{0}".format(pulsar.DEFAULT_PORT)
    desc = """\
        The socket to bind.

        A string of the form: ``HOST``, ``HOST:PORT``, ``unix:PATH``.
        An IP is a valid HOST.
        """


class KeepAlive(SocketSetting):
    name = "keep_alive"
    flags = ["--keep-alive"]
    validator = pulsar.validate_pos_int
    type = int
    default = 15
    desc = """\
        The number of seconds to keep an idle client connection
        open."""


class Backlog(SocketSetting):
    name = "backlog"
    flags = ["--backlog"]
    validator = pulsar.validate_pos_int
    type = int
    default = 2048
    desc = """\
        The maximum number of queued connections in a socket.

        This refers to the number of clients that can be waiting to be served.
        Exceeding this number results in the client getting an error when
        attempting to connect. It should only affect servers under significant
        load.
        Must be a positive integer. Generally set in the 64-2048 range.
        """


class KeyFile(SocketSetting):
    name = "key_file"
    flags = ["--key-file"]
    meta = "FILE"
    default = None
    desc = """\
    SSL key file
    """


class CertFile(SocketSetting):
    name = "cert_file"
    flags = ["--cert-file"]
    meta = "FILE"
    default = None
    desc = """\
    SSL certificate file
    """


class SocketServer(pulsar.Application):
    '''A :class:`.Application` which serve application on a socket.

    It bind a socket to a given address and listen for requests. The request
    handler is constructed from the callable passed during initialisation.

    .. attribute:: address

        The socket address, available once the application has started.
    '''
    name = 'socket'
    address = None
    server_class = TcpServer
    cfg = pulsar.Config(apps=['socket'])

    def protocol_factory(self):
        '''Factory of :class:`.ProtocolConsumer` used by the server.

        By default it returns the :meth:`.Application.callable`.
        '''
        return partial(Connection, self.callable)

    def monitor_start(self, monitor):
        '''Create the socket listening to the ``bind`` address.

        If the platform does not support multiprocessing sockets set the
        number of workers to 0.
        '''
        cfg = self.cfg
        loop = monitor._loop
        if (not pulsar.platform.has_multiProcessSocket
                or cfg.concurrency == 'thread'):
            cfg.set('workers', 0)
        if not cfg.address:
            raise pulsar.ImproperlyConfigured('Could not open a socket. '
                                              'No address to bind to')
        ssl = None
        if cfg.cert_file or cfg.key_file:
            if cfg.cert_file and not os.path.exists(cfg.cert_file):
                raise ValueError('cert_file "%s" does not exist' %
                                 cfg.cert_file)
            if cfg.key_file and not os.path.exists(cfg.key_file):
                raise ValueError('key_file "%s" does not exist' % cfg.key_file)
            ssl = SSLContext(keyfile=cfg.key_file, certfile=cfg.cert_file)
        address = parse_address(self.cfg.address)
        # First create the sockets
        server = yield loop.create_server(lambda: None, *address)
        addresses = []
        sockets = []
        for sock in server.sockets:
            addresses.append(sock.getsockname())
            sockets.append(WrapSocket(sock))
        server.close()
        monitor.params.sockets = sockets
        monitor.params.ssl = ssl
        self.addresses = addresses
        self.address = addresses[0]

    def worker_start(self, worker):
        '''Start the worker by invoking the :meth:`create_server` method.'''
        worker.servers[self.name] = self.create_server(worker)

    def worker_stopping(self, worker):
        server = servers[self.name]
        return server.close()

    def worker_info(self, worker, info):
        server = worker.servers.get(self.name)
        if server:
            info['tcpserver'] = server.info()
        return info

    #   INTERNALS
    def create_server(self, worker):
        '''Create the Server which will listen for requests.

        :return: a :class:`.TcpServer`.
        '''
        sockets = [sock.sock for sock in worker.params.sockets]
        cfg = self.cfg
        server = self.server_class(self.protocol_factory(),
                                   worker._loop,
                                   sockets=sockets,
                                   max_connections=cfg.max_requests,
                                   keep_alive=cfg.keep_alive,
                                   name=self.name)
        for event in ('connection_made', 'pre_request', 'post_request',
                      'connection_lost'):
            callback = getattr(cfg, event)
            if callback != pass_through:
                server.bind_event(event, callback)
        server.start_serving(cfg.backlog, sslcontext=worker.params.ssl)
        return server
