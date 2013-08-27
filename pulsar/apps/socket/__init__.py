'''Asynchronous application for serving requests
on a socket. This is the base class of :class:`pulsar.apps.wsgi.WSGIServer`.
All is needed by a :class:`SocketServer` is a callable which build a
:class:`pulsar.ProtocolConsumer` for each new client request received.
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
The :class:`SocketServer` application introduces the additional pulsar settings
:ref:`bind <setting-bind>` which is used to specify the address
to bind the server to::

    python script.py --bind 127.0.0.1:8070

backlog
---------   
To control the concurrency of the server you can use the
:ref:`backlog <setting-backlog>` settings. For example::

    python script.py --backlog 1000
    
will serve a maximum of 1000 clients per :class:`pulsar.Worker` concurrently.

keep_alive
---------------
To control how long a server :class:`pulsar.Connection` is kept alive after the
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
This is how, pre-forking servers operate.

When running a :class:`SocketServer` in threading mode::

    python script.py --concurrency thread
 
the number of :class:`pulsar.apps.Worker` serving the application is set to ``0``
so that the application is actually served by the :class:`pulsar.Arbiter`
event-loop (we refer this to a single process server).
This configuration is used when debugging, testing, benchmarking or on small
load servers.

In addition, a :class:`SocketServer` in multi-process mode is only available for:
    
* Posix systems.
* Windows running python 3.2 or above (python 2 on windows does not support
  the creation of sockets from file descriptors).
  
Check the :meth:`SocketServer.monitor_start` method for implementation details.
'''
import os

import pulsar
from pulsar import async, TcpServer
from pulsar.utils.internet import parse_address, SSLContext, WrapSocket


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
    '''A :class:`pulsar.apps.Application` which bind a socket to a given address
and listen for requests. The request handler is constructed from the
callable passed during initialisation.
    
.. attribute:: address

    The socket address, available once the application has started.
    '''
    name = 'socket'
    address = None
    cfg = pulsar.Config(apps=['socket'])
    
    def protocol_consumer(self):
        '''Returns the factory of :class:`pulsar.ProtocolConsumer` used by
the server. By default it returns the :attr:`pulsar.apps.Application.callable`
attribute.'''
        return self.callable
    
    @async()
    def monitor_start(self, monitor):
        '''Create the socket listening to the ``bind`` address. if the platform
does not support multiprocessing sockets set the number of workers to 0.'''
        cfg = self.cfg
        loop = monitor.event_loop
        if not pulsar.platform.has_multiProcessSocket\
            or cfg.concurrency == 'thread':
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
        sockets = yield loop.start_serving(lambda: None, *address)
        addresses = []
        for sock in sockets:
            assert loop.remove_reader(sock.fileno()), (
                        "Could not remove reader")
            addresses.append(sock.getsockname())
        monitor.params.sockets = [WrapSocket(s) for s in sockets]
        monitor.params.ssl = ssl
        self.addresses = addresses
        self.address = addresses[0]
    
    def worker_start(self, worker):
        '''Start the worker by invoking the :meth:`create_server` method.'''
        worker.servers[self.name] = servers = []
        for sock in worker.params.sockets:
            server = self.create_server(worker, sock.sock)
            servers.append(server)
    
    def on_info(self, worker, data):
        server = worker.socket_server
        data['socket'] = {'listen_on': server.address,
                          'read_timeout': server.timeout,
                          'active_connections': server.active_connections,
                          'received_connections': server.received}
        return data

    def create_server(self, worker, sock, ssl=None):
        '''Create the Server Protocol which will listen for requests. It
uses the :meth:`protocol_consumer` method as the protocol consumer factory.'''
        cfg = self.cfg
        server = TcpServer(worker.event_loop,
                           sock=sock,
                           consumer_factory=self.protocol_consumer(),
                           max_connections=cfg.max_requests,
                           timeout=cfg.keep_alive,
                           name=self.name)
        server.bind_event('connection_made', cfg.connection_made)
        server.bind_event('pre_request', cfg.pre_request)
        server.bind_event('post_request', cfg.post_request)
        server.bind_event('connection_lost', cfg.connection_lost)
        server.start_serving(cfg.backlog, sslcontext=worker.params.ssl)
        return server
        