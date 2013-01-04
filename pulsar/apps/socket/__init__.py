'''Asynchronous application for serving requests
on a socket. This is the base class of :class:`pulsar.apps.wsgi.WSGIServer`.

A simple echo server ``script.py`` can be implemented as follow::

    import pulsar
    from pulsar.apps.socket import SocketServer
    
    class EchoProtocol:
        "The simplest protocol possible."
        def decode(self, data):
            return bytes(data), bytearray()
        
        def encode(self, data):
            return data
    
    def echoserver(actor, socket, **kwargs):
        kwargs['protocol_factory'] = EchoProtocol
        return pulsar.AsyncSocketServer(actor, socket, **kwargs)
        
    if __name__ == '__main__':
        SocketServer(socket_server_factory=echoserver).start()
    
    
The main parameter for :class:`SocketServer` is the ``socket_server_factory``
callable which build a :class:`pulsar.AsyncSocketServer` for each
:class:`pulsar.Worker` serving the application.

.. _socket-protocol:

Protocol
==============

The protocol class needs to implement two methods, the ``decode`` for servers
and the ``encode`` for clients. Both methods accept as input one parameter which
can be ``bytes`` or a ``bytesarray``.

The ``decode`` method must return a **two elements tuple** of the form:

 * ``None``, ``bytesarray``: when more data is needed to form a message.
 * ``message``, ``bytesarray``: where *message* is a correctly parsed message
   and *bytesarray* contains data for the next message. 

The ``encode`` method must return ``bytes`` which will be sent to the server.

Useful settings
==================
All standard :ref:`settings <settings>` can be applied to a
:class:`SocketServer`. These are some of the most important:

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

keepalive
---------------
To control how long a :class:`pulsar.AsyncConnection` is kept alive after the
last read from the remote client, one can use the
:ref:`keepalive <setting-keepalive>` setting.

    python script.py --keepalive 10
    
will close a client connection which has been idle for 10 seconds.
 


Concurrency
==================

When running a :class:`SocketServer` in threading mode::

    python script.py --concurrency thread
 
the number of :class:`pulsar.Worker` serving the application is set to 0 so
that the application is actually served by the :class:`Arbiter`
eventloop (we refer this to a single process server).
This configuration is used when debugging, testing, benchmarking or small
load servers.

In addition, a :class:`SocketServer` in multi-process mode is only available for:
    
 * Posix systems
 * Windows running python 3.2 or above.
'''
import pulsar
from pulsar import AsyncIOStream


class Bind(pulsar.Setting):
    section = "Socket Servers"
    app = 'socket'
    name = "bind"
    flags = ["-b", "--bind"]
    meta = "ADDRESS"
    default = "127.0.0.1:{0}".format(pulsar.DEFAULT_PORT)
    desc = """\
        The socket to bind.
        
        A string of the form: 'HOST', 'HOST:PORT', 'unix:PATH'. An IP is a valid
        HOST.
        """
        
class Keepalive(pulsar.Setting):
    section = "Socket Servers"
    app = 'socket'
    name = "keepalive"
    flags = ["--keep-alive"]
    validator = pulsar.validate_pos_int
    type = int
    default = 15
    desc = """\
        The number of seconds to keep an idle client connection
        open."""


class SocketServer(pulsar.Application):
    '''This application bind a socket to a given address and listen for
requests. The request handler is constructued from the
:attr:`socket_server_factory` parameter/attribute.
    
.. attribute:: socket_server_factory

    Callable which returns the asynchronous socket server, usually
    a subclass of :class:`pulsar.AsyncSocketServer`.
    
.. attribute:: address

    The socket address, available once the application has started.
    '''
    _app_name = 'socket'
    socket_server_factory = None
    address = None
    
    def monitor_start(self, monitor):
        # if the platform does not support multiprocessing sockets set
        # the number of workers to 0.
        cfg = self.cfg
        if not self.socket_server_factory:
            raise TypeError('Socket server class not specified.')
        if not pulsar.platform.has_multiProcessSocket\
            or cfg.concurrency == 'thread':
            cfg.set('workers', 0)
        # Open the socket and bind to address
        address = self.cfg.address
        if address:
            socket = pulsar.create_socket(address, backlog=self.cfg.backlog)
        else:
            raise pulsar.ImproperlyConfigured('Could not open a socket. '
                                              'No address to bind to')
        self.logger.info('Listening on %s', socket)
        monitor.params.socket = socket
        self.address = socket.name
    
    def worker_start(self, worker):
        # Start the worker by starting the socket server
        s = self.socket_server_factory(worker, worker.params.socket,
                                       timeout=self.cfg.keepalive)
        # We add the file descriptor handler
        s.on_connection_callbacks.append(worker.handle_fd_event)
        worker.socket_server = s
    
    def worker_stop(self, worker):
        if hasattr(worker, 'socket_server'):
            # we don't shut down the socket, simply remove all active
            # connections and othe clean up operations.
            worker.socket_server.on_close()
        
    def on_event(self, worker, fd, events):
        connection = worker.socket_server.accept()
        if connection is not None:
            return connection.on_closed
    
    def on_info(self, worker, data):
        server = worker.socket_server
        data['socket'] = {'listen_on': server.address,
                          'read_timeout': server.timeout,
                          'active_connections': server.active_connections,
                          'received_connections': server.received}
        return data
