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
To control how long a :class:`pulsar.AsyncConnection` is kept alive after the
last read from the remote client, one can use the
:ref:`keepalive <setting-keep_alive>` setting.

    python script.py --keep-alive 10
    
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


class Bind(pulsar.Setting):
    section = "Socket Servers"
    app = 'socket'
    name = "bind"
    flags = ["-b", "--bind"]
    meta = "ADDRESS"
    default = "127.0.0.1:{0}".format(pulsar.DEFAULT_PORT)
    can_prefix = True
    desc = """\
        The socket to bind.
        
        A string of the form: ``HOST``, ``HOST:PORT``, ``unix:PATH``.
        An IP is a valid HOST.
        """
        
class KeepAlive(pulsar.Setting):
    section = "Socket Servers"
    app = 'socket'
    name = "keep_alive"
    flags = ["--keep-alive"]
    validator = pulsar.validate_pos_int
    type = int
    default = 15
    can_prefix = True
    desc = """\
        The number of seconds to keep an idle client connection
        open."""


class SocketServer(pulsar.Application):
    '''This application bind a socket to a given address and listen for
requests. The request handler is constructued from the
:attr:`handler` parameter/attribute.
    
.. attribute:: address

    The socket address, available once the application has started.
    '''
    name = 'socket'
    address = None
    cfg = pulsar.Config(apps=['socket'])
    
    def protocol_consumer(self):
        '''Returns the callable application handler which is stored in
:attr:`Worker.app_handler`, used by :class:`Worker` to carry out its task.
By default it returns the :attr:`Application.callable`.'''
        return self.callable
    
    def monitor_start(self, monitor):
        # if the platform does not support multiprocessing sockets set
        # the number of workers to 0.
        cfg = self.cfg
        if not pulsar.platform.has_multiProcessSocket\
            or cfg.concurrency == 'thread':
            cfg.set('workers', 0)
        # Open the socket and bind to address
        address = self.cfg.address
        if address:
            sock = pulsar.create_socket(address, bindto=True,
                                        backlog=self.cfg.backlog)
        else:
            raise pulsar.ImproperlyConfigured('Could not open a socket. '
                                              'No address to bind to')
        self.logger.info('Listening on %s', sock)
        monitor.params.sock = sock
        self.address = sock.address
    
    def worker_start(self, worker):
        # Start the worker by starting the socket server
        worker.servers[self.name] = self.create_server(worker)
    
    def on_info(self, worker, data):
        server = worker.socket_server
        data['socket'] = {'listen_on': server.address,
                          'read_timeout': server.timeout,
                          'active_connections': server.active_connections,
                          'received_connections': server.received}
        return data

    def create_server(self, worker):
        '''Create the Server Protocol which will listen for requests. It
uses the :meth:`handler` as its response protocol.'''
        cfg = self.cfg
        server = worker.event_loop.create_server(
                                      sock=worker.params.sock,
                                      consumer_factory=self.protocol_consumer(),
                                      max_connections=cfg.max_requests,
                                      timeout=cfg.keep_alive)
        server.bind_event('connection_made', cfg.connection_made)
        server.bind_event('pre_request', cfg.pre_request)
        server.bind_event('post_request', cfg.post_request)
        server.bind_event('connection_lost', cfg.connection_lost)
        return server
        