'''An application for asynchronous applications serving requests
on a socket.
An application for asynchronous applications serving requests
on a socket. This is the base class of :class:`pulsar.apps.wsgi.WSGIServer`.

.. attribute:: socket_server_class

    Class or callable which returns the asynchronous socket server, usually
    a subclass of :class:`pulsar.AsyncSocketServer`.
    
.. attribute:: address

    The socket address, available once the application has started.
    
'''
import pulsar
from pulsar import AsyncIOStream


class SocketSetting(pulsar.Setting):
    virtual = True
    app = 'socket'
    
    
class Bind(SocketSetting):
    name = "bind"
    flags = ["-b", "--bind"]
    meta = "ADDRESS"
    default = "127.0.0.1:{0}".format(pulsar.DEFAULT_PORT)
    desc = """\
        The socket to bind.
        
        A string of the form: 'HOST', 'HOST:PORT', 'unix:PATH'. An IP is a valid
        HOST.
        """
        

class SocketServer(pulsar.Application):
    _app_name = 'socket'
    socket_server_class = None
    address = None
    
    def monitor_start(self, monitor):
        # if the platform does not support multiprocessing sockets set
        # the number of workers to 0.
        cfg = self.cfg
        if not self.socket_server_class:
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
        s = self.socket_server_class(worker, worker.params.socket)
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
