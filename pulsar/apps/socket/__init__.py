'''An application for asynchronous applications serving requests
on a socket.'''
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
    '''An application for asynchronous applications serving requests
on a socket. This is the base class of :class:`pulsar.apps.wsgi.WSGIServer`.

.. attribute:: socket_server_class

    Class or callable which returns the asynchronous socket server, usually
    a subclass of :class:`pulsar.AsyncSocketServer`.
    
.. attribute:: address

    The socket address, available once the application has started.
    
'''
    _app_name = 'socket'
    socket_server_class = None
    address = None
    
    def monitor_init(self, monitor):
        # if the platform does not support multiprocessing sockets set
        # the number of workers to 0.
        cfg = self.cfg
        if not pulsar.platform.multiProcessSocket()\
            or cfg.concurrency == 'thread':
            cfg.set('workers', 0)
        monitor.num_actors = cfg.workers
        
    def monitor_start(self, monitor):
        # Open the socket and bind to address
        address = self.cfg.address
        if address:
            socket = pulsar.create_socket(address,
                                          log=monitor.log,
                                          backlog=self.cfg.backlog)
        else:
            raise pulsar.ImproperlyConfigured('Could not open a socket. '
                                              'No address to bind to')
        monitor.log.info('Listening on %s' % socket)
        monitor.set('socket', socket)
        self.address = socket.name
    
    def worker_start(self, worker):
        # Start the worker by starting the socket server
        if not self.socket_server_class:
            raise TypeError('Socket server class not specified.')
        socket = worker.get('socket')
        s = self.socket_server_class(worker, socket).start()
        # We add the file descriptor handler
        s.on_connection_callbacks.append(worker.handle_fd_event)
        worker.set('socket_server', s)
    
    def worker_stop(self, worker):
        s = worker.get('socket_server')
        if s:
            worker.ioloop.remove_handler(s)
            s.close()
        
    def on_event(self, worker, fd, events):
        connection = worker.get('socket_server').accept()
        return connection.on_closed
