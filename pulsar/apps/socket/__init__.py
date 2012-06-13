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
    
    def monitor_init(self, monitor):
        # First we create the socket we listen to
        cfg = self.cfg
        address = cfg.address
        # if the platform does not support multiprocessing sockets switch to
        # thread concurrency
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
    
    def request_instance(self, worker, fd, events):
        return worker.get('socket_server').accept()
             
    def worker_start(self, worker):
        # Start the worket by starting the socket server
        if not self.socket_server_class:
            raise TypeError('Socket server class not specified.')
        socket = worker.get('socket')
        s = self.socket_server_class(worker, socket).start()
        # We add the file descriptor handler
        s.on_connection_callbacks.append(worker.handle_fd_event)
        worker.set('socket_server',s)

    def monitor_stop(self, monitor):
        if monitor.num_actors == 0:
            self._close_socket(monitor)
    
    def worker_stop(self, monitor):
        self._close_socket(monitor)
        
    def handle_request(self, worker, connection):
        return connection.on_closed

    ########################################################### INTERNALS    
    def _close_socket(self, worker):
        socket = worker.get('socket')
        worker.ioloop.remove_handler(socket)
        socket.close(worker.log)
    