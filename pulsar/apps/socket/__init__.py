import pulsar
from pulsar import get_actor, AsyncIOStream


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
    
    def monitor_init(self, monitor):
        # First we create the socket we listen to
        cfg = self.cfg
        address = cfg.address
        # if the platform does not support multiprocessing sockets switch to
        # thread concurrency
        if not pulsar.platform.multiProcessSocket():
            cfg.concurrency = 'thread'
        # Socket in thread concurrency has no workers
        if cfg.concurrency == 'thread':
            cfg.set('workers', 0)
        monitor.num_actors = cfg.workers
        
    def monitor_start(self, monitor):
        address = self.cfg.address
        if address:
            socket = pulsar.create_socket(address,
                                          log=monitor.log,
                                          backlog=self.cfg.backlog)
        else:
            raise pulsar.ImproperlyConfigured('Could not open a socket. '
                                              'No address to bind to')
        monitor.set('socket', socket)
    
    def client_request(self, stream, client_address):
        '''Build a request instance from the connected stream and the client
address. This must be implemented by subclasses.'''
        raise NotImplementedError()
    
    def request_instance(self, worker, fd, events):
        client, client_address = worker.get('socket').accept()
        if client:
            stream = AsyncIOStream(socket=client)
            return self.client_request(stream, client_address)
             
    def worker_start(self, worker):
        self._register_handler(worker)

    def monitor_stop(self, monitor):
        if monitor.num_actors == 0:
            self._close_socket(monitor)
    
    def worker_stop(self, monitor):
        self._close_socket(monitor)

    ########################################################### INTERNALS
    def _register_handler(self, worker):
        '''Register the socket to the ioloop'''
        socket = worker.get('socket')
        worker.ioloop.add_handler(socket,
                                  worker.handle_fd_event,
                                  worker.ioloop.READ)
    
    def _close_socket(self, worker):
        socket = worker.get('socket')
        worker.ioloop.remove_handler(socket)
        socket.close(worker.log)
    