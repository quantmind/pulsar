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
        
class Backlog(SocketSetting):
    name = "backlog"
    flags = ["--backlog"]
    validator = pulsar.validate_pos_int
    type = int
    default = 2048
    desc = """\
        The maximum number of pending connections.    
        
        This refers to the number of clients that can be waiting to be served.
        Exceeding this number results in the client getting an error when
        attempting to connect. It should only affect servers under significant
        load.
        
        Must be a positive integer. Generally set in the 64-2048 range.    
        """

class SocketServer(pulsar.Application):
        
    def monitor_init(self, monitor):
        # First we create the socket we listen to
        cfg = self.cfg
        address = cfg.address
        if not pulsar.platform.multiProcessSocket():
            cfg.concurrency = 'thread'
        # Socket not in threaded concurrency has no workers
        if cfg.concurrency == 'thread':
            cfg.set('workers',0)
        monitor.num_actors = cfg.workers 
        if address:
            socket = pulsar.create_socket(address, log=monitor.log,
                                          backlog=self.cfg.backlog)
        else:
            raise pulsar.ImproperlyConfigured('Could not open a socket. '
                                              'No address to bind to')
        self.address = socket.name
        # put the socket in the parameters to be passed to workers
        monitor.set('socket', socket)
    
    def stream_request(self, stream, client_address):
        raise NotImplementedError()
    
    def on_event(self, fd, events):
        worker = get_actor()
        client, client_address = actior.get('socket').accept()
        if client:
            stream = AsyncIOStream(socket=client)
            request = self.stream_request(stream, client_address)
            self.handle_request(worker, request)
            
    def register_handler(self, worker):
        socket = worker.get('socket')
        worker.ioloop.add_handler(socket,
                                  self.handle_fd_event,
                                  worker.ioloop.READ)
    def close_socket(self, worker):
        socket = worker.get('socket')
        worker.ioloop.remove_handler(socket)
        socket.close(worker.log)
    
    def monitor_start(self, monitor):
        if monitor.num_actors == 0:
            self.register_handler(monitor)
            
    def worker_start(self, worker):
        self.register_handler(worker)

    def monitor_stop(self, monitor):
        if monitor.num_actors == 0:
            self.close_socket(monitor)
    
    def worker_stop(self, monitor):
        self.close_socket(monitor)
