import errno
import socket
try:
    import ssl
except:
    ssl = None 

import pulsar
from pulsar.http.utils import close


__all__ = ['HttpHandler','HttpPoolHandler']




class Bind(pulsar.Setting):
    app = 'wsgi'
    name = "bind"
    section = "Server Socket"
    cli = ["-b", "--bind"]
    meta = "ADDRESS"
    validator = validate_string
    default = "127.0.0.1:{0}".format(DEFAULT_PORT)
    desc = """\
        The socket to bind.
        
        A string of the form: 'HOST', 'HOST:PORT', 'unix:PATH'. An IP is a valid
        HOST.
        """
        
        
class Backlog(pulsar.Setting):
    app = 'wsgi'
    name = "backlog"
    section = "Server Socket"
    cli = ["--backlog"]
    validator = validate_pos_int
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



class HttpHandler(object):
    '''Handle HTTP requests and delegate the response to the worker'''
    ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED,
                      errno.EWOULDBLOCK, errno.EPIPE)
    ssl_options = None

    def __init__(self, worker):
        self.worker = worker
        
    def __call__(self, fd, events):
        client = None
        try:
            client, addr = self.worker.socket.accept()
            client.setblocking(1)
            req = self.worker.http.Request(client,
                                           addr,
                                           self.worker.address,
                                           self.worker.cfg)
        except socket.error as e:
            close(client)
            if e.errno not in self.ALLOWED_ERRORS:
                raise
            else:
                return
        except StopIteration:
            close(client)
            self.worker.log.debug("Ignored premature client disconnection.")
            return
        
        self.handle(fd, req)

    def handle(self, fd, req):
        self.worker.handle_task(fd, req)
        
        
class AsyncHttpHandler(HttpHandler):
    
    def __call__(self, fd, events):
        client = None
        try:
            client, addr = self.worker.socket.accept()
        except socket.error as e:
            close(client)
            if e.errno not in self.ALLOWED_ERRORS:
                raise
            else:
                return
        except StopIteration:
            close(client)
            self.worker.log.debug("Ignored premature client disconnection.")
            return
        
        stream = pulsar.IOStream(worker, client)
        req = http.Request(stream, addr, self.worker.address, self.worker.cfg)
        self.handle(fd, req)

    def handle(self, fd, req):
        self.worker.handle_task(fd, req)
        

class HttpPoolHandler(HttpHandler):
    '''THis is used when the monitor is using thread-based workes.'''
    def handle(self, fd, req):
        self.worker.task_queue.put((fd,req))

            
    
