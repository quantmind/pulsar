"""
Pulsar is shipped with a HTTP applications which conforms the python
web server gateway interface (WSGI_).

The application can be used in conjunction with several web frameworks
as well as the :ref:`pulsar RPC middleware <apps-rpc>` and
the :ref:`websocket middleware <apps-ws>`.

An example of a web server written with ``pulsar.apps.wsgi`` which responds 
with "Hello World!" for every request:: 

    from pulsar.apps import wsgi
    
    def hello(environ, start_response):
        data = b"Hello World!"
        response_headers = (
            ('Content-type','text/plain'),
            ('Content-Length', str(len(data)))
        )
        start_response("200 OK", response_headers)
        return [data]
    
    if __name__ == '__main__':
        wsgi.createServer(callable = hello).start()


.. _WSGI: http://www.wsgi.org
"""
import pulsar
from pulsar.net import HttpResponse

from .handlers import *
from .wsgi import *


class WSGIApplication(pulsar.Application):
    '''A WSGI application running on pulsar concurrent framework.
It can be configured to run as a multiprocess or a multithreaded server.'''
    app = 'wsgi'
    _name = 'wsgi'
    
    def on_config(self):
        if not pulsar.platform.multiProcessSocket():
            self.cfg.set('concurrency','thread')
    
    def get_ioqueue(self):
        '''If the concurrency is thread we create a task queue for processing
requests in the threaded workers.''' 
        if self.cfg.concurrency == 'process':
            return None
        else:
            return pulsar.ThreadQueue()
        
    def worker_start(self, worker):
        # If the worker is listening to a socket
        # Add the socket handler to the event loop, otherwise do nothing.
        socket = worker.get('socket')
        if socket:
            worker.log.info(socket.info())
            socket.setblocking(False)
            worker.ioloop.add_handler(socket,
                                      HttpHandler(worker,socket),
                                      worker.ioloop.READ)
        
    def handle_request(self, worker, request):
        cfg = worker.cfg
        mt = cfg.concurrency == 'thread' and cfg.workers > 1
        mp = cfg.concurrency == 'process' and cfg.workers > 1
        environ = request.wsgi_environ(actor = worker,
                                       multithread = mt,
                                       multiprocess = mp)
        if not environ:
            yield request.on_headers
            environ = request.wsgi_environ(
                                       actor = worker,
                                       multithread = mt,
                                       multiprocess = mp)
        # Create the response object
        response = HttpResponse(request)
        # WSGI
        data = worker.app_handler(environ, response.start_response)
        yield response.write(data)
        yield response
            
    def monitor_start(self, monitor):
        '''If the concurrency model is thread, a new handler is
added to the monitor event loop which listen for requests on
the socket. Otherwise the monitor has no handlers since requests are
directly handled by the workers.'''
        # First we create the socket we listen to
        address = self.cfg.address
        if address:
            socket = pulsar.create_socket(address, log = monitor.log,
                                          backlog = self.cfg.backlog)
        else:
            raise pulsar.ImproperlyConfigured('\
 WSGI application with no address for socket')
        
        # We have a task queue, This means the monitor itself listen for
        # requests on the socket and delegate the handling to the
        # workers
        if monitor.ioqueue is not None:
            self.socket = socket
            monitor.log.info(socket.info())
            monitor.ioloop.add_handler(socket,
                                       HttpPoolHandler(monitor,socket),
                                       monitor.ioloop.READ)
        else:
            # put the socket in the parameters to be passed to workers
            monitor['socket'] = socket
            
    def monitor_stop(self, monitor):
        if monitor.ioqueue is not None:
            monitor.ioloop.remove_handler(self.socket)
            self.socket.close(monitor.log)


def createServer(callable = None, **params):
    return WSGIApplication(callable = callable, **params)
    
