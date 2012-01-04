"""
Pulsar is shipped with an HTTP :class:`pulsar.apps.Application` which conforms
with the python web server gateway interface (WSGI_).

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


For more information regarding WSGI check the pep3333_ specification.

.. _pep3333: http://www.python.org/dev/peps/pep-3333/
.. _WSGI: http://www.wsgi.org
"""
from inspect import isclass

import pulsar
from pulsar.net import HttpResponse
from pulsar.utils.importer import module_attribute

from .handlers import *
from .wsgi import *
from . import middleware


class WSGIApplication(pulsar.Application):
    '''A WSGI application running on pulsar concurrent framework.
It can be configured to run as a multiprocess or a multithreaded server.'''
    app = 'wsgi'
    _name = 'wsgi'
    
    def handler(self):
        callable = self.callable
        if getattr(callable,'wsgifactory',False):
            callable = callable()
        return self.wsgi_handler(callable)
    
    def wsgi_handler(self, hnd, resp_middleware = None):
        '''Build the wsgi handler'''
        if not isinstance(hnd,WsgiHandler):
            if not isinstance(hnd,(list,tuple)):
                hnd = [hnd]
            hnd = WsgiHandler(hnd)
        response_middleware = self.cfg.response_middleware or []
        for m in response_middleware:
            if '.' not in m:
                mm = getattr(middleware,m,None)
                if not mm:
                    raise ValueError('Response middleware "{0}" not available'\
                                     .format(m))
            else:
                mm = module_attribute(m)
            if isclass(mm):
                mm = mm()
            hnd.response_middleware.append(mm)
        if resp_middleware:
            hnd.response_middleware.extend(resp_middleware)
        return hnd
    
    def concurrency(self, cfg):
        if not pulsar.platform.multiProcessSocket():
            return 'thread'
        else:
            return cfg.concurrency
    
    def get_ioqueue(self):
        '''If the concurrency is thread we create a task queue for processing
requests in the threaded workers.''' 
        if self.concurrency(self.cfg) == 'process':
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
        concurrency = self.concurrency(cfg)
        mt = concurrency == 'thread' and cfg.workers > 1
        mp = concurrency == 'process' and cfg.workers > 1
        environ = request.wsgi_environ(actor = worker,
                                       multithread = mt,
                                       multiprocess = mp)
        if not environ:
            yield request.on_body
            environ = request.wsgi_environ(
                                       actor = worker,
                                       multithread = mt,
                                       multiprocess = mp)
        # Create the response object
        response = HttpResponse(request)
        if environ:
            # Get the data from the WSGI handler
            try:
                data = worker.app_handler(environ, response.start_response)
            except Exception as e:
                # An exception in the handler
                data = WsgiResponse(environ = environ)
                cfg.handle_http_error(data, e)
                data(environ, response.start_response)
            yield response.write(data)
        yield response
    
    def monitor_init(self, monitor):
        # First we create the socket we listen to
        address = self.cfg.address
        if address:
            socket = pulsar.create_socket(address, log = monitor.log,
                                          backlog = self.cfg.backlog)
        else:
            raise pulsar.ImproperlyConfigured('\
 WSGI application with no address for socket')
        self.address = socket.name
        self.local['socket'] = socket
        
    def monitor_start(self, monitor):
        '''If the concurrency model is thread, a new handler is
added to the monitor event loop which listen for requests on
the socket. Otherwise the monitor has no handlers since requests are
directly handled by the workers.'''
        # We have a task queue, This means the monitor itself listen for
        # requests on the socket and delegate the handling to the
        # workers
        socket = self.local.get('socket')
        if monitor.ioqueue is not None or not monitor.num_actors:
            monitor.log.info(socket.info())
            handler = HttpPoolHandler(monitor,socket) if monitor.num_actors\
                      else HttpHandler(monitor,socket)
            monitor.ioloop.add_handler(socket,
                                       handler,
                                       monitor.ioloop.READ)
        else:
            # put the socket in the parameters to be passed to workers
            monitor.set('socket',socket)
            
    def monitor_stop(self, monitor):
        if monitor.ioqueue is not None:
            socket = self.local['socket']
            monitor.ioloop.remove_handler(socket)
            socket.close(monitor.log)


def createServer(callable = None, **params):
    return WSGIApplication(callable = callable, **params)
    
