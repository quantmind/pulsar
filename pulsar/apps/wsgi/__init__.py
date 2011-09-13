'''
Pulsar is shipped with a Http applications which conforms the python
web server gateway interface (WSGI).

The application can be used in conjunction with several web frameworks
as well as the pulsar RPC handler in :mod:`pulsar.apps.rpc`.
'''
import sys
import traceback
import errno
import socket

import pulsar
from pulsar.http.utils import write_nonblock, write_error, close

from .http import *


class WsgiMonitor(pulsar.ApplicationMonitor):
    '''A specialized worker monitor for wsgi applications.'''
    def set_socket(self, socket):
        if not self.task_queue:
            self._listening = False
        super(WsgiMonitor,self).set_socket(socket)


class WSGIApplication(pulsar.Application):
    
    def get_task_queue(self): 
        if self.cfg.concurrency == 'process':
            return None
        else:
            return pulsar.ThreadQueue()
        
    def update_worker_paramaters(self, monitor, params):
        if not monitor.task_queue:
            params['socket'] = monitor.socket
        return params
        
    def worker_start(self, worker):
        # If the worker is a process and it is listening to a socket
        # Add the socket handler to the event loop, otherwise do nothing.
        # The worker will receive requests on a task queue
        if worker.socket:
            worker.socket.setblocking(False)
            handler = HttpHandler(worker)
            worker.ioloop.add_handler(worker.socket,
                                      handler,
                                      worker.ioloop.READ)
        
    def handle_event_task(self, worker, request):
        response, environ = request.wsgi(worker = worker)
        response.force_close()
        try:
            return response, worker.app_handler(environ,
                                                response.start_response)
        except socket.error as e:
            if e[0] != errno.EPIPE:
                worker.log.exception("Error processing request.")
            else:
                worker.log.debug("Ignoring EPIPE")
            return response,None
        except Exception as e:
            worker.log.error("Error processing request: {0}".format(e),
                             exc_info = sys.exc_info())
            if not worker.debug:
                msg = "HTTP/1.1 500 Internal Server Error\r\n\r\n"
            else:
                msg = traceback.format_exc()
            return response,[msg]

    def end_event_task(self, worker, response, result):
        try:
            if result:
                for item in result:
                    response.write(item)
            response.close()
        except socket.error as e:
            worker.log.error("Error processing request: {0}".format(e))
        finally:    
            close(response.client_sock)
            
    def monitor_start(self, monitor):
        '''If the concurrency model is thread, a new handler is
added to the monitor event loop which listen for requests on
the socket.'''
        # We have a task queue, This means the monitor itself listen for
        # requests on the socket and delegate the handling to the
        # workers
        address = self.cfg.address
        if address:
            socket = pulsar.create_socket(address, log = monitor.log)
        else:
            raise pulsar.ImproperlyConfigured('\
 WSGI application with no address for socket')
        
        if monitor.task_queue is not None:
            monitor.set_socket(socket)
            monitor.ioloop.add_handler(monitor.socket,
                                       HttpPoolHandler(monitor),
                                       monitor.ioloop.READ)
        else:
            monitor.socket = socket
            
            
    def monitor_stop(self, monitor):
        if monitor.task_queue is not None:
            monitor.ioloop.remove_handler(monitor.socket)


def createServer(callable = None, **params):
    return WSGIApplication(callable = callable, **params)
    
