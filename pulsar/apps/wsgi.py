import sys
import traceback
import errno
import socket

import pulsar
from pulsar.http.utils import write_nonblock, write_error, close


class WsgiMonitor(pulsar.WorkerMonitor):
    '''A specialized worker monitor for wsgi applications.'''
    def set_socket(self, socket):
        if not self.task_queue:
            self._listening = False
        super(WsgiMonitor,self).set_socket(socket)


class WSGIApplication(pulsar.Application):
    monitor_class = WsgiMonitor
    
    def get_task_queue(self): 
        if self.cfg.concurrency == 'process':
            return None
        else:
            return pulsar.ThreadQueue()
        
    def handle_event_task(self, worker, request):
        response, environ = request.wsgi(worker = worker)
        response.force_close()
        try:
            return response, worker.app_handler(environ, response.start_response)
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
            pass
        finally:    
            close(response.client_sock)


def createServer(callable = None, **params):
    return WSGIApplication(callable = callable, **params)
    

