import errno

from pulsar import http, IOStream, AsyncIOStream
from pulsar.http.utils import close


__all__ = ['HttpHandler','HttpPoolHandler']


class HttpHandler(object):
    '''Handle HTTP requests and delegate the response to the worker'''
    ALLOWED_ERRORS = (errno.EAGAIN, errno.ECONNABORTED,
                      errno.EWOULDBLOCK, errno.EPIPE)

    def __init__(self, worker):
        self.worker = worker
        self.iostream = IOStream if self.worker.cfg.synchronous else\
                        AsyncIOStream
        
    def __call__(self, fd, events):
        client, addr = self.worker.socket.accept()
        stream = self.iostream(socket = client)
        self.handle(http.HttpRequest(stream, addr))

    def handle(self, request):
        self.worker.handle_task(request)


class HttpPoolHandler(HttpHandler):
    '''This is used when the monitor is using thread-based workers.'''
    def handle(self, request):
        self.worker.task_queue.put(request)

            
    
