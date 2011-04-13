import pulsar

from .http import HttpHandler, HttpMixin, get_httplib
from .base import WorkerThread
from .task import get_task_loop, start_task_loop


class HttpPoolHandler(HttpHandler):
    
    def handle(self, fd, *args):
        request = self.worker.request(*args)
        self.worker.putRequest(request)


class Worker(WorkerThread,HttpMixin):
    '''A Http worker on a thread. This worker process http requests from the
pool queue.'''
    worker_name = 'Worker.HttpThread'
    
    def get_ioimpl(self):
        return get_task_loop(self)
    
    def _run(self):
        start_task_loop(self)
    
    @classmethod
    def modify_arbiter_loop(cls, wp, ioloop):
        '''The arbiter listen for client connections and delegate the handling
to the Thread Pool. This is different from the Http Worker on Processes'''
        if wp.socket:
            ioloop.add_handler(wp.socket,
                               HttpPoolHandler(wp),
                               ioloop.READ)
    
    