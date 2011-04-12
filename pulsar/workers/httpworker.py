from pulsar.http import get_httplib

from .base import WorkerProcess
from .http import HttpMixin



class Worker(WorkerProcess,HttpMixin):
    '''A Http worker on a child process'''
    
    def _run(self, ioloop = None):
        ioloop = self.ioloop
        if ioloop.add_handler(self.socket, self._handle_events, IOLoop.READ):
            self.socket.setblocking(0)
            self.http = get_httplib(self.cfg)
            ioloop.start()