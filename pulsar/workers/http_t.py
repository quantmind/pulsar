import pulsar
import pulsar.workers.sync as sync

from .http import HttpMixin
from .base import WorkerThread


class Worker(WorkerThread,HttpMixin):
    '''A Http worker on a child process'''
    
    def _run(self, ioloop = None):
        ioloop = self.ioloop
        ioloop.add
        if ioloop.add_handler(self.socket, self._handle_events, IOLoop.READ):
            self.socket.setblocking(0)
            self.http = get_httplib(self.cfg)
            ioloop.start()