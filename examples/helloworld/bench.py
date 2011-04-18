from time import time
from threading import Thread
from multiprocessing import Process

import pulsar
from pulsar.http import HttpClient

from .manage import server


class RequestMixin(object):
    
    def __init__(self):
        self.time = None
        self.requests = 0
        self._done = False
        
    def request(self):
        c = HttpClient()
        t = time()
        r = 0
        while not self._done:
            c.request('http://localhost:8060')
            r += 1
        self.time = time() - t
        self.requests = r


class ThreadRequest(Thread,RequestMixin):
    
    def __init__(self):
        RequestMixin.__init__(self)
        Thread.__init__(self)
        self.daemon = True
        
    def run(self):
        self.request()
        

class Request(pulsar.WorkerThread):
    '''Callable object on a child process'''
        
    def _run(self):
        self.handler()    


class Config(object):
    
    def __init__(self, workers, num_threads):
        self.num_threads = num_threads
        self.workers = workers
        
    @property
    def worker_class(self):
        return Request
    
    def __getattr__(self, name):
        return None
        
        
class Arbiter(pulsar.Arbiter):
    
    def install_signals(self):
        pass
    
    
class Bench(pulsar.Application,RequestMixin):
    ArbiterClass = Arbiter
    
    def __init__(self, p, t):
        pulsar.Application.__init__(self,cfg = Config(p,t))
        RequestMixin.__init__(self)
    
    def handler(self):
        return self
    
    def load_config(self,**params):
        pass
    
    def configure_logging(self):
        pass
    
    def __call__(self):
        threads = []
        for i in range(self.cfg.num_threads):
            t = ThreadRequest()
            threads.append(t)
            t.start()
        self.request()
        tot_time = self.time
        tot_reqs = self.requests
        for t in threads:
            t._done = True
            t.join()
            tot_time += t.time
            tot_reqs = t.requests
        self.time = tot_time
        self.requests = tot_reqs
        
    def stop(self):
        self._done = True
        
        
class TestResponseSpeed(pulsar.test.TestCase):
        
    def testResponse100(self):
        b = Bench(1,10)
        b.add_timeout(time()+10,b.stop)
        b.start()
        t = b.time
        r = b.requests
        print('Total requests {0} in {0} seconds'.format(t,r))
        
