import time

import pulsar
from pulsar.utils import system

from .base import Runner
from .workerpool import WorkerPool


__all__ = ['Arbiter']


class Arbiter(Runner):
    '''An Arbiter is an object which controls pools of workers'''
    CLOSE_TIMEOUT = 3
        
    def __init__(self, app):
        self.pid = None
        self.socket = None
        self.app = app
        self.cfg = app.cfg
        self.pidfile = None
        self.reexec_pid = 0
        self._pools = []
        self.address = self.cfg.address
        self.debug = self.cfg.debug
        # Create the listener if not available
        if not self.socket and self.address:
            self.socket = system.create_socket(self)
        self.log = self.getLogger()
        self.ioloop = self.get_eventloop()
        self.ioloop.add_loop_task(self)
        
    def setup(self):
        self.addpool(self.cfg, self.socket)
        
    def __repr__(self):
        return self.__class__.__name__
    
    def __str__(self):
        return self.__repr__()
    
    def __call__(self):
        sig = self.arbiter()
        if sig is None:
            for pool in self._pools:
                pool.arbiter_task()
        
    def arbiter(self):
        return None
    
    def addpool(self, cfg, socket = None, start = False):
        worker_class = cfg.worker_class
        pool = WorkerPool(self,
                          worker_class,
                          cfg.workers,
                          app = self.app,
                          timeout = cfg.timeout,
                          socket = socket)
        self._pools.append(pool)
        if start:
            pool.start()
    
    def start(self):
        self.run()
    
    def run(self):
        self.init_process()
        
    def _run(self):
        self.ioloop.start()
        
    def is_alive(self):
        return self.ioloop.running()
    
    def stop(self):
        '''Alias of :meth:`close`'''
        self.close()
        
    def close(self):
        '''Stop the pools and the arbiter event loop.'''
        if self._pools:
            for pool in self._pools:
                pool.close()
                
            #timeout = self.CLOSE_TIMEOUT / float(len(self._pools))
            timeout = self.CLOSE_TIMEOUT
            for pool in self._pools:
                pool.join(timeout)
    
    def terminate(self):
        '''Force termination of pools and close arbiter event loop'''
        for pool in self._pools:
            pool.terminate()
        
    def server_info(self):
        started = self.started
        if not started:
            return
        uptime = time.time() - started
        server = {'uptime':uptime,
                  'version':pulsar.__version__,
                  'name':pulsar.SERVER_NAME,
                  'number_of_pools':len(self._pools),
                  'event_loops':self.ioloop.num_loops,
                  'socket':str(self.socket)}
        pools = []
        for p in self._pools:
            pools.append(p.info())
        return {'server':server,
                'pools':pools}