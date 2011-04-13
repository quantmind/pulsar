import os
import time
from multiprocessing import Pipe

from pulsar.utils.py2py3 import iteritems
from pulsar.utils.exceptions import PulsarPoolAlreadyStarted

STOP_WORKER = b'STOP'


__all__ = ['WorkerPool']


class WorkerPool(object):
    '''\
A pool of worker classes for performing asynchronous tasks and input/output
 
:parameter worker_class: a Worker class derived form :class:`pulsar.Worker`
:parameter num_workers: The number of workers in the pool.
:parameter app: The working application
:parameter timeout: Timeout in seconds for murdering unresponsive workers 
    '''
    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3
    _state = None
    
    def __init__(self,
                 worker_class,
                 num_workers,
                 app = None,
                 timeout = 30,
                 socket = None):
        self.worker_class = worker_class
        self.num_workers = num_workers
        self.timeout = timeout
        self.worker_age = 0
        self.app = app
        if self.app:
            self.cfg = getattr(app,'cfg',None)
        else:
            self.cfg = None
        self.LISTENER = socket
        self.WORKERS = {}
        
    def is_alive(self):
        return self._state == self.RUN
    
    @property
    def pid(self):
        return os.getpid()
        
    def start(self):
        if not self._state:
            self._state = self.RUN
            self.spawn_workers()
        else:
            raise PulsarPoolAlreadyStarted('Already started')
            
    def close(self):
        '''Close the Pool by putting a STOP_WORKER command in the command queue
of each worker.'''
        if self._state < self.CLOSE:
            self._state = self.CLOSE
            for wid, proc in list(iteritems(self.WORKERS)):
                w = proc['worker']
                c = proc['command_queue']
                if not w.is_alive():
                    self.WORKERS.pop(wid)
                c.put(STOP_WORKER)
        
    def terminate(self):
        '''Force each worker to terminate'''
        self.close()
        if self._state < self.TERMINATE:
            for wid, proc in list(iteritems(self.WORKERS)):
                w = proc['worker']
                if not w.is_alive():
                    self.WORKERS.pop(wid)
                w.terminate()
            
    def kill(self, sig = None):
        """\
Kill all workers with the signal ``sig``

:parameter sig: `signal.SIG*` value
"""
        if sig == signal.SIGQUIT:
            self.close()
        else:
            self.terminate()
                
    def murder_workers(self):
        """Murder workers to avoid zombie processes.
A worker is murdered when it has not notify itself for a period longer than
the pool timeout.
        """
        if self.timeout:
            for wid, proc in list(iteritems(self.WORKERS)):
                worker = proc['worker']
                connection = proc['reader']
                notified = proc['notified']
                while connection.poll():
                    notified = connection.recv()
                proc['notified'] = notified
                gap = time.time() - notified
                if gap > self.timeout:
                    if worker.is_alive():
                        self.log.info('Terminating worker. Reason timeout surpassed.')
                        worker.terminate()
    
    def remove_workers(self):
        '''Remove not alive workers from the dictionary'''
        for wid, proc in list(iteritems(self.WORKERS)):
            if not proc['worker'].is_alive():
                self.WORKERS.pop(wid)
        
    def manage_workers(self):
        """Maintain the number of workers by spawning or killing
as required."""
        if len(self.WORKERS) < self.num_workers:
            self.spawn_workers()

        num_to_kill = len(self.WORKERS) - self.num_workers
        for i in range(num_to_kill, 0, -1):
            pid, age = 0, sys.maxsize
            for (wpid, worker) in iteritems(self.WORKERS):
                if worker.age < age:
                    pid, age = wpid, worker.age
            self.join_worker(pid)
            
    def spawn_workers(self):
        """\
        Spawn new workers as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        for i in range(self.num_workers - len(self.WORKERS)):
            self.spawn_worker()
            
    def spawn_worker(self):
        '''Spawn a new worker'''
        self.worker_age += 1
        worker_class = self.worker_class
        pool_reader, pool_writer = Pipe(duplex = False)
        # Create the command queue
        command_queue = worker_class.CommandQueue()
        worker = worker_class(age = self.worker_age,
                              ppid = self.pid,
                              socket = self.LISTENER,
                              app = self.app,
                              timeout = self.timeout/2.0,
                              cfg = self.cfg,
                              pool_writer = pool_writer,
                              command_queue = command_queue)
        if self.cfg:
            self.cfg.pre_fork(worker)
        worker.start()
        wid = worker.wid
        if wid != 0:
            self.WORKERS[wid] = {'worker':worker,
                                 'reader':pool_reader,
                                 'command_queue':command_queue,
                                 'notified':time.time()}
            
    def join_worker(self, wid):
        w = self.WORKERS[wid]['worker']
        w.stop()
        self.log.info("Joining {0}".format(w))
        w.join(self.JOIN_TIMEOUT)
                    
    def terminate_worker(self, wid):
        w = self.WORKERS.pop(wid)
        self.log.info("Terminating {0}".format(w))
        w['worker'].terminate()
        
    
    
    