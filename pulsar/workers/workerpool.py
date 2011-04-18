import os
import sys
import time
from multiprocessing import Pipe

from pulsar import getLogger
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.exceptions import PulsarPoolAlreadyStarted
from pulsar.http import get_httplib

STOP_WORKER = b'STOP'


__all__ = ['WorkerPool']


class HttpMixin(object):
    
    @property
    def http(self):
        return get_httplib(self.cfg)
    


class WorkerPool(HttpMixin):
    '''\
A pool of worker classes for performing asynchronous tasks and input/output
 
:parameter worker_class: a Worker class derived form :class:`pulsar.Worker`
:parameter num_workers: The number of workers in the pool.
:parameter app: The working application
:parameter timeout: Timeout in seconds for murdering unresponsive workers 
    '''
    INITIAL = 0X0
    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3
    status = {0x0:'not started',
              0x1:'started',
              0x2:'closed',
              0x3:'terminated'}
    
    def __init__(self,
                 ioloop,
                 worker_class,
                 num_workers,
                 app = None,
                 timeout = 30,
                 socket = None):
        self.__state = self.INITIAL
        self.ioloop = ioloop
        self.worker_class = worker_class
        self.num_workers = num_workers
        self.timeout = timeout or 0
        self.worker_age = 0
        self.app = app
        self.log = getLogger(worker_class.code())
        if self.app:
            self.cfg = getattr(app,'cfg',None)
        else:
            self.cfg = None
        if self.cfg:
            self.address = self.cfg.address
        else:
            self.address = None
        self.socket = socket
        self.task_queue = self.get_task_queue()
        self.WORKERS = {}
        
    def __str__(self):
        return '{0} - {1}'.format(self.worker_class.code(),self.status[self.__state])
        
    def __repr__(self):
        return self.__str__()
    
    def is_alive(self):
        return self.__state == self.RUN
    
    def started(self):
        return self.__state >= self.RUN
    
    def stopped(self):
        return self.__state >= self.CLOSE
    
    def putRequest(self, request):
        self.task_queue.put(request)
        
    def get_task_queue(self):
        return self.worker_class.CommandQueue()
    
    @property
    def pid(self):
        return os.getpid()
    
    def arbiter(self):
        if self.is_alive():
            self.murder_workers()
            self.manage_workers()
        elif self.started():
            self.murder_workers()
            #if not len(self.WORKERS) and self.__state == self.CLOSE:
            #    self.__state = self.INITIAL
        else:
            self.start()
            
    def start(self):
        if self.__state == self.INITIAL:
            self.worker_class.modify_arbiter_loop(self,self.ioloop)
            self.__state = self.RUN
            self.spawn_workers()
        else:
            raise PulsarPoolAlreadyStarted('Already started')
    
    def clean_worker(self, wid):
        proc = self.WORKERS.pop(wid)
        command = proc['command_queue']
        if hasattr(command,'close'):
            command.close()
            command.join_thread()
        
    def stop_worker(self, wid):
        proc =  self.WORKERS[wid]
        w = proc['worker']
        if not w.is_alive():
            self.clean_worker(wid)
        else:
            c = proc['command_queue']
            c.put(STOP_WORKER)
        
    def close(self):
        '''Close the Pool by putting a STOP_WORKER command in the command queue
of each worker.'''
        if self.started():
            if self.__state < self.CLOSE:
                self.__state = self.CLOSE
                if hasattr(self.task_queue,'close'):
                    self.task_queue.close()
                    self.task_queue.join_thread()
                self.worker_class.clean_arbiter_loop(self,self.ioloop)
                for wid in list(self.WORKERS):
                    self.stop_worker(wid)
        
    def terminate(self):
        '''Force each worker to terminate'''
        if self.started():
            self.close()
            if self.__state < self.TERMINATE:
                self.__state = self.TERMINATE
                for wid, proc in list(iteritems(self.WORKERS)):
                    w = proc['worker']
                    if not w.is_alive():
                        self.clean_worker(wid)
                    w.terminate()
                
    def join(self, timeout = 1):
        '''Join the pool, close or terminate must have been called before.'''
        if not self.stopped():
            raise ValueError('Cannot join worker pool. Must be stopped or terminated first.')
        for wid, proc in list(iteritems(self.WORKERS)):
            worker = proc['worker']
            if not worker.is_alive():
                self.clean_worker(wid)
            else:
                worker.join(timeout)
                
    def murder_workers(self):
        """Murder workers to avoid zombie processes.
A worker is murdered when it has not notify itself for a period longer than
the pool timeout.
        """
        timeout = self.timeout
        for wid, proc in list(iteritems(self.WORKERS)):
            worker = proc['worker']
            if not worker.is_alive():
                self.clean_worker(wid)
            elif timeout:
                connection = proc['reader']
                notified = proc['notified']
                while connection.poll():
                    notified = connection.recv()
                proc['notified'] = notified
                gap = time.time() - notified
                if gap > timeout:
                    self.log.info('Terminating worker {0}. Timeout surpassed.'.format(worker))
                    worker.terminate()
                    worker.join()
        
    def manage_workers(self):
        """Maintain the number of workers by spawning or killing
as required."""
        if len(self.WORKERS) < self.num_workers:
            self.spawn_workers()

        num_to_kill = len(self.WORKERS) - self.num_workers
        for i in range(num_to_kill, 0, -1):
            kwid, kage = 0, sys.maxsize
            for (wid, worker) in iteritems(self.WORKERS):
                if worker['worker'].age < kage:
                    kwid, kage = wid, worker['worker'].age
            self.stop_worker(kwid)
            
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
                              socket = self.socket,
                              app = self.app,
                              timeout = self.timeout/2.0,
                              cfg = self.cfg,
                              pool_writer = pool_writer,
                              command_queue = command_queue,
                              task_queue = self.task_queue)
        if self.cfg.pre_fork:
            self.cfg.pre_fork(worker)
        worker.start()
        wid = worker.wid
        if wid != 0:
            self.WORKERS[wid] = {'worker':worker,
                                 'reader':pool_reader,
                                 'command_queue':command_queue,
                                 'notified':time.time()}
        
    def info(self):
        return {'worker_class':self.worker_class.code(),
                'workers':len(self.WORKERS)}
    
    