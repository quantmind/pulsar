import os
import sys
import time
from multiprocessing import Pipe

from pulsar import getLogger
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.defer import RemoteProxy
from pulsar.utils.exceptions import PulsarPoolAlreadyStarted
from pulsar.http import get_httplib


__all__ = ['WorkerPool',
           'PoolWorkerProxy']
        
        
class PoolWorkerProxy(RemoteProxy):
    '''A specialization of a :class:`RemoteProxy`. This class does not need to
be serializable since it stays in the workerpool process domain.'''
    remotes = ('stop',)
    
    def __init__(self, arbiter, worker, connection, log = None):
        super(PoolWorkerProxy,self).__init__(connection,log=log)
        self.arbiter = arbiter
        self.worker = worker
        self.notified = time.time()
    
    def __repr__(self):
        return self.worker.__repr__()
    
    def __str__(self):
        return self.worker.__str__()
    
    def is_alive(self):
        return self.worker.is_alive()
     
    def terminate(self):
        self.worker.terminate()

    def join(self, timeout = 0):
        self.worker.join(timeout)

    # proxy Functions    
    def proxy_notify(self, t):
        self.notified = t
        
    def proxy_server_info(self):
        '''Get server Info and send it back.'''
        return self.arbiter.info()


class HttpMixin(object):
    
    @property
    def http(self):
        return get_httplib(self.cfg)
    
    
class WorkerPool(HttpMixin):
    '''\
A pool of worker classes for performing asynchronous tasks and input/output

:parameter arbiter: the arbiter managing the pool. 
:parameter worker_class: a Worker class derived form :class:`pulsar.Worker`
:parameter num_workers: The number of workers in the pool.
:parameter app: The working application
:parameter timeout: Timeout in seconds for murdering unresponsive workers 
    '''
    WORKER_PROXY = PoolWorkerProxy
    INITIAL = 0X0
    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3
    status = {0x0:'not started',
              0x1:'started',
              0x2:'closed',
              0x3:'terminated'}
    
    def __init__(self,
                 arbiter,
                 worker_class,
                 num_workers,
                 app = None,
                 timeout = 30,
                 socket = None):
        self.__state = self.INITIAL
        self.arbiter = arbiter
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
    def ioloop(self):
        return self.arbiter.ioloop
    
    @property
    def multithread(self):
        return self.worker_class.is_thread()
    
    @property
    def multiprocess(self):
        return self.worker_class.is_process()
    
    @property
    def pid(self):
        return os.getpid()
    
    def arbiter_task(self):
        if self.is_alive():
            self.manage_workers()
            self.spawn_workers()
            self.stop_workers()
        elif self.started():
            self.manage_workers()
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
        proxy = self.WORKERS.pop(wid)
        proxy.close()
        
    def stop_worker(self, wid):
        proxy =  self.WORKERS[wid]
        if not proxy.is_alive():
            self.clean_worker(wid)
        else:
            return proxy.stop()
        
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
                for wid, proxy in list(iteritems(self.WORKERS)):
                    if not proxy.is_alive():
                        self.clean_worker(wid)
                    else:
                        proxy.terminate()
                
    def join(self, timeout = 1):
        '''Join the pool, close or terminate must have been called before.'''
        if not self.stopped():
            raise ValueError('Cannot join worker pool. Must be stopped or terminated first.')
        for wid, proxy in list(iteritems(self.WORKERS)):
            if not proxy.is_alive():
                self.clean_worker(wid)
            else:
                proxy.join(timeout)
                
    def manage_workers(self):
        """Murder workers to avoid zombie processes.
A worker is murdered when it has not notify itself for a period longer than
the pool timeout.
        """
        timeout = self.timeout
        for wid, proxy in list(iteritems(self.WORKERS)):
            if not proxy.is_alive():
                self.clean_worker(wid)
            else:
                if timeout:
                    gap = time.time() - proxy.notified
                    if gap > timeout:
                        self.log.info('Terminating worker {0}. Timeout surpassed.'.format(proxy))
                        proxy.terminate()
                        proxy.join()
                        continue
                proxy.flush()
        
    def stop_workers(self):
        """Maintain the number of workers by spawning or killing
as required."""
        num_to_kill = len(self.WORKERS) - self.num_workers
        for i in range(num_to_kill, 0, -1):
            kwid, kage = 0, sys.maxsize
            for wid, proxy in iteritems(self.WORKERS):
                age = proxy.worker.age
                if age < kage:
                    kwid, kage = wid, age
            self.stop_worker(kwid)
            
    def spawn_workers(self):
        """\
        Spawn new workers as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        while len(self.WORKERS) < self.num_workers:
            self.spawn_worker()
            
    def spawn_worker(self):
        '''Spawn a new worker'''
        self.worker_age += 1
        worker_class = self.worker_class
        worker_connection, pool_connection = Pipe()
        # Create the command queue
        command_queue = worker_class.CommandQueue()
        worker = worker_class(age = self.worker_age,
                              ppid = self.pid,
                              socket = self.socket,
                              app = self.app,
                              timeout = self.timeout/2.0,
                              cfg = self.cfg,
                              pool_connection = pool_connection,
                              command_queue = command_queue,
                              task_queue = self.task_queue)
        if self.cfg.pre_fork:
            self.cfg.pre_fork(worker)
        worker.start()
        if self.multiprocess:
            pool_connection.close()
        wid = worker.wid
        if wid != 0:
            self.WORKERS[wid] = self.WORKER_PROXY(self.arbiter,
                                                  worker,
                                                  worker_connection,
                                                  log = self.log)
        
    def info(self):
        return {'worker_class':self.worker_class.code(),
                'workers':len(self.WORKERS)}

