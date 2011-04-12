# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import os
import random
import signal
import sys
import time
import tempfile
try:
    import queue
except ImportError:
    import Queue as queue
ThreadQueue = queue.Queue

from multiprocessing import Process
from multiprocessing.queues import Queue, Empty
from threading import current_thread, Thread

import pulsar
from pulsar import getLogger
from pulsar.utils.eventloop import IOLoop
from pulsar.utils import system, threadpool

from .workerpool import STOP_WORKER


__all__ = ['Runner',
           'Worker',
           'WorkerProcess',
           'WorkerThreadPool']


class Runner(object):
    '''Base class for classes with an event loop.
    '''
    DEF_PROC_NAME = 'pulsar'
    SIG_QUEUE = None
    
    def init_process(self):
        '''Initialise the runner. This function
will block the current thread since it enters the event loop.
If the runner is a instance of a subprocess, this function
is called after fork by the run method.'''
        self.log = getLogger(self.__class__.__name__)
        self.ioloop = IOLoop(impl = self.get_ioimpl(), logger = self.log)
        self.set_proctitle()
        self.setup()
        self.install_signals()
        self._run()
        
    def set_proctitle(self):
        '''Set the process title'''
        if not self.isthread:
            if self.cfg:
                proc_name = self.cfg.proc_name
            else:
                proc_name = self.DEF_PROC_NAME
            system.set_proctitle("{0} - {1}".format(proc_name,self))
        
    def install_signals(self):
        '''Initialise signals for correct signal handling.'''
        if not self.isthread:
            self.log.debug('Installing signals')
            sfun = getattr(self,'signal',None)
            for name in system.ALL_SIGNALS:
                func = getattr(self,'handle_{0}'.format(name),sfun)
                if func:
                    sig = getattr(signal,'SIG{0}'.format(name))
                    signal.signal(sig, func)
    
    def setup(self):
        pass
    
    def _run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()
    
    @property
    def tid(self):
        '''Thread Name'''
        if self.isthread:
            return self.name
        else:
            return current_thread().name
    
    @property
    def isthread(self):
        return isinstance(self,Thread)
        

class ArbiterBase(Runner):
    
    def start(self):
        self.run()
    
    def run(self):
        self.init_process()
        
    def is_alive(self):
        return self.ioloop.running()
    

class Worker(Runner):
    """\
Base class for all workers. The constructor is called
called pre-fork so it shouldn't do anything to the current process.
If there's a need to make process wide changes you'll want to do that
in ``self.init_process()``.

.. attribute:: age

    The age of worker, used to access how long the worker has been created.
    
.. attribute:: pid

    The worker process id.
    
.. attribute:: ppid

    The worker parent process id.
    
.. attribute:: tid

    The worker thread id.
    
.. attribute:: wid

    The worker unique id. If the Worker has not started it is ``None``.
"""
    COMMAND_TIMEOUT = 0
    CommandQueue = None
    
    def __init__(self,
                 age = 0,
                 ppid = None,
                 socket = None,
                 app = None,
                 pool_writer = None,
                 timeout = None,
                 command_queue = None,
                 cfg = None,
                 logger = None,
                 command_timeout = None,
                 **kwargs):
        self.age = age
        self.ppid = ppid
        self.nr = 0
        self.max_requests = getattr(cfg,'max_requests',None) or sys.maxsize
        self.debug = getattr(cfg,'debug',False)
        self.timeout = timeout
        self.cfg = cfg
        self.command_queue = command_queue
        self.pool_writer = pool_writer
        self.COMMAND_TIMEOUT = command_timeout if command_timeout is not None else self.COMMAND_TIMEOUT
        self.set_listener(socket, app)
        
    def _run(self):
        self.ioloop.start()
            
    def check_pool_commands(self):
        while True:
            try:
                c = self.command_queue.get(timeout = self.COMMAND_TIMEOUT)
            except Empty:
                break
            if c == STOP_WORKER:
                self.command_queue.close()
                self.ioloop.stop()
                break
        
    def get_ioimpl(self):
        '''Return the event-loop implementation. By default it returns ``None``.'''
        return None
    
    def set_listener(self, socket, app):
        self.socket = socket
        self.address = None if not socket else socket.getsockname()
        self.app = app
    
    def __str__(self):
        return "<{0} {1}>".format(self.__class__.__name__,self.wid)
    
    def check_num_requests(self, nr):
        max_requests = self.max_requests
        if max_requests and nr >= self.max_requests:
            self.log.info("Auto-restarting worker after current request.")
            self.alive = False
    
    def notify(self):
        """\
        Your worker subclass must arrange to have this method called
        once every ``self.timeout`` seconds. If you fail in accomplishing
        this task, the master process will murder your workers.
        """
        self.pool_writer.send(time.time())

    def reseed(self):
        pass
    
    def setup(self):
        if self.cfg:
            system.set_owner_process(self.cfg.uid, self.cfg.gid)
        self.reseed()
        self.log.info('Booting worker "{0}"'.format(self.wid))
        self.handler = self.app.handler()
        self.add_loop_tasks()
        if self.cfg:
            self.cfg.post_fork(self)
        
    def add_loop_tasks(self):
        # Add the notify task
        if self.pool_writer is not None:
            self.ioloop.add_loop_task(self.notify)
        # Add the check pool commands to the loop tasks
        if self.command_queue is not None:
            self.ioloop.add_loop_task(self.check_pool_commands)
    
    def handle(self, *args):
        self.handle_loop_event(*args)
    
    def handle_loop_event(self):
        raise NotImplementedError
    
    def handle_int(self, sig, frame):
        signame = system.SIG_NAMES.get(sig,None)
        self.log.info('Received signal {0}. Exiting.'.format(signame))
        self.alive = False
        
    def handle_quit(self, sig, frame):
        signame = system.SIG_NAMES.get(sig,None)
        self.log.info('Received signal {0}. Exiting.'.format(signame))
        self.alive = False
    
    def get_parent_id(self):
        return os.getpid()
    
    @property
    def wid(self):
        return '{0}-{1}'.format(self.pid,self.tid)



def runworker(self):
    """Called in the subprocess, if this is a subprocess worker, or
in thread if a Thread Worker."""
    try:
        self.init_process()
    except SystemExit:
        raise
    except Exception as e:
        self.log.exception("Exception in worker {0}: {1}".format(self,e))
    finally:
        self.log.info("Worker exiting {0}".format(self))
        try:
            self.cfg.worker_exit(self)
        except:
            pass



class WorkerThreadPool(Worker):
    '''A :class:`pulsar.Worker` With an associated pool of threads.
This worker does not span a new process.'''
    NUM_THREADS = 5
    
    def __init__(self, *args, **kwargs):
        super(WorkerThreadPool,self).__init__(*args, **kwargs)
        self.ioloop = kwargs['ioloop']
        
    def start(self):
        self.pool = threadpool.ThreadPool(self.NUM_THREADS)
        self.run()
        
    def run(self):
        self.init_process()
    
    def install_signals(self):
        pass
    
    def set_proctitle(self):
        pass
    
    def stop(self):
        self.pool.dismissWorkers()
    
    def join(self, timeout = 0):
        self.pool.joinAllDismissedWorkers()
        
    def terminate(self):
        self.join()
        
    def is_alive(self):
        return len(self.pool.workers)
    
    def handle(self, *args):
        req = threadpool.makeRequests(self.handle_loop_event,args)
        self.pool.putRequest(req)

    
    
class WorkerProcess(Process,Worker):
    '''A :class:`pulsar.Worker` on a subprocess. This worker class
inherit from the :class:`multiprocessProcess` class.'''
    CommandQueue = Queue
    
    def __init__(self, **kwargs):
        Process.__init__(self)
        Worker.__init__(self, **kwargs)
        self.daemon = True
        
    def reseed(self):
        random.seed()
    
    def run(self):
        runworker(self)
    
    @property    
    def get_parent_id(self):
        return os.getppid()
    
    
    
class WorkerThread(Thread,Worker):
    CommandQueue = ThreadQueue
    CommandQueue = Queue
    
    def __init__(self, **kwargs):
        Thread.__init__(self)
        Worker.__init__(self, **kwargs)
        self.daemon = True
        
    def run(self):
        runworker(self)
        
    @property
    def pid(self):
        return os.getpid()
        
    @property
    def get_parent_id(self):
        return self.pid
    
