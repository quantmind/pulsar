# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import os
import random
import signal
import sys
import time
try:
    import queue
except ImportError:
    import Queue as queue
ThreadQueue = queue.Queue

from multiprocessing import Process, current_process
from multiprocessing.queues import Queue, Empty
from threading import current_thread, Thread

import pulsar
from pulsar.utils.async import RemoteServer, IOLoop
from pulsar.utils import system

from .workerpool import HttpMixin


__all__ = ['Runner',
           'ThreadQueue',
           'Worker',
           'WorkerThread',
           'WorkerProcess']

_main_thread = current_thread()


class Runner(pulsar.PickableMixin):
    '''Base class for classes with an event loop.
    '''
    DEF_PROC_NAME = 'pulsar'
    SIG_QUEUE = None
    
    def init_process(self):
        '''Initialise the runner. This function
will block the current thread since it enters the event loop.
If the runner is a instance of a subprocess, this function
is called after fork by the run method.'''
        self.set_proctitle()
        self.setup()
        self.install_signals()
        self._run()
        
    def get_eventloop(self):
        return IOLoop(impl = self.get_ioimpl(), logger = pulsar.LogSelf(self,self.log))
        
    def get_ioimpl(self):
        '''Return the event-loop implementation. By default it returns ``None``.'''
        return None
    
    @property
    def started(self):
        if hasattr(self,'ioloop'):
            return self.ioloop._started
        
    def set_proctitle(self):
        '''Set the process title'''
        if not self.isthread and hasattr(self,'cfg'):
            proc_name = self.cfg.proc_name or self.cfg.default_proc_name
            if proc_name:
                system.set_proctitle("{0} - {1}".format(proc_name,self))
        
    def current_thread(self):
        '''Return the current thread'''
        return current_thread()
    
    def current_process(self):
        return current_process()
    
    def install_signals(self):
        '''Initialise signals for correct signal handling.'''
        current = self.current_thread()
        if current == _main_thread and not self.isthread:
            self.log.info('Installing signals')
            sfun = getattr(self,'signal',None)
            for name in system.ALL_SIGNALS:
                func = getattr(self,'handle_{0}'.format(name.lower()),sfun)
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
        


class Worker(Runner, RemoteServer, HttpMixin):
    """\
Base class for all workers. The constructor is called
called pre-fork so it shouldn't do anything to the current process.
If there's a need to make process wide changes you'll want to do that
in ``self.setup()``.

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
    
.. attribute:: task_queue

    The task queue where the worker pool add tasks to be processed by the worker.
    This queue is used by a subsets of workers only.
"""
    COMMAND_TIMEOUT = 0
    CommandQueue = None
    
    def __init__(self,
                 connection,
                 age = 0,
                 ppid = None,
                 socket = None,
                 app = None,
                 timeout = None,
                 cfg = None,
                 command_timeout = None,
                 task_queue = None,
                 **kwargs):
        RemoteServer.__init__(self,connection)
        self.pool = None
        self.age = age
        self.notified = time.time()
        self.ppid = ppid
        self.nr = 0
        self.max_requests = getattr(cfg,'max_requests',None) or sys.maxsize
        self.debug = getattr(cfg,'debug',False)
        self.timeout = timeout
        self.cfg = cfg
        self.task_queue = task_queue
        self.COMMAND_TIMEOUT = command_timeout if command_timeout is not None else self.COMMAND_TIMEOUT
        self.set_listener(socket, app)
    
    def remote_pool(self, pool):
        self.pool = pool
    remote_pool.ack = False
    
    def remote_stop(self):
        self.worker._stop()
    remote_stop.ack = False
    
    @classmethod
    def modify_arbiter_loop(cls, wp, ioloop):
        '''Called by an instance of :class:`pulsar.WorkerPool`, it modify the 
event loop of the arbiter if required.

:parameter wp: Instance of :class:`pulsar.WorkerPool`
:parameter ioloop: Arbiter event loop
'''
        pass
    
    @classmethod
    def clean_arbiter_loop(cls, wp, ioloop):
        pass
    
    @classmethod
    def is_thread(cls):
        return issubclass(cls,Thread)
    
    @classmethod
    def is_process(cls):
        return issubclass(cls,Process)
    
    def _run(self):
        self.ioloop.start()
    
    def _stop(self):
        if self.ioloop.running():
            self._pool_proxy.close()
            self.ioloop.stop()
        
    def set_listener(self, socket, app):
        self.socket = socket
        self.address = None if not socket else socket.getsockname()
        self.app = app
    
    def __str__(self):
        return "<{0} {1}>".format(self.__class__.__name__,self.wid)
    
    def check_num_requests(self):
        '''Check the number of requests. If they exceed the maximum number
stop the event loop and exit.'''
        max_requests = self.max_requests
        if max_requests and self.nr >= self.max_requests:
            self.log.info("Auto-restarting worker after current request.")
            self._stop()
    
    def setup(self):
        '''Called after fork, it set ups the application handler
and perform several post fork processing before starting the event loop.'''
        # if this is a thread, the pool proxy is not a proxy, it is the actual pool.
        # In this case we manually serialize it
        if self.is_thread():
            self.pool = self.pool.get_proxy(self)
        else:
            random.seed()
            if self.cfg:
                system.set_owner_process(self.cfg.uid, self.cfg.gid)
            
        self.log = self.getLogger()
        # Set self as the worker in the pool
        self.pool.worker(self)
        self.ioloop = self.get_eventloop()
        self.log.info('Booting worker "{0}"'.format(self.wid))
        # Get the Application handler
        self.handler = self.app.handler()
        self.handler.server_proxy = self.pool
        self.ioloop.add_loop_task(self)
        if self.cfg.post_fork:
            self.cfg.post_fork(self)
        
    def __call__(self):
        '''Tasks to be performed at each iteration of the event loop.
Notify the worker pool and flush the pipe.'''
        p = self.pool
        if p:
            p.notify(time.time())
            self.flush()                
        
    def handle_request(self, fd, req):
        '''Handle request. A worker class must implement the ``_handle_request``
method.'''
        self.nr += 1
        self.check_num_requests()
        self.cfg.pre_request(self, req)
        try:
            self._handle_request(req)
        finally:
            try:
                self.cfg.post_request(self, req)
            except:
                pass
    
    def signal_stop(self, sig, frame):
        signame = system.SIG_NAMES.get(sig,None)
        self.log.warning('Received signal {0}. Exiting.'.format(signame))
        self._stop()
        
    handle_int  = signal_stop
    handle_quit = signal_stop
    handle_term = signal_stop
    
    def get_parent_id(self):
        return os.getpid()
    
    @property
    def wid(self):
        return '{0}-{1}'.format(self.pid,self.tid)


def runworker(self):
    """Run the worker, in suprocess or therad."""
    try:
        self.init_process()
    except SystemExit:
        raise
    except Exception as e:
        self.log.exception("Exception in worker {0}: {1}".format(self,e))
    finally:
        self.log.info("exiting {0}".format(self))
        try:
            self.cfg.worker_exit(self)
        except:
            pass
        
def updaterequests(f):
    
    def _(self,*args,**kwargs):
        self.nr += 1
        self.check_num_requests()
        return f(self,*args,**kwargs)
    
    return _
   
    
class WorkerProcess(Process,Worker):
    '''A :class:`pulsar.Worker` on a subprocess. This worker class
inherit from the :class:`multiprocessProcess` class.'''
    CommandQueue = Queue
    
    def __init__(self, *args, **kwargs):
        Process.__init__(self)
        Worker.__init__(self, *args, **kwargs)
        self.daemon = True
        
    def run(self):
        runworker(self)
    
    @property    
    def get_parent_id(self):
        return os.getppid()
    
    
class WorkerThread(Thread,Worker):
    CommandQueue = ThreadQueue
    #CommandQueue = Queue
    
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        Worker.__init__(self, *args, **kwargs)
        self.daemon = True
        
    def run(self):
        runworker(self)
        
    def terminate(self):
        self.ioloop.stop()
        
    @property
    def pid(self):
        return os.getpid()
        
    @property
    def get_parent_id(self):
        return self.pid
    
