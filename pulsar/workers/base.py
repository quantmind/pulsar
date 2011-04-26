# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import os
import random
import signal
import sys

from multiprocessing import Process, current_process
from multiprocessing.queues import Queue
from threading import current_thread, Thread

import pulsar
from pulsar.utils.async import ProcessWithRemote, IOLoop
from pulsar.utils import system
from pulsar.async import Runner, Actor


__all__ = ['Worker']

    

class Worker(Actor, Runner):
    """\
Base class for all workers. The constructor is called
called pre-fork so it shouldn't do anything to the current process.
If there's a need to make process wide changes you'll want to do that
in :meth:`setup` method.

A worker is manages its own event loop and can leve on a thread or on a Process.

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
    def on_start(self):
        self.init_runner()
        
    def _init(self,
              impl,
              app = None,
              age = None,
              socket = None,
              timeout = None,
              **kwargs):
        self.app = app
        self.cfg = app.cfg
        self.age = age or 0
        self.nr = 0
        self.max_requests = self.cfg.max_requests or sys.maxsize
        self.debug = self.cfg.debug
        self.socket = socket
        self.address = None if not socket else socket.getsockname()
        super(Worker,self)._init(impl,**kwargs)
    
    def on_exit(self):
        try:
            self.cfg.worker_exit(self)
        except:
            pass
        
    def on_task(self):
        self.app.worker_task(self)   
    
    def _shut_down(self):
        '''Shut down the application. Hard core function to use with care.'''
        if self.ioloop.running() and self.arbiter_proxy:
            self.arbiter_proxy.shut_down()
    
    def __str__(self):
        return "<{0} {1}>".format(self.__class__.__name__,self.wid)
        
    def check_num_requests(self):
        '''Check the number of requests. If they exceed the maximum number
stop the event loop and exit.'''
        max_requests = self.max_requests
        if max_requests and self.nr >= self.max_requests:
            self.log.info("Auto-restarting worker after current request.")
            self._stop()
    
    def _setup(self):
        '''Called after fork, it set ups the application handler
and perform several post fork processing before starting the event loop.'''
        if self.is_process():
            random.seed()
            if self.cfg:
                system.set_owner_process(self.cfg.uid, self.cfg.gid)
        # Get the Application handler
        self.handler = self.app.handler()
        if self.cfg.post_fork:
            self.cfg.post_fork(self)       
        
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
    
    def configure_logging(self, **kwargs):
        pass
    
    def get_parent_id(self):
        return os.getpid()
    
    @property
    def wid(self):
        return '{0}-{1}'.format(self.pid,self.tid)
        
        
def updaterequests(f):
    
    def _(self,*args,**kwargs):
        self.nr += 1
        self.check_num_requests()
        return f(self,*args,**kwargs)
    
    return _
   
