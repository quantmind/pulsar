# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import os
import random
import sys

from multiprocessing.queues import Empty

from pulsar.utils import system
from pulsar import Runner, Actor, is_async


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
              task_queue = None,
              **kwargs):
        self.app = app
        self.cfg = app.cfg
        self.age = age or 0
        self.nr = 0
        self.max_requests = self.cfg.max_requests or sys.maxsize
        self.debug = self.cfg.debug
        self.socket = socket
        self.address = None if not socket else socket.getsockname()
        self.task_queue = task_queue
        super(Worker,self)._init(impl,**kwargs)
    
    def on_exit(self):
        try:
            self.cfg.worker_exit(self)
        except:
            pass
        
    def on_task(self):
        self.app.worker_task(self)
        if self.task_queue:
            try:
                args = self.task_queue.get(timeout = 0.1)
            except Empty:
                return
            self.handle_task(*args)
    
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
        if self.isprocess():
            random.seed()
            if self.cfg:
                system.set_owner_process(self.cfg.uid, self.cfg.gid)
        # Get the Application handler
        self.handler = self.app.handler()
        if self.cfg.post_fork:
            self.cfg.post_fork(self)       
        
    def handle_task(self, fd, request):
        '''Handle request on a channel. This is a high level function
which wraps the low level implementation in :meth:`_handle_task`
and :meth:`_end_task` methods.'''
        self.nr += 1
        self.check_num_requests()
        self.cfg.pre_request(self, request)
        try:
            response, result = self._handle_task(request)
        except Exception as e:
            self.end_task(request, e)
        self.end_task(request, response, result)
    
    def end_task(self, request, response, result = None):
        if not isinstance(response,Exception):
            if is_async(result):
                if result.called:
                    result = result.result
                else:
                    return self.ioloop.add_callback(lambda : self.end_task(request, response, result))
        try:
            self._end_task(response, result)
        finally:
            try:
                self.cfg.post_request(self, request)
            except:
                pass
        
    def _handle_task(self, request):
        ''''''
        pass
    
    def _end_task(self, response, result):
        ''''''
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
    
    @classmethod
    def get_task_queue(cls, monitor):
        return monitor.app.get_task_queue()
    
