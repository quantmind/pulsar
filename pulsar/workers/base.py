# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import os
import random
import sys

from multiprocessing.queues import Empty

from pulsar.utils import system
from pulsar import Actor, is_async


__all__ = ['Worker']

    

class Worker(Actor):
    """\
Base class for actors implementing applications.
    
.. attribute:: wid

    The worker unique id. If the Worker has not started it is ``None``.

"""        
    def _init(self,
              impl,
              app = None,
              **kwargs):
        self.app = app
        self.cfg = app.cfg
        self.max_requests = self.cfg.max_requests or sys.maxsize
        self.debug = self.cfg.debug
        self.app_handler = app.handler()
        super(Worker,self)._init(impl,**kwargs)
         
    def on_exit(self):
        self.app.on_exit(self)
        try:
            self.cfg.worker_exit(self)
        except:
            pass
        
    def on_task(self):
        self.app.worker_task(self)
    
    #def __str__(self):
    #    return "<{0} {1}>".format(self.__class__.__name__,self.wid)
        
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
        if self.cfg.post_fork:
            self.cfg.post_fork(self)       
        
    def handle_task(self, fd, request):
        '''Handle request on a channel. This is a high level function
which wraps the low level implementation in :meth:`_handle_task`
and :meth:`_end_task` methods.'''
        self.nr += 1
        self.check_num_requests()
        try:
            self.cfg.pre_request(self, request)
        except Exception:
            pass
        try:
            response, result = self.app.handle_event_task(self, request)
        except Exception as e:
            response = e
            result = None
        self.end_task(request, response, result)
    
    def end_task(self, request, response, result = None):
        if not isinstance(response,Exception):
            if is_async(result):
                if result.called:
                    result = result.result
                else:
                    return self.ioloop.add_callback(lambda : self.end_task(request, response, result))
        try:
            self.app.end_event_task(self, response, result)
        except Exception as e:
            self.log.critical('Handled exception : {0}'.format(e),exc_info = sys.exc_info())
        finally:
            try:
                self.cfg.post_request(self, request)
            except:
                pass
    
    def configure_logging(self, **kwargs):
        #switch off configure logging. Done by self.app
        pass
    
    @classmethod
    def get_task_queue(cls, monitor):
        return monitor.app.get_task_queue()
    
