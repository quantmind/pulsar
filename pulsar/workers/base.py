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
from multiprocessing import Process, Pipe
from threading import current_thread, Thread

import pulsar
from pulsar import getLogger
from pulsar.utils import system
from pulsar.workers.workertmp import WorkerTmp


__all__ = ['RunnerMixin',
           'Worker',
           'WorkerProcess',
           'WorkerThread',
           'SIGNALS',
           'SIG_NAMES']

SIGNALS = map(
              lambda x: getattr(signal, "SIG%s" % x),
              system.ALL_SIGNALS.split()
              )

SIG_NAMES = dict(
                 (getattr(signal, name), name[3:].lower()) for name in dir(signal)
                 if name[:3] == "SIG" and name[3] != "_"
                 )


class RunnerMixin(object):
    '''Mixin for classes exposing process-type functionalities.'''
    
    SIG_QUEUE = None
    '''Signal Queue'''
    
    def check_num_requests(self, nr):
        pass
    
    def init_process(self):
        self.log = getLogger(self.__class__.__name__)
        self.set_proctitle()
        self.setup_runner()
        
    def init_signals(self):
        '''Initialise signals for correct signal handling.'''
        pass
    
    def setup_runner(self):
        pass
    
    def set_proctitle(self):
        '''Set the process title'''
        pass
    
    @property
    def tid(self):
        '''Thread Name'''
        return current_thread().name
    
    @classmethod
    def pipe(cls):
        return Pipe()
        

class Worker(RunnerMixin):
    """\
Base class for all workers. The constructor is called
called pre-fork so it shouldn't do anything to the current process.
If there's a need to make process wide changes you'll want to do that
in ``self.init_process()``.

.. attribute:: pid

    The worker process id.
    
.. attribute:: tid

    The worker thread id.
    
.. attribute:: wid

    The worker unique id. If the Worker has not started it is ``None``.
"""
    def __init__(self, age, ppid,
                 socket = None, 
                 app = None,
                 timeout = None,
                 cfg = None,
                 connection = None,
                 SIG_QUEUE = None):
        self.age = age
        self.ppid = ppid
        self.nr = 0
        self.alive = True
        self.max_requests = cfg.max_requests or sys.maxsize
        self.debug = getattr(cfg,'debug',False)
        self.SIG_QUEUE = SIG_QUEUE
        self.timeout = timeout
        self.cfg = cfg
        self.connection = connection
        self.set_listener(socket, app)
        
    def set_listener(self, socket, app):
        self.socket = socket
        self.address = socket.getsockname()
        self.app = app
    
    def __str__(self):
        return "<{0} {1} {2}>".format(self.__class__.__name__,self.pid,self.tid)
    
    def check_num_requests(self, nr):
        max_requests = getattr(self,'max_requests',None)
        if max_requests and nr >= self.max_requests:
            self.log.info("Autorestarting worker after current request.")
            self.alive = False
    
    def notify(self):
        """\
        Your worker subclass must arrange to have this method called
        once every ``self.timeout`` seconds. If you fail in accomplishing
        this task, the master process will murder your workers.
        """
        self.connection.send(time.time())
        
    def run(self):
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

    def reseed(self):
        pass
    
    def setup_runner(self):
        system.set_owner_process(self.cfg.uid, self.cfg.gid)
        # Reseed the random number generator
        self.reseed()
        # Prevent fd inherientence
        #util.close_on_exec(self.socket)
        #util.close_on_exec(self.tmp.fileno())
        self.init_signals()
        self.handler = self.app.handler()
        self.log.info('Booting worker "{0}"'.format(self.wid))
        self.cfg.post_fork(self)
        # Enter main run loop
        self._run()
            
    def handle_quit(self, sig, frame):
        self.alive = False

    def handle_exit(self, sig, frame):
        self.alive = False
        sys.exit(0)

    def handle_winch(self, sig, fname):
        # Ignore SIGWINCH in worker. Fixes a crash on OpenBSD.
        return
    
    def get_parent_id(self):
        return os.getpid()


class WorkerThread(Worker,Thread):
    '''A :class:`pulsar.Worker` on a thread. This worker class
inherit from the :class:`threading.Thread` class.'''
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)
        Thread.__init__(self)
        self.daemon = True
        
    def _run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()
    
    def terminate(self):
        self.join()
    
    @property
    def tid(self):
        '''Thread Name'''
        return current_thread().name
        
    @property
    def pid(self):
        return os.getpid()
        
    @property
    def get_parent_id(self):
        return self.pid
    
    @property
    def wid(self):
        return '{0}-{1}'.format(self.pid,self.tid)

    
    
class WorkerProcess(Worker,Process):
    '''A :class:`pulsar.Worker` on a subprocess. This worker class
inherit from the :class:`multiprocessProcess` class.'''
    
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)
        Process.__init__(self)
        self.daemon = True
        
    def reseed(self):
        random.seed()
        
    def init_signals(self):
        map(lambda s: signal.signal(s, signal.SIG_DFL), pulsar.SIGNALS)
        signal.signal(signal.SIGQUIT, self.handle_quit)
        signal.signal(signal.SIGTERM, self.handle_exit)
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGWINCH, self.handle_winch)
        
    def _run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()
    
    def set_proctitle(self):
        system.set_proctitle("worker [{0}]".format(self.cfg.proc_name))
    
    @property    
    def get_parent_id(self):
        return os.getppid()
    
    @property
    def wid(self):
        return self.pid
 