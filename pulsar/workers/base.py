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

from pulsar import getLogger
from pulsar.utils import system
from pulsar.workers.workertmp import WorkerTmp


class RunnerMixin(object):
    
    SIG_QUEUE = None
    '''Signal Queue'''
    
    SIGNALS = map(
                  lambda x: getattr(signal, "SIG%s" % x),
                  system.ALL_SIGNALS.split()
                  )
    
    SIG_NAMES = dict(
                     (getattr(signal, name), name[3:].lower()) for name in dir(signal)
                     if name[:3] == "SIG" and name[3] != "_"
                     )
    
    def check_num_requests(self, nr):
        pass
    
    def init_process(self):
        self.log = getLogger(self.__class__.__name__)
        self.setup_runner()
        self.set_proctitle()
    
    def setup_runner(self):
        pass
    
    def set_proctitle(self):
        '''Set the process title'''
        pass
        

class WorkerMixin(RunnerMixin):
    
    def __init__(self,
                 age = None,
                 socket = None, 
                 app = None,
                 timeout = None,
                 cfg = None,
                 connection = None,
                 SIG_QUEUE = None):
        """\
        This is called pre-fork so it shouldn't do anything to the
        current process. If there's a need to make process wide
        changes you'll want to do that in ``self.init_process()``.
        """
        self.age = age
        self.nr = 0
        self.alive = True
        self.max_requests = getattr(cfg,'max_requests',sys.maxsize)
        self.debug = getattr(cfg,'debug',False)
        self.SIG_QUEUE = SIG_QUEUE
        self.timeout = timeout
        self.cfg = cfg
        self.connection = connection
        self.set_listener(socket, app)
        
    def set_listner(self, socket, app):
        self.socket = socket
        self.app = app
    
    @property
    def tid(self):
        '''Thread Name'''
        return current_thread().name
    
    @property
    def parent_pid(self):
        return self._parent_pid
    
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
            self.log.info("Booting worker with pid: %s" % self.pid)
            self.cfg.post_fork(self)
        except SystemExit:
            raise
        except:
            self.log.exception("Exception in worker process:")
        finally:
            self.log.info("Worker exiting {0}".format(self))
            try:
                self.cfg.worker_exit(self)
            except:
                pass

    def init_process(self):
        system.set_owner_process(self.cfg.uid, self.cfg.gid)

        # Reseed the random number generator
        random.seed()

        # Prevent fd inherientence
        #util.close_on_exec(self.socket)
        #util.close_on_exec(self.tmp.fileno())
        self.init_signals()
        
        self.wsgi = self.app.wsgi()
        
        # Enter main run loop
        self._run()

    def init_signals(self):
        map(lambda s: signal.signal(s, signal.SIG_DFL), self.SIGNALS)
        signal.signal(signal.SIGQUIT, self.handle_quit)
        signal.signal(signal.SIGTERM, self.handle_exit)
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGWINCH, self.handle_winch)
            
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


class WorkerThread(WorkerMixin,Thread):
    '''Base Thread Worker Class'''
    def __init__(self, *args, **kwargs):
        WorkerMixin.__init__(self, *args, **kwargs)
        Thread.__init__(self)
        self._parent_pid = os.getpid()
        self.daemon = True
        
    def _run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()
    
    @property
    def pid(self):
        return os.getpid()
    
    @classmethod
    def pipe(cls):
        return Pipe()
    
    def terminate(self):
        self.join()
    
    
class WorkerProcess(WorkerMixin,Process):
    '''Base Process Worker Class'''
    
    def __init__(self, *args, **kwargs):
        WorkerMixin.__init__(self, *args, **kwargs)
        Process.__init__(self)
        self.daemon = True
        
    def _run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()
    
    @classmethod
    def pipe(cls):
        return Pipe()
    
    def set_proctitle(self):
        system.set_proctitle("worker [{0}]".format(self.cfg.proc_name))
        
    def get_parent_id(self):
        return os.getppid()
    
