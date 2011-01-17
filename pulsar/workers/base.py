# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.


import logging
import os
import random
import signal
import sys
import tempfile
from multiprocessing import Process
from threading import current_thread, Thread

import pulsar.http as http
from pulsar.workers.workertmp import WorkerTmp


class WorkerMixin(object):
    
    SIGNALS = map(
        lambda x: getattr(signal, "SIG%s" % x),
        "HUP QUIT INT TERM USR1 USR2 WINCH CHLD".split()
    )
    
    PIPE = []
    
    def __init__(self, age = None, socket = None, app = None, timeout = None, cfg = None):
        """\
        This is called pre-fork so it shouldn't do anything to the
        current process. If there's a need to make process wide
        changes you'll want to do that in ``self.init_process()``.
        """
        self.age = age
        self.socket = socket
        self.app = app
        self.timeout = timeout
        self.cfg = cfg
        self.booted = False
        self.http = http.get_library(cfg)

        self.nr = 0
        self.max_requests = getattr(cfg,'max_requests',sys.maxsize)
        self.alive = True
        self.log = logging.getLogger(__name__)
        self.debug = getattr(cfg,'debug',False)
        if self.socket:
            self.address = self.socket.getsockname()
        self.tmp = WorkerTmp(cfg)
    
    @property
    def pid(self):
        return os.getpid()
    
    @property
    def tid(self):
        return current_thread().name
    
    @property
    def parent_pid(self):
        return self._parent_pid
    
    def __str__(self):
        return "<{0} {1} {2}>".format(self.__class__.__name__,self.pid,self.tid)
    
    def notify(self):
        """\
        Your worker subclass must arrange to have this method called
        once every ``self.timeout`` seconds. If you fail in accomplishing
        this task, the master process will murder your workers.
        """
        self.tmp.notify()
        
    def init_process(self):
        """\
        If you override this method in a subclass, the last statement
        in the function should be to call this method with
        super(MyWorkerClass, self).init_process() so that the ``run()``
        loop is initiated.
        """
        utils.set_owner_process(self.cfg.uid, self.cfg.gid)

        # Reseed the random number generator
        random.seed()

        # For waking ourselves up
        self.PIPE = os.pipe()
        map(util.set_non_blocking, self.PIPE)
        map(util.close_on_exec, self.PIPE)
        
        # Prevent fd inherientence
        util.close_on_exec(self.socket)
        util.close_on_exec(self.tmp.fileno())
        self.init_signals()
        
        self.wsgi = self.app.wsgi()
        
        # Enter main run loop
        self.booted = True
        self.run()

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


class WorkerThread(WorkerMixin,Thread):
    '''Base Thread Worker Class'''
    def __init__(self, *args, **kwargs):
        WorkerMixin.__init__(self, *args, **kwargs)
        Thread.__init__(self)
        self._parent_pid = os.getpid()
        
    def run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()
    
    
class WorkerProcess(WorkerMixin,Process):
    '''Base Process Worker Class'''
    
    def __init__(self, *args, **kwargs):
        WorkerMixin.__init__(self, *args, **kwargs)
        Process.__init__(self)

    def run(self):
        """\
        This is the mainloop of a worker process. You should override
        this method in a subclass to provide the intended behaviour
        for your particular evil schemes.
        """
        raise NotImplementedError()