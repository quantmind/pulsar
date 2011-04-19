import os
import signal
import sys

import pulsar
from pulsar.utils.py2py3 import range
from pulsar.utils.tools import Pidfile
from pulsar.utils import system
from pulsar.utils.async import MainIOLoop

from .arbiter import Arbiter

__all__ = ['Server']


class Server(Arbiter):
    """The Arbiter is the core element of pulsar.
It maintain pool workers alive. It also manages application reloading
via SIGHUP/USR2 if the platform allows it.
""" 
    def __init__(self, app):
        super(Server,self).__init__(app)
        os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        # get current path, try to use PWD env first
        try:
            a = os.stat(os.environ['PWD'])
            b = os.stat(os.getcwd())
            if a.ino == b.ino and a.dev == b.dev:
                cwd = os.environ['PWD']
            else:
                cwd = os.getcwd()
        except:
            cwd = os.getcwd()
            
        args = sys.argv[:]
        args.insert(0, sys.executable)

        # init start context
        self.START_CTX = {
            "args": args,
            "cwd": cwd,
            0: sys.executable
        }
    
    def get_eventloop(self):
        return MainIOLoop.instance(logger = pulsar.LogSelf(self,self.log))
    
    def setup(self):
        super(Server,self).setup()
        self.log.info("Starting pulsar %s" % pulsar.__version__)
        if self.debug:
            self.log.debug("Current configuration:")
            for config, value in sorted(self.cfg.settings.items()):
                self.log.debug("  %s: %s" % (config, value.value))
        
    def make_pidfile(self):
        if self.cfg.pidfile is not None:
            self.pidfile = Pidfile(self.cfg.pidfile)
            self.pidfile.create(self.pid)
            
    def handle_queued_chld(self, sig, frame):
        "SIGCHLD handling"
        self.reap_workers()
        
    def handle_queued_hup(self):
        """\
        HUP handling.
        - Reload configuration
        - Start the new worker processes with a new configuration
        - Gracefully shutdown the old worker processes
        """
        self.log.info("Hang up: %s" % self)
        self.reload()

    def handle_queued_ttin(self):
        """\
        SIGTTIN handling.
        Increases the number of workers by one.
        """
        self.num_workers += 1
        self.manage_workers()
    
    def handle_queued_ttou(self):
        """\
        SIGTTOU handling.
        Decreases the number of workers by one.
        """
        if self.num_workers <= 1:
            return
        self.num_workers -= 1
        self.manage_workers()

    def handle_queued_usr1(self):
        """\
        SIGUSR1 handling.
        Kill all workers by sending them a SIGUSR1
        """
        self.kill_workers(signal.SIGUSR1)
    
    def handle_queued_usr2(self):
        """\
        SIGUSR2 handling.
        Creates a new master/worker set as a slave of the current
        master without affecting old workers. Use this to do live
        deployment with the ability to backout a change.
        """
        self.reexec()
        
    def handle_queued_winch(self):
        "SIGWINCH handling"
        if os.getppid() == 1 or os.getpgrp() != os.getpid():
            self.log.info("graceful stop of workers")
            self.num_workers = 0
            self.kill_workers(signal.SIGQUIT)
        else:
            self.log.info("SIGWINCH ignored. Not daemonized")

    def reexec(self):
        """\
        Relaunch the master and workers.
        """
        if self.pidfile is not None:
            self.pidfile.rename("%s.oldbin" % self.pidfile.fname)
        
        self.reexec_pid = os.fork()
        if self.reexec_pid != 0:
            self.master_name = "Old Master"
            return
            
        os.environ['PULSAR_FD'] = str(self.socket.fileno())
        os.chdir(self.START_CTX['cwd'])
        self.cfg.pre_exec(self)
        os.execvpe(self.START_CTX[0], self.START_CTX['args'], os.environ)
        
    def reload(self):
        old_address = self.cfg.address

        # reload conf
        self.app.reload()
        self.setup(self.app)

        # do we need to change listener ?
        if old_address != self.cfg.address:
            self.socket.close()
            self.socket = system.create_socket(self.cfg)
            self.log.info("Listening at: %s" % self.socket)    

        # spawn new workers with new app & conf
        for i in range(self.app.cfg.workers):
            self.spawn_worker()
        
        # unlink pidfile
        if self.pidfile is not None:
            self.pidfile.unlink()

        # create new pidfile
        if self.cfg.pidfile is not None:
            self.pidfile = Pidfile(self.cfg.pidfile)
            self.pidfile.create(self.pid)
            
        # set new proc_name
        system._setproctitle("master [%s]" % self.proc_name)
        
        # manage workers
        self.manage_workers()
        