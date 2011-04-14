# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import errno
import os
import signal
import sys
import time
import traceback
from multiprocessing import Pipe
from multiprocessing.queues import Empty
from select import error as selecterror

import pulsar
from pulsar.utils.py2py3 import iteritems, map, range
from pulsar.utils.pidfile import Pidfile
from pulsar.utils import system
from pulsar.http import get_httplib

from .workerpool import WorkerPool
from .base import ArbiterBase, ThreadQueue


__all__ = ['Arbiter']



class Arbiter(ArbiterBase):
    """The Arbiter is the core element of pulsar.
It maintain pool workers alive. It also manages application reloading
via SIGHUP/USR2 if the platform allows it.
"""
    WORKER_BOOT_ERROR = 3
    SIG_TIMEOUT = 0.001
    START_CTX = {}
    LISTENER = None
    
    def __init__(self, app):
        self.pid = None
        os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        self.app = app
        self.cfg = app.cfg
        self.pidfile = None
        self.reexec_pid = 0
        self.SIG_QUEUE = ThreadQueue()
        self._pools = []
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
    
    def setup(self):
        self.log.info("Starting pulsar %s" % pulsar.__version__)
        self.address = self.cfg.address
        self.debug = self.cfg.debug
        self.ioloop.add_loop_task(self.arbiter)
        
        # Create the listener if not available
        if not self.LISTENER:
            self.LISTENER = system.create_socket(self)
            
        worker_class = self.cfg.worker_class        
        # Setup the server pool of workers
        cfg = self.cfg
        pool = WorkerPool(worker_class,
                          cfg.workers,
                          app = self.app,
                          timeout = cfg.timeout,
                          socket = self.LISTENER)
        self._pools.append(pool)
        
        worker_class.modify_arbiter_loop(pool,self.ioloop)
        
        if self.debug:
            self.log.debug("Current configuration:")
            for config, value in sorted(self.cfg.settings.items()):
                self.log.debug("  %s: %s" % (config, value.value))
    
    def __repr__(self):
        return self.__class__.__name__
    
    def __str__(self):
        return self.__repr__()

    def arbiter(self):
        while True:
            try:
                sig = self.SIG_QUEUE.get(timeout = self.SIG_TIMEOUT)
            except Empty:
                sig = None
                break
            if sig not in system.SIG_NAMES:
                sig = None
                self.log.info("Ignoring unknown signal: %s" % sig)
            else:        
                signame = system.SIG_NAMES.get(sig)
                handler = getattr(self, "handle_%s" % signame.lower(), None)
                if not handler:
                    self.log.error("Unhandled signal: %s" % signame)
                else:
                    self.log.info("Handling signal: %s" % signame)
                    handler()
                    
        if sig is None:
            for pool in self._pools:
                pool.arbiter()
                
    def _run(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        ioloop = self.ioloop
        if ioloop._stopped:
            ioloop._stopped = False
            return False
        assert not ioloop.running(), 'cannot start arbiter twice'
        self.pid = os.getpid()
        
        if self.cfg.pidfile is not None:
            self.pidfile = Pidfile(self.cfg.pidfile)
            self.pidfile.create(self.pid)
        self.log.debug("{0} booted".format(self))
        self.log.info("Listening at: %s (%s)" % (self.LISTENER, self.pid))
        self.cfg.when_ready(self)
        try:
            ioloop.start()
        except StopIteration:
            self.halt('Stop Iteration')
        except KeyboardInterrupt:
            self.halt('Keyboard Interrupt')
        except pulsar.HaltServer as inst:
            self.halt(reason=inst.reason, exit_status=inst.exit_status)
        except SystemExit:
            raise
        except Exception:
            self.log.info("Unhandled exception in main loop:\n%s" %  
                        traceback.format_exc())
            self.terminate()
            if self.pidfile is not None:
                self.pidfile.unlink()
                
    def halt(self, reason=None, exit_status=0):
        """ halt arbiter """
        self.stop()
        if reason is not None:
            self.log.info("Shutting down: %s" % reason)
        else:
            self.log.info("Shutting down")
        if self.pidfile is not None:
            self.pidfile.unlink()
        sys.exit(exit_status)
        
    def getsig(self):
        """Get signals from the signal queue.
        """
        try:
            #self.log.debug('waiting {0} seconds for signals'.format(self.SIG_TIMEOUT))
            return self.SIG_QUEUE.get(timeout = self.SIG_TIMEOUT)
        except Empty:
            return
        except IOError:
            return
        
    def stop(self):
        self.close()
        
    def close(self):
        for pool in self._pool:
            pool.close()
    
    def terminate(self):
        for pool in self._pool:
            pool.terminate()
        

    def signal(self, sig, frame):
        signame = system.SIG_NAMES.get(sig,None)
        if signame:
            self.log.warn('Received and queueing signal {0}.'.format(signame))
            self.SIG_QUEUE.put(sig)
        else:
            self.log.info('Received unknown signal "{0}". Skipping.'.format(sig))
            
    def handle_chld(self, sig, frame):
        "SIGCHLD handling"
        self.reap_workers()
        
    def handle_hup(self):
        """\
        HUP handling.
        - Reload configuration
        - Start the new worker processes with a new configuration
        - Gracefully shutdown the old worker processes
        """
        self.log.info("Hang up: %s" % self)
        self.reload()
        
    def handle_quit(self):
        "SIGQUIT handling"
        raise StopIteration
    
    def handle_int(self):
        "SIGINT handling"
        raise StopIteration
    
    def handle_term(self):
        "SIGTERM handling"
        raise StopIteration

    def handle_ttin(self):
        """\
        SIGTTIN handling.
        Increases the number of workers by one.
        """
        self.num_workers += 1
        self.manage_workers()
    
    def handle_ttou(self):
        """\
        SIGTTOU handling.
        Decreases the number of workers by one.
        """
        if self.num_workers <= 1:
            return
        self.num_workers -= 1
        self.manage_workers()

    def handle_usr1(self):
        """\
        SIGUSR1 handling.
        Kill all workers by sending them a SIGUSR1
        """
        self.kill_workers(signal.SIGUSR1)
    
    def handle_usr2(self):
        """\
        SIGUSR2 handling.
        Creates a new master/worker set as a slave of the current
        master without affecting old workers. Use this to do live
        deployment with the ability to backout a change.
        """
        self.reexec()
        
    def handle_winch(self):
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
            
        os.environ['PULSAR_FD'] = str(self.LISTENER.fileno())
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
            self.LISTENER.close()
            self.LISTENER = system.create_socket(self.cfg)
            self.log.info("Listening at: %s" % self.LISTENER)    

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
        
    