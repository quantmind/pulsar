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
from pulsar.utils.eventloop import IOLoop
from pulsar.utils import system
from pulsar.http import get_httplib

from .base import ArbiterBase, ThreadQueue


__all__ = ['Arbiter']



class Arbiter(ArbiterBase):
    """The Arbiter is the core element of pulsar.
It maintain workers processes alive by launching or killing
them as needed. It also manages application reloading
via SIGHUP/USR2 if the platform allows it.
"""
    WORKER_BOOT_ERROR = 3
    SIG_TIMEOUT = 0.5
    JOIN_TIMEOUT = 0.5
    START_CTX = {}
    LISTENER = None
    WORKERS = {}
    
    def __init__(self, app):
        self.pid = None
        os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        self.app = app
        self.cfg = app.cfg
        self.pidfile = None
        self.worker_age = 0
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
        self.num_workers = self.cfg.workers
        self.debug = self.cfg.debug
        self.timeout = self.cfg.timeout
        
        if self.debug:
            self.log.debug("Current configuration:")
            for config, value in sorted(self.cfg.settings.items()):
                self.log.debug("  %s: %s" % (config, value.value))
        
        if self.cfg.preload_app:
            if not self.cfg.debug:
                self.app.wsgi()
            else:
                self.log.warning("debug mode: app isn't preloaded.")
    
    def __repr__(self):
        return self.__class__.__name__
    
    def __str__(self):
        return self.__repr__()

    def _run(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        ioloop = self.get_ioloop()
        ioloop.add_loop_task(self.arbiter)
        if ioloop._stopped:
            ioloop._stopped = False
            return False
        assert not ioloop._running, 'cannot start arbiter twice'
        assert not self.pid, 'cannot start arbiter twice'
        self.pid = os.getpid()
        
        # Create the listener
        if not self.LISTENER:
            self.LISTENER = system.create_socket(self)
        
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
            self.stop(False)
            if self.pidfile is not None:
                self.pidfile.unlink()
            sys.exit(-1)

    def arbiter(self):
        self.log.debug('Manage workers')
        self.reap_workers()
        sig = self.getsig()
        if sig is None:
            self.murder_workers()
            self.manage_workers()
        elif sig not in system.SIG_NAMES:
            self.log.info("Ignoring unknown signal: %s" % sig)
        else:        
            signame = system.SIG_NAMES.get(sig)
            handler = getattr(self, "handle_%s" % signame.lower(), None)
            if not handler:
                self.log.error("Unhandled signal: %s" % signame)
            else:
                self.log.info("Handling signal: %s" % signame)
                handler()
                
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
            self.log.debug('waiting {0} seconds for signals'.format(self.SIG_TIMEOUT))
            return self.SIG_QUEUE.get(timeout = self.SIG_TIMEOUT)
        except Empty:
            return
        except IOError:
            return
        
    def stop(self, graceful=True):
        """\
        Stop workers
        
        :attr graceful: boolean, If True (the default) workers will be
        killed gracefully  (ie. trying to wait for the current connection)
        """
        sig = signal.SIGQUIT
        if not graceful:
            self.log.info("Force Stopping.")
            sig = signal.SIGTERM
        else:
            self.log.info("Stopping Gracefully.")
        self.stopping = True            
        self.LISTENER = None
        limit = time.time() + self.timeout
        while self.WORKERS and time.time() < limit:
            self.kill_workers(sig)
            time.sleep(0.2)
            self.reap_workers()
        self.kill_workers(signal.SIGKILL)

    def signal(self, sig, frame):
        signame = system.SIG_NAMES.get(sig,None)
        if signame:
            self.log.info('Received signal {0}. Putting it into the queue'.format(signame))
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
        
    def murder_workers(self):
        """Murder workers to avoid zombie processes
        """
        if self.timeout:
            for wid, proc in list(iteritems(self.WORKERS)):
                worker = proc['worker']
                connection = proc['reader']
                notified = proc['notified']
                while connection.poll():
                    notified = connection.recv()
                proc['notified'] = notified
                gap = time.time() - notified
                if gap > self.timeout:
                    if worker.is_alive():
                        self.log.info('Terminating worker. Reason timeout surpassed.')
                        self.terminate_worker(wid,system.SIGQUIT)
    
    def reap_workers(self):
        '''Remove not alive workers from the dictionary'''
        for wid, proc in list(iteritems(self.WORKERS)):
            if not proc['worker'].is_alive():
                self.WORKERS.pop(wid)
        
    def manage_workers(self):
        """Maintain the number of workers by spawning or killing
as required."""
        if len(self.WORKERS) < self.num_workers:
            self.spawn_workers()

        num_to_kill = len(self.WORKERS) - self.num_workers
        for i in range(num_to_kill, 0, -1):
            pid, age = 0, sys.maxsize
            for (wpid, worker) in iteritems(self.WORKERS):
                if worker.age < age:
                    pid, age = wpid, worker.age
            self.join_worker(pid)
            
    def spawn_workers(self):
        """\
        Spawn new workers as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        
        for i in range(self.num_workers - len(self.WORKERS)):
            self.spawn_worker()
            
    def spawn_worker(self):
        '''Spawn a new worker'''
        self.worker_age += 1
        worker_class = self.cfg.worker_class
        arbiter_reader,worker_writer = Pipe(duplex = False)
        worker = worker_class(self.worker_age,
                              self.pid,
                              self.LISTENER,
                              self.app,
                              self.timeout/2.0,
                              self.cfg,
                              worker_writer,
                              ioloop = self.ioloop)
        
        self.cfg.pre_fork(worker)
        worker.start()
        wid = worker.wid
        if wid != 0:
            self.WORKERS[wid] = {'worker':worker,
                                 'reader':arbiter_reader,
                                 'notified':time.time()}

    def kill_workers(self, sig):
        """\
Kill all workers with the signal ``sig``

:parameter sig: `signal.SIG*` value
"""
        for wid in list(self.WORKERS.keys()):
            if sig == signal.SIGQUIT:
                self.join_worker(wid)
            elif sig == signal.SIGTERM:
                self.terminate_worker(wid)
            else:
                self.terminate_worker(wid)
            
    def join_worker(self, wid):
        w = self.WORKERS[wid]['worker']
        w.stop()
        self.log.info("Joining {0}".format(w))
        w.join(self.JOIN_TIMEOUT)
                    
    def terminate_worker(self, wid):
        w = self.WORKERS.pop(wid)
        self.log.info("Terminating {0}".format(w))
        w['worker'].terminate()
                   


        
        