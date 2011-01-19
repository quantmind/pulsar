# -*- coding: utf-8 -
#
# This file is part of gunicorn released under the MIT license. 
# See the NOTICE for more information.
import errno
import logging
import os
import signal
import sys
import time
import traceback
from multiprocessing import Queue, Pipe
from multiprocessing.queues import Empty
from select import error as selecterror

try:
    range = xrange
    map = imap
except NameError:
    pass

from pulsar.utils.pidfile import Pidfile
from pulsar.utils.py2py3 import iteritems
from pulsar.utils import system
from pulsar import __version__, SERVER_SOFTWARE

from .errors import HaltServer

select = system.select


__all__ = ['Arbiter']


class Arbiter(object):
    """
    Arbiter maintain the workers processes alive. It launches or
    kills them if needed. It also manages application reloading
    via SIGHUP/USR2.
    """

    # A flag indicating if a worker failed to
    # to boot. If a worker process exist with
    # this error code, the arbiter will terminate.
    WORKER_BOOT_ERROR = 3

    START_CTX = {}
    
    LISTENER = None
    WORKERS = {}    
    PIPE = []

    SIGNALS = map(
                  lambda x: getattr(signal, "SIG%s" % x),
                  system.ALL_SIGNALS.split()
                  )
    SIG_NAMES = dict(
                     (getattr(signal, name), name[3:].lower()) for name in dir(signal)
                     if name[:3] == "SIG" and name[3] != "_"
                     )
    
    def __init__(self, app):
        self.log = logging.getLogger(__name__)
        self.log.info("Starting pulsar %s" % __version__)
       
        os.environ["SERVER_SOFTWARE"] = SERVER_SOFTWARE

        self.setup(app)
        
        self.pidfile = None
        self.worker_age = 0
        self.reexec_pid = 0
        self.master_name = "Master"
        self.SIG_QUEUE = Queue()
        
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
        
    def setup(self, app):
        self.app = app
        self.cfg = app.cfg
        self.address = self.cfg.address
        self.num_workers = self.cfg.workers
        self.debug = self.cfg.debug
        self.timeout = self.cfg.timeout
        self.proc_name = self.cfg.proc_name
        self.worker_class = self.cfg.worker_class
        
        if self.cfg.debug:
            self.log.debug("Current configuration:")
            for config, value in sorted(self.cfg.settings.items()):
                self.log.debug("  %s: %s" % (config, value.value))
        
        if self.cfg.preload_app:
            if not self.cfg.debug:
                self.app.wsgi()
            else:
                self.log.warning("debug mode: app isn't preloaded.")

    def start(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        self.pid = os.getpid()
        self.init_signals()
        if not self.LISTENER:
            self.LISTENER = system.create_socket(self)
        
        if self.cfg.pidfile is not None:
            self.pidfile = Pidfile(self.cfg.pidfile)
            self.pidfile.create(self.pid)
        self.log.debug("Arbiter booted")
        self.log.info("Listening at: %s (%s)" % (self.LISTENER,
            self.pid))
        self.cfg.when_ready(self)
    
    def init_signals(self):
        """\
        Initialize master signal handling. Most of the signals
        are queued. Child signals only wake up the master.
        """
        if self.PIPE:
            map(lambda p: os.close(p), self.PIPE)
        server_p, worker_p = self.worker_class.pipe()
        worker_p.close()
        #map(system.set_non_blocking, pair)
        #map(system.close_on_exec, pair)
        map(lambda s: signal.signal(s, self.signal), self.SIGNALS)
        #signal.signal(signal.SIGCHLD, self.handle_chld)

    def signal(self, sig, frame):
        signame = self.SIG_NAMES.get(sig,None)
        if signame:
            self.log.info('Received signal {0}. Putting it into the queue'.format(signame))
            self.SIG_QUEUE.put(sig)
        else:
            self.log.info('Received unknown signal. Skipping')

    def run(self):
        "Main master loop."
        self.start()
        system.set_proctitle("master [%s]" % self.proc_name)
        self.manage_workers()
        while True:
            try:
                self.reap_workers()
                sig = self.get()
                if sig is None:
                    self.murder_workers()
                    self.manage_workers()
                    continue
                
                if sig not in self.SIG_NAMES:
                    self.log.info("Ignoring unknown signal: %s" % sig)
                    continue
                
                signame = self.SIG_NAMES.get(sig)
                handler = getattr(self, "handle_%s" % signame, None)
                if not handler:
                    self.log.error("Unhandled signal: %s" % signame)
                    continue
                self.log.info("Handling signal: %s" % signame)
                handler()
            except StopIteration:
                self.halt()
            except KeyboardInterrupt:
                self.halt()
            except HaltServer as inst:
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
                
    def halt(self, reason=None, exit_status=0):
        """ halt arbiter """
        self.stop()
        self.log.info("Shutting down: %s" % self.master_name)
        if reason is not None:
            self.log.info("Reason: %s" % reason)
        if self.pidfile is not None:
            self.pidfile.unlink()
        sys.exit(exit_status)
        
    def get(self):
        """Get signals from the signal queue.
        """
        try:
            return self.SIG_QUEUE.get(timeout = 0.5)
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
        self.stopping = True
        self.LISTENER = None
        if self.SIG_QUEUE is not None:
            self.SIG_QUEUE.close()
            self.SIG_QUEUE.join_thread()
            self.SIG_QUEUE = None
        limit = time.time() + self.timeout
        while self.WORKERS and time.time() < limit:
            self.join_workers()
            time.sleep(0.1)
            self.murder_workers()
        self.join_workers()

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
        self.log.info("Hang up: %s" % self.master_name)
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
                connection = proc['connection']
                notified = proc['notified']
                while connection.poll():
                    notified = connection.recv()
                proc['notified'] = notified
                if time.time() - notified > self.timeout:
                    if worker.is_alive():
                        self.terminate_worker(wid)
    
    def reap_workers(self):
        for wid, proc in list(iteritems(self.WORKERS)):
            if not proc['worker'].is_alive():
                self.WORKERS.pop(wid)
        
    def manage_workers(self):
        """\
        Maintain the number of workers by spawning or killing
        as required.
        """
        if len(self.WORKERS.keys()) < self.num_workers:
            self.spawn_workers()

        num_to_kill = len(self.WORKERS) - self.num_workers
        for i in range(num_to_kill, 0, -1):
            pid, age = 0, sys.maxsize
            for (wpid, worker) in iteritems(self.WORKERS):
                if worker.age < age:
                    pid, age = wpid, worker.age
            self.join_worker(pid)
            
    def spawn_worker(self):
        self.worker_age += 1
        connection,worker_conn = self.worker_class.pipe()
        worker = self.worker_class(self.worker_age,
                                   self.LISTENER,
                                   self.app,
                                   self.timeout/2.0,
                                   self.cfg,
                                   worker_conn,
                                   self.SIG_QUEUE)
        
        self.cfg.pre_fork(worker)

        worker.start()
        pid = worker.pid
        if pid != 0:
            self.WORKERS[pid] = {'worker':worker,
                                 'connection':connection,
                                 'notified':time.time()}        
        
    def spawn_workers(self):
        """\
        Spawn new workers as needed.
        
        This is where a worker process leaves the main loop
        of the master process.
        """
        
        for i in range(self.num_workers - len(self.WORKERS)):
            self.spawn_worker()

    def join_workers(self):
        """\
        Kill all workers with the signal `sig`
        :attr sig: `signal.SIG*` value
        """
        for pid in self.WORKERS.keys():
            self.join_worker(pid)
            
    def join_worker(self, wid):
        w = self.WORKERS.pop(wid)
        w['worker'].join()
        
    def terminate_workers(self, sig):
        """\
        Kill all workers with the signal `sig`
        :attr sig: `signal.SIG*` value
        """
        for pid in self.WORKERS.keys():
            self.terminate_worker(pid, sig)
                    
    def terminate_worker(self, wid, sig):
        """\
        Terminate a worker
        
        :attr wid: int, worker process id
         """
        try:
            proc = self.WORKERS[wid]
            proc['worker'].terminate()
        except OSError as e:
            if e.errno == errno.ESRCH:
                try:
                    worker.tmp.close()
                    self.cfg.worker_exit(self, worker)
                    return
                except (KeyError, OSError):
                    return
            raise            
