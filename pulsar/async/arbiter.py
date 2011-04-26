from time import time
import os
import sys
import traceback
import signal
from multiprocessing.queues import Queue, Empty
from threading import current_thread

try:
    import queue
except ImportError:
    import Queue as queue
ThreadQueue = queue.Queue

import pulsar
from pulsar.utils import system
from pulsar.utils.py2py3 import iteritems
from pulsar.utils.tools import gen_unique_id

from .actor import Actor
from .monitor import ActorPool


__all__ = ['arbiter','spawn','ThreadQueue','Runner']

_main_thread = current_thread()


def arbiter():
    return Arbiter.instance()
    
    
def spawn(actor_class, *args, **kwargs):
    return arbiter().spawn(actor_class, *args, **kwargs)


class Runner(object):
    '''Base class for classes with an event loop.
    '''
    DEF_PROC_NAME = 'pulsar'
    SIG_QUEUE = None
    
    def init_runner(self):
        '''Initialise the runner. This function
will block the current thread since it enters the event loop.
If the runner is a instance of a subprocess, this function
is called after fork by the run method.'''
        self._set_proctitle()
        self._setup()
        self._install_signals()
        
    def _set_proctitle(self):
        '''Set the process title'''
        if self.isprocess() and hasattr(self,'cfg'):
            proc_name = self.cfg.proc_name or self.cfg.default_proc_name
            if proc_name:
                system.set_proctitle("{0} - {1}".format(proc_name,self))
    
    def _install_signals(self):
        '''Initialise signals for correct signal handling.'''
        current = self.current_thread()
        if current == _main_thread and self.isprocess():
            self.log.info('Installing signals')
            sfun = getattr(self,'signal',None)
            for name in system.ALL_SIGNALS:
                func = getattr(self,'handle_{0}'.format(name.lower()),sfun)
                if func:
                    sig = getattr(signal,'SIG{0}'.format(name))
                    signal.signal(sig, func)
    
    def _setup(self):
        pass
        

class Arbiter(ActorPool,Runner):
    '''An Arbiter is a special actor. It manages them all.'''
    CLOSE_TIMEOUT = 3
    WORKER_BOOT_ERROR = 3
    STOPPING_LOOPS = 2
    SIG_TIMEOUT = 0.001
    MINIMUM_ACTOR_TIMEOUT = 0.1
    DEFAULT_ACTOR_TIMEOUT = 10
    EXIT_SIGNALS = (signal.SIGINT,signal.SIGTERM,signal.SIGABRT,system.SIGQUIT)
    HaltServer = pulsar.HaltServer
    
    @classmethod
    def instance(cls):
        if not hasattr(cls,'_instance'):
            cls._instance = cls.spawn(cls,impl='monitor')
        return cls._instance
    
    @classmethod
    def spawn(cls, actor_class, *args, **kwargs):
        '''Create a new concurrent Actor'''
        actor = actor_class()
        actor._aid = gen_unique_id()
        actor._inbox = Queue()
        actor._ppid = os.getpid()
        actor._timeout = max(kwargs.pop('timeout',cls.DEFAULT_ACTOR_TIMEOUT),cls.MINIMUM_ACTOR_TIMEOUT)
        impl = kwargs.pop('impl',actor_class.DEFAULT_IMPLEMENTATION)
        actor._impl = impl
        impl_cls = actor._runner_impl[impl]
        arbiter = getattr(cls,'_instance',None)
        kwargs['arbiter'] = arbiter
        impl = impl_cls(actor,args,kwargs)
        impl.start()
        monitor = impl.proxy_monitor(actor)
        if monitor:
            arbiter.LIVE_ACTORS[actor.aid] = monitor
            return monitor
        else:
            return actor

    
    def _init(self, **kwargs):
        os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        self.cfg = None
        self.pidfile = None
        self.reexec_pid = 0
        self._monitors = []
        self.SIG_QUEUE = ThreadQueue()
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
            
        sargs = sys.argv[:]
        sargs.insert(0, sys.executable)

        # init start context
        self.START_CTX = {
            "args": sargs,
            "cwd": cwd,
            0: sys.executable
        }
        super(Arbiter,self)._init(**kwargs)
        
    def isprocess(self):
        return True
 
    # ARBITER HOOKS
    
    def on_start(self):
        for m in self._monitors:
            if hasattr(m,'cfg'):
                self.cfg = m.cfg
                break
        self.init_runner()
        for m in self._monitors:
            m.start()
            
    def on_task(self):
        sig = self.arbiter_task()
        if self._stopping:
            if not self._linked_actors:
                self.ioloop.stop()
        else:
            if sig is None:
                self.manage_actors()
                
    def on_manage_actor(self, actor):
        '''If an actor failed to notify itself to the arbiter for more than the timeout. Stop the arbiter.'''
        gap = time() - actor.notified
        if gap > actor.timeout:
            if actor.stopping < self.STOPPING_LOOPS:
                self.log.info('Stopping {0}. Timeout surpassed.'.format(actor))
                if not actor.stopping:
                    self.proxy().stop(actor)
            else:
                self.log.warn('Terminating {0}. Timeout surpassed.'.format(actor))
                actor.terminate()
            actor.stopping += 1
            
    def on_stop(self):
        '''Stop the pools and the arbiter event loop.'''
        self.close_monitors()
        self.signal(system.SIGQUIT)
        return True
        
    def shut_down(self):
        self.stop()
    
    # LOW LEVEL FUNCTIONS
    def __repr__(self):
        return self.__class__.__name__
    
    def __str__(self):
        return self.__repr__()
    
    def _setup(self):
        if self.cfg:
            cfg = self.cfg
            if cfg.pidfile is not None:
                self.pidfile = Pidfile(cfg.pidfile)
                self.pidfile.create(self.pid)
            if cfg.daemon:
                system.daemonize()
            self._monitors[0].configure_logging()
        else:
            self.configure_logging()
                
    def add_monitor(self, monitor_class, *args):
        '''Add a new monitor to the arbiter'''
        m = spawn(monitor_class,*args,**{'impl':'monitor'})
        self._monitors.append(m)
        return m
        
    def _run(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        self.log.debug("{0} booted".format(self))
        if self.cfg.when_ready:
            self.cfg.when_ready(self)
        try:
            self.ioloop.start()
        except StopIteration:
            self.halt('Stop Iteration')
        except KeyboardInterrupt:
            self.halt('Keyboard Interrupt')
        except self.HaltServer as e:
            self.halt(reason=e.reason, sig=e.signal)
        except SystemExit:
            raise
        except Exception:
            self.halt("Unhandled exception in main loop:\n%s" % traceback.format_exc())
    
    def halt(self, reason=None, sig=None):
        """ halt arbiter """
        _msg = lambda x : x if not reason else '{0}: {1}'.format(x,reason)
        
        if self.pidfile is not None:
            self.pidfile.unlink()
            
        if sig:
            msg = _msg('Shutting down')
            self.close()
            self.log.info(msg)
            sys.exit(0)
        else:
            msg = _msg('Force termination')
            self.log.critical(msg)
            sys.exit(1)

    def close_monitors(self):
        for pool in self._monitors:
            pool.stop()
        #self._monitors = []
        
    def close(self):
        self.close_monitors()
        stop = self.proxy().stop
        for actor in self.linked_actors():
            stop(actor)
        self._stopping = True
        
    def server_info(self):
        started = self.started
        if not started:
            return
        uptime = time.time() - started
        server = {'uptime':uptime,
                  'version':pulsar.__version__,
                  'name':pulsar.SERVER_NAME,
                  'number_of_monitors':len(self._monitors),
                  'event_loops':self.ioloop.num_loops,
                  'socket':str(self.socket)}
        pools = []
        for p in self._monitors:
            pools.append(p.info())
        return {'server':server,
                'pools':pools}

    def arbiter_task(self):
        '''Called by the Event loop to perform the signal handling from the signal queue'''
        sig = None
        while True:
            try:
                sig = self.SIG_QUEUE.get(timeout = self.SIG_TIMEOUT)
            except Empty:
                sig = None
                break
            except IOError:
                sig = None
                break
            if sig not in system.SIG_NAMES:
                self.log.info("Ignoring unknown signal: %s" % sig)
                sig = None
            else:        
                signame = system.SIG_NAMES.get(sig)
                if sig in self.EXIT_SIGNALS:
                    raise self.HaltServer('Received Signal {0}.'.format(signame),sig)
                handler = getattr(self, "handle_queued_%s" % signame.lower(), None)
                if not handler:
                    self.log.debug('Cannot handle signal "{0}". No Handle'.format(signame))
                    sig = None
                else:
                    self.log.info("Handling signal: %s" % signame)
                    handler()
        return sig                
    
    def signal(self, sig, frame = None):
        signame = system.SIG_NAMES.get(sig,None)
        if signame:
            self.log.info('Received and queueing signal {0}.'.format(signame))
            self.SIG_QUEUE.put(sig)
        else:
            self.log.info('Received unknown signal "{0}". Skipping.'.format(sig))
    
    def on_actor_exit(self, actor):
        pass
