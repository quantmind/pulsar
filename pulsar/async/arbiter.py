from time import time
import os
import sys
import traceback
import signal
from multiprocessing.queues import Empty
from threading import current_thread, Lock

try:
    import queue
except ImportError:
    import Queue as queue
ThreadQueue = queue.Queue

import pulsar
from pulsar.utils import system
from pulsar.utils.tools import Pidfile
from pulsar.utils.py2py3 import itervalues

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
    '''The Arbiter is a special :class:`pulsar.Monitor` class. There is only one and it manages
all the actors and monitors.
In other words the Arbiter should be used as a singletone in the following way:

    import pulsar
    
    arbiter = pulsar.arbiter()
    
The arbiter runs the main event-loop, in a way similar to the twisted reactor.
'''
    CLOSE_TIMEOUT = 3
    WORKER_BOOT_ERROR = 3
    STOPPING_LOOPS = 2
    SIG_TIMEOUT = 0.001
    CLOSE_TIMEOUT = 10
    EXIT_SIGNALS = (signal.SIGINT,signal.SIGTERM,signal.SIGABRT,system.SIGQUIT)
    HaltServer = pulsar.HaltServer
    lock = Lock()
    _name = 'Arbiter'
    
    # ARBITER HIGH LEVEL API
    
    def add_monitor(self, monitor_class, actor_class, *args, **kwargs):
        '''Add a new :class:`pulsar.Monitor` to the arbiter.
MonitorS manage group of actors performing specific tasks.

:parameter monitor_class: a :class:`pulsar.Monitor` class.
:parameter actor_class: a :class:`pulsar.Actor` class but not a :class:`pulsar.Monitor` class.'''
        kwargs['impl'] = 'monitor'
        m = spawn(monitor_class,actor_class,*args,**kwargs)
        self._monitors[m.aid] = m
        return m
    
    @property
    def monitors(self):
        '''Dictionary of all :clas:`pulsar.Monitor` registered with the the arbiter.'''
        return self._monitors
    
    
    @classmethod
    def instance(cls):
        if not hasattr(cls,'_instance'):
            cls._instance = cls.spawn(cls,impl='monitor')
        return cls._instance
    
    @classmethod
    def spawn(cls, actor_class, *args, **kwargs):
        '''Create a new concurrent Actor and return its proxy.'''
        cls.lock.acquire()
        try:
            arbiter = getattr(cls,'_instance',None)
            if arbiter:
                arbiter.actor_age += 1
                kwargs['age'] = arbiter.actor_age
            impl = kwargs.pop('impl',actor_class.DEFAULT_IMPLEMENTATION)
            timeout = max(kwargs.pop('timeout',cls.DEFAULT_ACTOR_TIMEOUT),cls.MINIMUM_ACTOR_TIMEOUT)
            impl_cls = actor_class._runner_impl[impl]
            actor = impl_cls(actor_class,impl,timeout,arbiter,args,kwargs)
            monitor = actor.proxy_monitor()
            if monitor:
                arbiter.LIVE_ACTORS[actor.aid] = monitor
                actor.start()
                return monitor
            else:
                return actor.actor
        finally:
            cls.lock.release()
        
    def isprocess(self):
        return True
 
    # ARBITER HOOKS
    
    def on_start(self):
        for m in itervalues(self._monitors):
            if hasattr(m,'cfg'):
                self.cfg = m.cfg
                break
        self.init_runner()
            
    def on_task(self):
        if not self._stopping:
            sig = self.arbiter_task()
            if sig is None:
                self.manage_actors()
                for m in list(itervalues(self._monitors)):
                    if m.started():
                        if not m.is_alive():
                            self._monitors.pop(m.aid)
                    else:
                        m.start()
                
    def on_manage_actor(self, actor):
        '''If an actor failed to notify itself to the arbiter for more than the timeout. Stop the arbiter.'''
        gap = time() - actor.notified
        if gap > actor.timeout:
            if actor.stopping < self.STOPPING_LOOPS:
                self.log.info('Stopping {0}. Timeout surpassed.'.format(actor))
                if not actor.stopping:
                    self.proxy.stop(actor)
            else:
                self.log.warn('Terminating {0}. Timeout surpassed.'.format(actor))
                actor.terminate()
            actor.stopping += 1
            
    def on_stop(self):
        '''Stop the pools and the arbiter event loop.'''
        self._stopping = False
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
    
    def _init(self, impl, *args, **kwargs):
        os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        self.actor_age = 0
        self.cfg = None
        self.pidfile = None
        self.reexec_pid = 0
        self._monitors = {}
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
        super(Arbiter,self)._init(impl, *args, **kwargs)
    
    def _setup(self):
        if self.cfg:
            cfg = self.cfg
            if cfg.pidfile is not None:
                self.pidfile = Pidfile(cfg.pidfile)
                self.pidfile.create(self.pid)
            if cfg.daemon:
                system.daemonize()
        
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
        """halt arbiter. If there no signal ``sig`` it is an unexpected exit."""
        x = 'Shutting down pulsar arbiter'
        _msg = lambda : x if not reason else '{0}: {1}'.format(x,reason)
        
        if self.pidfile is not None:
            self.pidfile.unlink()
        
        if not sig:
            self.log.critical(_msg())
        else:
            self.log.info(_msg())
        self.close(sig)

    def close_monitors(self):
        for pool in itervalues(self._monitors):
            pool.stop()
        
    def close(self, sig):
        if not self._stopping:
            self._stopping = True
            self.close_message_queue()
            stop = self.proxy.stop
            for actor in self.linked_actors():
                stop(actor)
            self.close_signal = sig
            self._stop_ioloop()
            self._stop()
            
    def on_exit(self):
        '''Callback after the event loop has stopped.'''
        st = time()
        self._state = self.TERMINATE
        while time() - st < self.CLOSE_TIMEOUT:
            self.flush(closing = True)
            if not self.LIVE_ACTORS:
                self._state = self.CLOSE
                break
        self._inbox.close()
    
    def close_message_queue(self):
        return
        while True:
            try:
                self.SIG_QUEUE.get(timeout = 0)
                self.SIG_QUEUE.task_done()
            except Empty:
                self.SIG_QUEUE.join()
                break
            except:
                pass
        
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
        for p in itervalues(self._monitors):
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

    def configure_logging(self,**kwargs):
        if self._monitors:
            monitor = list(self._monitors.values())[0]
            monitor.configure_logging(**kwargs)
            self.loglevel = monitor.loglevel
        else:
            super(Arbiter,self).configure_logging(**kwargs)
    