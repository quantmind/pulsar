from time import time
import os
import sys
import signal
from multiprocessing.queues import Empty
from threading import Lock

import pulsar
from pulsar.utils import system
from pulsar.utils.system import IObase
from pulsar.utils.tools import Pidfile
from pulsar.utils.py2py3 import itervalues

from .impl import actor_impl
from .actor import Actor
from .monitor import PoolMixin
from .proxy import ActorCallBacks
from .defer import ThreadQueue


__all__ = ['arbiter','spawn','Arbiter']


def arbiter(daemonize = False):
    return Arbiter.instance(daemonize)
    
    
def spawn(actor_class, *args, **kwargs):
    '''Create a new :class:`Actor` instance and return its proxy in the arbiter
process domain.

:param actor_class: :class:`Actor` or one of its subclasses.
:rtype: instance of :class:`ActorProxyMonitor`'''
    return arbiter().spawn(actor_class, *args, **kwargs)
    
        

class Arbiter(PoolMixin,Actor):
    '''The Arbiter is a very special :class:`Actor`. It is used as
singletone in the main process and it manages one or more
:class:`Monitor`.  
It runs the main :class:`IOLoop` of your concurrent application.
It is the equivalent of the gunicorn_ arbiter, the twisted_ reactor
and the tornado_ eventloop.

Users access the arbiter by the high level api::

    import pulsar
    
    arbiter = pulsar.arbiter()
    
.. _gunicorn: http://gunicorn.org/
.. _twisted: http://twistedmatrix.com/trac/
.. _tornado: http://www.tornadoweb.org/
'''
    CLOSE_TIMEOUT = 3
    WORKER_BOOT_ERROR = 3
    STOPPING_LOOPS = 20
    SIG_TIMEOUT = 0.01
    EXIT_SIGNALS = (signal.SIGINT,
                    signal.SIGTERM,
                    signal.SIGABRT,
                    system.SIGQUIT)
    HaltServer = pulsar.HaltServer
    lock = Lock()
    _name = 'Arbiter'
    
    # ARBITER HIGH LEVEL API
    
    def add_monitor(self, monitor_class, monitor_name, **kwargs):
        '''Add a new :class:`pulsar.Monitor` to the arbiter.

:parameter monitor_class: a :class:`pulsar.Monitor` class.
:parameter monitor_name: a unique name for the monitor.
:parameter kwargs: dictionary of key-valued parameters for the monitor.
:rtype: an instance of a :class:`pulsar.Monitor`.'''
        kwargs['impl'] = 'monitor'
        if monitor_name in self._monitors:
            raise KeyError('Monitor "{0}" already available'\
                           .format(monitor_name))
        kwargs['name'] = monitor_name
        m = spawn(monitor_class,**kwargs)
        self._linked_actors[m.aid] = m
        self._monitors[m.name] = m
        return m
    
    @classmethod
    def instance(cls,daemonize=False):
        if not hasattr(cls,'_instance'):
            cls._instance = cls.spawn(cls,impl='monitor',daemonize=daemonize)
        return cls._instance
    
    @classmethod
    def spawn(cls, actor_class, **kwargs):
        '''Create a new :class:`Actor` and return its
:class:`ActorProxyMonitor`.'''
        cls.lock.acquire()
        try:
            arbiter = getattr(cls,'_instance',None)
            if arbiter:
                arbiter.actor_age += 1
                kwargs['age'] = arbiter.actor_age
            impl = kwargs.pop('impl',actor_class.DEFAULT_IMPLEMENTATION)
            timeout = max(kwargs.pop('timeout',cls.DEFAULT_ACTOR_TIMEOUT),
                            cls.MINIMUM_ACTOR_TIMEOUT)
            actor_maker = actor_impl(impl,actor_class,timeout,arbiter,kwargs)
            monitor = actor_maker.proxy_monitor()
            # Add to the list of managed actors if this is a remote actor
            if monitor:
                arbiter.MANAGED_ACTORS[actor_maker.aid] = monitor
                actor_maker.start()
                return monitor
            else:
                return actor_maker.actor
        finally:
            cls.lock.release()
            
    @property
    def close_signal(self):
        return self._close_signal
        
    def isprocess(self):
        return True
 
    # ARBITER HOOKS
    
    def on_start(self):
        for m in itervalues(self._monitors):
            if hasattr(m,'cfg'):
                self.cfg = m.cfg
                break
    
    def on_task(self):
        if self.running():
            sig = self.arbiter_task()
            if sig is None:
                self.manage_actors()
                for m in list(itervalues(self._monitors)):
                    if m.started():
                        if not m.running():
                            self._monitors.pop(m.name)
                    else:
                        m.start()
    
    def on_manage_actor(self, actor):
        '''If an actor failed to notify itself to the arbiter for more than
the timeout. Stop the arbiter.'''
        gap = time() - actor.notified
        if gap > actor.timeout:
            if actor.stopping_loops < self.STOPPING_LOOPS:
                if not actor.stopping_loops:
                    self.log.info(\
                        'Stopping {0}. Timeout surpassed.'.format(actor))
                    actor.send(self,'stop')
            else:
                self.log.warn(\
                        'Terminating {0}. Timeout surpassed.'.format(actor))
                actor.terminate()
            actor.stopping_loops += 1
        
    def shut_down(self):
        self.stop()
    
    # LOW LEVEL FUNCTIONS
    
    def _init_arbiter(self, impl, daemonize = False, **kwargs):
        self._repr = self._name
        self._close_signal = None
        os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        if daemonize:
            system.daemonize()
        self.actor_age = 0
        self.cfg = None
        self.pidfile = None
        self.reexec_pid = 0
        self.SIG_QUEUE = ThreadQueue()
    
    def _setup(self):
        if self.cfg:
            cfg = self.cfg
            if cfg.pidfile is not None:
                self.pidfile = Pidfile(cfg.pidfile)
                self.pidfile.create(self.pid)
        
    def _run(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        if self.cfg.when_ready:
            self.cfg.when_ready(self)
        try:
            self.ioloop.start()
            print('ciao')
        except StopIteration:
            self.halt('Stop Iteration')
        except KeyboardInterrupt:
            self.halt('Keyboard Interrupt')
        except self.HaltServer as e:
            self.halt(reason=str(e), sig=e.signal)
        except SystemExit:
            raise
        except:
            self.log.critical("Unhandled exception in main loop.",
                              exc_info = True)
            self.halt()
    
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
        self._close_signal = sig
        self.stop()

    def close_monitors(self):
        for pool in itervalues(self._monitors):
            pool.stop()
        
    def on_stop(self):
        '''Stop the pools the message queue and remaining actors.'''
        self.close_message_queue()
        self.close_monitors()
        self.close_actors()
        self._stopping_start = time()
        return self._on_actors_stopped()
    
    def _on_actors_stopped(self):
        if self.MANAGED_ACTORS:
            if time() - self._stopping_start < self.CLOSE_TIMEOUT:
                return self.ioloop.add_callback(self._on_actors_stopped)
            else:
                self.log.warning('There are {0} actors still alive.'\
                                 .format(len(self.MANAGED_ACTORS)))
        return self.ioloop.stop()
            
    def on_exit(self):
        if self.MANAGED_ACTORS:
            self._state = self.TERMINATE
    
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
        
    def info(self, full = False):
        if not self.started():
            return
        pools = []
        for p in itervalues(self.monitors):
            pools.append(p.info(full))
        return ActorCallBacks(self,pools).add_callback(self._info)
    
    def _info(self, result):
        server = super(Arbiter,self).info()
        server.update({'version':pulsar.__version__,
                       'name':pulsar.SERVER_NAME,
                       'number_of_monitors':len(self._monitors),
                       'number_of_actors':len(self.MANAGED_ACTORS)})
        return {'server':server,
                'monitors':result}

    def arbiter_task(self):
        '''Called by the Event loop to perform the signal handling from the
signal queue'''
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
                    raise self.HaltServer('Received Signal {0}.'\
                                          .format(signame),sig)
                handler = getattr(self, "handle_queued_%s"\
                                   % signame.lower(), None)
                if not handler:
                    self.log.debug('Cannot handle signal "{0}". No Handle'\
                                   .format(signame))
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
            self.ioloop.wake()
        else:
            self.log.info('Received unknown signal "{0}". Skipping.'\
                          .format(sig))
    
    def on_actor_exit(self, actor):
        pass

    def configure_logging(self,**kwargs):
        if self._monitors:
            monitor = list(self._monitors.values())[0]
            monitor.configure_logging(**kwargs)
            self.loglevel = monitor.loglevel
        else:
            super(Arbiter,self).configure_logging(**kwargs)
    
    def get_all_monitors(self):
        return dict(((mon.name,mon.proxy) for mon in itervalues(self.monitors)))
            
    # REMOTES
    
    def actor_kill_actor(self, caller, aid):
        '''Remote function for ``caller`` to kill an actor with id ``aid``'''
        a = self.get_actor(aid)
        return a.stop() if a else False
    