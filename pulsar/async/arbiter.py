from time import time
import os
import sys
import signal
from multiprocessing import current_process, Lock
from multiprocessing.queues import Empty

import pulsar
from pulsar.utils import system
from pulsar.utils.tools import Pidfile, gen_unique_id
from pulsar.utils.py2py3 import itervalues, iteritems

from .impl import actor_impl
from .actor import Actor, get_actor, send
from .monitor import PoolMixin
from .proxy import ActorCallBacks, ActorProxyDeferred
from .defer import ThreadQueue


process_global = pulsar.process_global

__all__ = ['arbiter','spawn','Arbiter']


def arbiter(daemonize = False):
    '''Obtain the arbiter instance.'''
    arbiter = process_global('_arbiter')
    if not arbiter:
        arbiter = Arbiter.spawn(Arbiter,
                                impl='monitor',
                                daemonize=daemonize)
        process_global('_arbiter',arbiter,True)
    return arbiter
    
    
def spawn(**kwargs):
    '''Spawn a new :class:`Actor` instance and return an
:class:`ActorProxyDeferred`. This method
can be used from any :class:`Actor`. If not in the :class:`Arbiter` domain,
the method send a request to the :class:`Arbiter` to spawn a new actor, once
the arbiter creates the actor it returns the proxy to the original caller.

:rtype: an :class:`ActorProxyDeferred`.

A typical usage::

    >>> a = spawn()
    >>> a.aid
    'ba42b02b'
    >>> a.called
    True
    >>> p = a.result
    >>> p.address
    ('127.0.0.1', 46691)
    '''
    aid = gen_unique_id()[:8]
    kwargs['aid'] = aid
    actor = get_actor()
    # The actor is not the Arbiter. We send a message to the Arbiter to spawn
    # a new Actor
    if not isinstance(actor, Arbiter):
        msg = send('arbiter', 'spawn', **kwargs).add_callback(actor.link_actor)
    else:
        msg = actor.spawn(**kwargs)
    return ActorProxyDeferred(aid, msg)


class Arbiter(PoolMixin, Actor):
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
    STOPPING_LOOPS = 20
    SIG_TIMEOUT = 0.01
    EXIT_SIGNALS = (signal.SIGINT,
                    signal.SIGTERM,
                    signal.SIGABRT,
                    system.SIGQUIT)
    HaltServer = pulsar.HaltServer
    lock = Lock()
    _name = 'Arbiter'
    
    ############################################################################
    # ARBITER HIGH LEVEL API
    ############################################################################
    
    def is_arbiter(self):
        return True
    
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
        m = arbiter().spawn(monitor_class,**kwargs)
        self._linked_actors[m.aid] = m
        self._monitors[m.name] = m
        return m
    
    @classmethod
    def spawn(cls, actorcls = None, aid = None, **kwargs):
        '''Create a new :class:`Actor` and return its
:class:`ActorProxyMonitor`.'''
        actorcls = actorcls or Actor
        arbiter = process_global('_arbiter')
        cls.lock.acquire()
        try:
            if arbiter:
                arbiter.actor_age += 1
                kwargs['age'] = arbiter.actor_age
                kwargs['ppid'] = arbiter.ppid
            impl = kwargs.pop('impl',actorcls.DEFAULT_IMPLEMENTATION)
            timeout = max(kwargs.pop('timeout',cls.DEFAULT_ACTOR_TIMEOUT),
                          cls.MINIMUM_ACTOR_TIMEOUT)
            actor_maker = actor_impl(impl, actorcls, timeout,
                                     arbiter, aid, kwargs)
            monitor = actor_maker.proxy_monitor()
            # Add to the list of managed actors if this is a remote actor
            if monitor:
                arbiter.MANAGED_ACTORS[actor_maker.aid] = monitor
                actor_maker.start()
                return monitor.on_address.add_callback(
                                    lambda address: monitor.proxy)
                return monitor
            else:
                return actor_maker.actor
        finally:
            cls.lock.release()
            
    @property
    def close_signal(self):
        '''Return the signal that caused the arbiter to stop.'''
        return self._close_signal
        
    def isprocess(self):
        return True
    
    def get_all_monitors(self):
        '''A dictionary of all :class:`Monitor` in the arbiter'''
        return dict(((mon.name,mon.proxy) for mon in itervalues(self.monitors)))
    
    def close_monitors(self):
        '''Clase all :class:`Monitor` instances in the arbiter.'''
        for pool in list(itervalues(self._monitors)):
            pool.stop()
            
    def info(self, full = False):
        if not self.started():
            return
        pools = []
        for p in itervalues(self.monitors):
            pools.append(p.info(full))
        return ActorCallBacks(self,pools).add_callback(self._info)
    
    def configure_logging(self, config = None):
        if self._monitors:
            monitor = list(self._monitors.values())[0]
            monitor.configure_logging(config = config)
            self.setlog()
            self.loglevel = monitor.loglevel
        else:
            super(Arbiter,self).configure_logging(config = config)
 
    @property
    def cfg(self):
        return self.get('cfg')
    
    ############################################################################
    # OVERRIDE ACTOR HOOKS
    ############################################################################
    
    def on_init(self, daemonize = False, **kwargs):
        if process_global('_arbiter'):
            raise pulsar.PulsarException('Arbiter already created')
        if current_process().daemon:
            raise pulsar.PulsarException(
                    'Cannot create the arbiter in a daemon process')
        PoolMixin.on_init(self,**kwargs)
        self._repr = self._name
        self._close_signal = None
        os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        if daemonize:
            system.daemonize()
        self.actor_age = 0
        self.reexec_pid = 0
        self.SIG_QUEUE = ThreadQueue()
    
    def on_start(self):
        cfg = self.get('cfg')
        if cfg:
            pidfile = cfg.pidfile
        
            if pidfile is not None:
                p = Pidfile(cfg.pidfile)
                p.create(self.pid)
                self.local['pidfile'] = p
        PoolMixin.on_start(self)
        
    def on_task(self):
        '''Override the :class:`Actor.on_task` callback to perfrom the
arbiter tasks at every iteration in the event loop.'''
        sig = self._arbiter_task()
        if sig is None:
            self.manage_actors()
            for m in list(itervalues(self._monitors)):
                if m.started():
                    if not m.running():
                        self.log.info('Removing monitor {0}'.format(m))
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
            
    def on_stop(self):
        '''Stop the pools the message queue and remaining actors.'''
        self.close_monitors()
        self.close_actors()
        self._stopping_start = time()
        self._close_message_queue()
        return self._on_actors_stopped()
    
    def on_exit(self):
        p = self.get('pidfile')
        if p is not None:
            p.unlink()
        if self.MANAGED_ACTORS:
            self._state = self.TERMINATE
    
    ############################################################################
    # ADDITIONAL REMOTES
    ############################################################################
    def actor_spawn(self, caller, linked_actors = None, **kwargs):
        linked_actors = linked_actors if linked_actors is not None else {}
        linked_actors[caller.aid] = caller.proxy
        return self.spawn(linked_actors = linked_actors, **kwargs)
        #p = self.spawn(linked_actors = linked_actors, **kwargs)
        #return p.on_address.add_callback(lambda address: p.proxy)
     
    def actor_kill_actor(self, caller, aid):
        '''Remote function for ``caller`` to kill an actor with id ``aid``'''
        a = self.get_actor(aid)
        if a:
            if isinstance(a,Actor):
                a.stop()
            else:
                a.send(self,'stop')
            return 'stopped {0}'.format(a)
        else:
            self.log.info('Could not kill "{0}" no such actor'.format(aid))
    
    ############################################################################
    # INTERNALS
    ############################################################################
        
    def _run(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        try:
            self.get('cfg').when_ready(self)
        except:
            pass
        try:
            self.ioloop.start()
        except StopIteration:
            self._halt('Stop Iteration')
        except KeyboardInterrupt:
            self._halt('Keyboard Interrupt')
        except self.HaltServer as e:
            self._halt(reason=str(e), sig=e.signal)
        except SystemExit:
            raise
        except:
            self.log.critical("Unhandled exception in main loop.",
                              exc_info = True)
            self._halt()
    
    def _halt(self, reason=None, sig=None):
        #halt the arbiter. If there no signal ``sig`` it is an unexpected exit
        x = 'Shutting down pulsar arbiter'
        _msg = lambda : x if not reason else '{0}: {1}'.format(x,reason)        
        if not sig:
            self.log.critical(_msg())
        else:
            self.log.info(_msg())
        self._close_signal = sig
        self.stop()
            
    def _on_actors_stopped(self):
        if self.MANAGED_ACTORS:
            if time() - self._stopping_start < self.CLOSE_TIMEOUT:
                return self.ioloop.add_callback(self._on_actors_stopped)
            else:
                self.log.warning('There are {0} actors still alive.'\
                                 .format(len(self.MANAGED_ACTORS)))
        return self.ioloop.stop()
    
    def _close_message_queue(self):   
        return
    
    def _info(self, result):
        server = super(Arbiter,self).info()
        server.update({'version':pulsar.__version__,
                       'name':pulsar.SERVER_NAME,
                       'number_of_monitors':len(self._monitors),
                       'number_of_actors':len(self.MANAGED_ACTORS)})
        return {'server':server,
                'monitors':result}

    def _arbiter_task(self):
        '''Called by the Event loop to perform the signal handling from the
signal queue'''
        sig = None
        while True:
            try:
                sig = self.SIG_QUEUE.get(timeout = self.SIG_TIMEOUT)
            except (Empty,IOError):
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
            if self.ioloop.running():
                self.log.debug('Received and queueing signal {0}.'\
                               .format(signame))
                self.SIG_QUEUE.put(sig)
                self.ioloop.wake()
            else:
                pass
                #exit(1)
        else:
            self.log.debug('Received unknown signal "{0}". Skipping.'\
                          .format(sig))

    

    