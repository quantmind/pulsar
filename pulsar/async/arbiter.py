from time import time
import os
import sys
import signal
from multiprocessing import current_process
from multiprocessing.queues import Empty

import pulsar
from pulsar.utils import system
from pulsar.utils.tools import Pidfile
from pulsar.utils.security import gen_unique_id
from pulsar import HaltServer

from .defer import itervalues, iteritems, multi_async
from .actor import Actor, send
from .monitor import PoolMixin
from .proxy import ActorProxyDeferred
from .access import get_actor, set_actor
from . import commands

try:    #pragma nocover
    import queue
except ImportError: #pragma nocover
    import Queue as queue
ThreadQueue = queue.Queue


process_global = pulsar.process_global

__all__ = ['arbiter', 'spawn', 'Arbiter', 'ThreadQueue']


def arbiter(daemonize=False):
    '''Obtain the arbiter instance.'''
    arbiter = get_actor()
    if arbiter is None:
        return set_actor(Arbiter.make(daemonize=daemonize))
    elif isinstance(arbiter, Actor) and arbiter.is_arbiter():
        return arbiter


def spawn(**kwargs):
    '''Spawn a new :class:`Actor` and return an :class:`ActorProxyDeferred`.
This method can be used from any :class:`Actor`.
If not in the :class:`Arbiter` domain,
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
    # The actor is not the Arbiter domain.
    # We send a message to the Arbiter to spawn a new Actor
    if not isinstance(actor, Arbiter):
        msg = send('arbiter', 'spawn', **kwargs)\
                        .add_callback(actor.link_actor)
        return ActorProxyDeferred(aid, msg)
    else:
        return actor.spawn(**kwargs)

    
class Arbiter(PoolMixin, Actor):
    '''The Arbiter is a special :class:`Monitor`. It is used as singletone
in the main process and it manages one or more :class:`Monitor`.
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
    STOPPING_LOOPS = 20
    SIG_TIMEOUT = 0.01
    EXIT_SIGNALS = (signal.SIGINT,
                    signal.SIGTERM,
                    signal.SIGABRT,
                    system.SIGQUIT)

    ############################################################################
    # ARBITER HIGH LEVEL API
    ############################################################################
    def is_arbiter(self):
        return True

    def add_monitor(self, monitor_class, monitor_name, **kwargs):
        '''Add a new :class:`Monitor` to the :class:`Arbiter`.

:parameter monitor_class: a :class:`pulsar.Monitor` class.
:parameter monitor_name: a unique name for the monitor.
:parameter kwargs: dictionary of key-valued parameters for the monitor.
:rtype: an instance of a :class:`pulsar.Monitor`.'''
        if monitor_name in self._monitors:
            raise KeyError('Monitor "{0}" already available'\
                           .format(monitor_name))
        kwargs['name'] = monitor_name
        kwargs['aid'] = monitor_name
        m = arbiter().spawn(monitor_class, **kwargs)
        # Monitors don't hold other monitors
        m._monitors.clear()
        self._linked_actors[m.aid] = m
        self._monitors[m.name] = m
        return m

    @classmethod
    def make(cls, commands_set = None, impl=None, **kwargs):
        commands_set = set(commands_set or commands.actor_commands)
        commands_set.update(commands.arbiter_commands)
        return cls._spawn_actor(None, cls, 'arbiter', commands_set, **kwargs)

    @property
    def close_signal(self):
        '''Return the signal that caused the arbiter to stop.'''
        return self._close_signal

    def isprocess(self):
        return True

    def get_all_monitors(self):
        '''A dictionary of all :class:`Monitor` in the arbiter'''
        return dict(((mon.name,mon.proxy) for mon in itervalues(self.monitors)))

    @multi_async
    def close_monitors(self):
        '''Close all :class:`Monitor` at once.'''
        for pool in list(itervalues(self._monitors)):
            yield pool.stop()

    def info(self):
        if not self.started():
            return
        server = super(Arbiter,self).info()
        monitors = [p.info() for p in itervalues(self.monitors)]
        server.update({
            'version':pulsar.__version__,
            'name':pulsar.SERVER_NAME,
            'number_of_monitors':len(self._monitors),
            'number_of_actors':len(self.MANAGED_ACTORS),
            'workers': [a.info for a in itervalues(self.MANAGED_ACTORS)]})
        return {'server': server,
                'monitors': monitors}

    def configure_logging(self, config = None):
        if self._monitors:
            monitor = list(self._monitors.values())[0]
            monitor.configure_logging(config = config)
            self.setlog()
            self.loglevel = monitor.loglevel
        else:
            super(Arbiter,self).configure_logging(config = config)

    ############################################################################
    # OVERRIDE ACTOR HOOKS
    ############################################################################
    def on_init(self, daemonize=False, **kwargs):
        testing = kwargs.pop('__test_arbiter__',False)
        if not testing:
            if current_process().daemon:
                raise pulsar.PulsarException(
                        'Cannot create the arbiter in a daemon process')
            if isinstance(get_actor(), self.__class__):
                raise pulsar.PulsarException('Arbiter already created')
            os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        self._close_signal = None
        if daemonize:
            system.daemonize()
        self.actor_age = 0
        self.reexec_pid = 0
        self.SIG_QUEUE = ThreadQueue()
        return PoolMixin.on_init(self, **kwargs)

    def on_start(self):
        pidfile = self.get('pidfile')
        if pidfile is not None:
            p = Pidfile(pidfile)
            p.create(self.pid)
            self.local.pidfile = p

    def on_task(self):
        '''Override the :class:`Actor.on_task` callback to perform the
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
        try:
            self.cfg.arbiter_task(self)
        except:
            pass

    def manage_actor(self, actor):
        '''If an actor failed to notify itself to the arbiter for more than
the timeout. Stop the arbiter.'''
        if self.running():
            gap = time() - actor.notified
            if gap > actor.timeout:
                if actor.stopping_loops < self.STOPPING_LOOPS:
                    if not actor.stopping_loops:
                        self.log.info(\
                            'Stopping {0}. Timeout surpassed.'.format(actor))
                        self.send(actor, 'stop')
                else:
                    self.log.warn(\
                            'Terminating {0}. Timeout surpassed.'.format(actor))
                    actor.terminate()
                    actor.join(self.JOIN_TIMEOUT)
                actor.stopping_loops += 1

    def on_stop(self):
        '''Stop the pools the message queue and remaining actors.'''
        # close all monitors
        self.log.info('Shutting down server.')
        yield self.close_monitors()
        # close remaining actors
        yield self.close_actors()
        yield self._close_message_queue()

    def on_exit(self):
        p = self.get('pidfile')
        if p is not None:
            p.unlink()
        if self.MANAGED_ACTORS:
            self._state = self.TERMINATE

    ############################################################################
    # INTERNALS
    ############################################################################

    def _run(self):
        """\
        Initialize the arbiter. Start listening and set pidfile if needed.
        """
        self._on_run()
        try:
            self.cfg.get('when_ready')(self)
        except:
            pass
        try:
            self.ioloop.start()
        except StopIteration:
            self._halt('Stop Iteration')
        except KeyboardInterrupt:
            self._halt('Keyboard Interrupt')
        except HaltServer as e:
            self._halt(reason=str(e), sig=e.signal)
        except SystemExit:
            raise
        except:
            self._halt("Unhandled exception in main loop.")

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

    def _close_message_queue(self):
        return

    def _arbiter_task(self):
        '''Called by the Event loop to perform the signal handling from the
signal queue'''
        sig = None
        while True:
            try:
                sig = self.SIG_QUEUE.get(timeout=self.SIG_TIMEOUT)
            except (Empty,IOError):
                break
            if sig not in system.SIG_NAMES:
                self.log.info("Ignoring unknown signal: %s" % sig)
                sig = None
            else:
                signame = system.SIG_NAMES.get(sig)
                if sig in self.EXIT_SIGNALS:
                    raise HaltServer('Received Signal {0}.'\
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

    def signal(self, sig, frame=None):
        signame = system.SIG_NAMES.get(sig, None)
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

