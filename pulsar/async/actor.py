import sys
import os
import atexit
import logging
from time import time
import random
from functools import partial

from pulsar import AlreadyRegistered, HaltServer, CommandError,\
                   ActorAlreadyStarted, system, Config, platform
from pulsar.utils.pep import pickle, set_event_loop_policy, itervalues
from pulsar.utils.log import LogginMixin

from .eventloop import EventLoop, setid, signal
from .defer import Deferred, EventHandler, log_failure
from .threads import Empty, ThreadQueue, ThreadPool
from .proxy import ActorProxy, get_proxy, ActorProxyMonitor, ActorIdentity
from .mailbox import MailboxClient, command_in_context
from .access import set_actor, is_mainthread, get_actor, remove_actor
from .consts import *


__all__ = ['is_actor', 'send', 'Actor', 'ACTOR_STATES', 'Pulsar', 'ThreadQueue']


def is_actor(obj):
    return isinstance(obj, Actor)

def send(target, action, *args, **params):
    '''Send a :ref:`message <api-remote_commands>` to *target* to perform
a given *action*.

:parameter target: the :class:`Actor` id or an :class:`ActorProxy` or name of
    the target actor which will receive the message.
:parameter action: the name of the :ref:`remote command <api-remote_commands>`
    to perform in the *target* :class:`Actor`.
:parameter args: positional arguments to pass to the
    :ref:`remote command <api-remote_commands>` *action*.
:parameter params: dictionary of parameters to pass to
    :ref:`remote command <api-remote_commands>` *action*.
:rtype: a :class:`Deferred` if the action acknowledge the caller or `None`.

Typical example::

    >>> a = spawn()
    >>> r = a.add_callback(lambda p: send(p,'ping'))
    >>> r.result
    'pong'
'''
    return get_actor().send(target, action, *args, **params)

def stop_on_error(f):
    def _(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except Exception as e:
            self.stop(e)
    return _


class Pulsar(LogginMixin):
    
    def configure_logging(self, **kwargs):
        configure_logging = super(Pulsar, self).configure_logging
        configure_logging(logger='pulsar',
                          config=self.cfg.logconfig,
                          level=self.cfg.loglevel,
                          handlers=self.cfg.loghandlers) 
        configure_logging(logger='pulsar.%s' % self.name,
                          config=self.cfg.logconfig,
                          level=self.cfg.loglevel,
                          handlers=self.cfg.loghandlers)
        

class Actor(EventHandler, Pulsar, ActorIdentity):
    '''The base class for parallel execution in pulsar. In computer science,
the **Actor model** is a mathematical model of concurrent computation that
treats *actors* as the universal primitives of computation.
In response to a message that it receives, an actor can make local decisions,
create more actors, send more messages, and determine how to respond to
the next message received.

The current implementation allows for actors to perform specific tasks such
as listening to a socket, acting as http server, consuming
a task queue and so forth.

To spawn a new actor::

    >>> from pulsar import spawn
    >>> a = spawn()
    >>> a.is_alive()
    True

Here ``a`` is actually a reference to the remote actor, it is
an :class:`ActorProxy`.

**ATTRIBUTES**

.. attribute:: name

    The name of this :class:`Actor`.
    
.. attribute:: aid

    Unique ID for this :class:`Actor`.
    
.. attribute:: impl

    The :class:`Concurrency` implementation for this :class:`Actor`.
    
.. attribute:: event_loop

    An instance of :class:`EventLoop` which listen for input/output events
    on sockets or socket-like :class:`Transport`. It is the driver of the
    :class:`Actor`. If the :attr:event_loop` stps, the :class:`Actor` stop
    running and goes out of scope.

.. attribute:: mailbox

    Used to send and receive :ref:`actor messages <tutorials-messages>`.
    
.. attribute:: address

    The socket address for this :attr:`Actor.mailbox`.
    
.. attribute:: proxy

    Instance of a :class:`ActorProxy` holding a reference
    to this :class:`Actor`. The proxy is a lightweight representation
    of the actor which can be shared across different processes
    (i.e. it is pickable).

.. attribute:: state

    The actor :ref:`numeric state <actor-states>`.
    
.. attribute:: thread_pool

    A :class:`ThreadPool` associated with this :class:`Actor`. This attribute
    is ``None`` unless one is created via the :meth:`create_thread_pool`
    method.

.. attribute:: params

    A :class:`pulsar.utils.structures.AttributeDictionary` which contains
    parameters which are passed to actors spawned by this actor.
    
.. attribute:: info_state

    Current state description string. One of ``initial``, ``running``,
    ``stopping``, ``closed`` and ``terminated``.
 
**PUBLIC METHODS**
'''
    ONE_TIME_EVENTS = ('start', 'stopping', 'stop')
    exit_code = None
    mailbox = None
    signal_queue = None
    
    def __init__(self, impl):
        super(Actor, self).__init__()
        self.state = ACTOR_STATES.INITIAL
        self._thread_pool = None
        self.__impl = impl
        for name in self.all_events():
            hook = impl.params.pop(name, None)
            if hook:
                self.bind_event(name, hook)
        self.monitor = impl.params.pop('monitor', None)
        self.params = AttributeDictionary(**impl.params)
        self.servers = {}
        del impl.params
        setid(self)
        try:
            self.cfg.post_fork(self)
        except Exception:
            pass

    def __repr__(self):
        return self.impl.unique_name
    
    def __str__(self):
        return self.__repr__()
    
    ############################################################### PROPERTIES
    @property
    def name(self):
        return self.__impl.name

    @property
    def aid(self):
        return self.__impl.aid
    
    @property
    def impl(self):
        return self.__impl
    
    @property
    def cfg(self):
        return self.__impl.cfg
    
    @property
    def proxy(self):
        return ActorProxy(self)

    @property
    def address(self):
        return self.mailbox.address

    @property
    def event_loop(self):
        return self.mailbox.event_loop
    
    @property
    def thread_pool(self):
        return self._thread_pool

    @property
    def info_state(self):
        return ACTOR_STATES.DESCRIPTION[self.state]

    ############################################################################
    ##    HIGH LEVEL API METHODS
    ############################################################################
    def start(self):
        '''Called after forking to start the actor's life. This is where
logging is configured, the :attr:`Actor.mailbox` is registered and the
:attr:`Actor.event_loop` is initialised and started. Calling this method
more than once does nothing.'''
        if self.state == ACTOR_STATES.INITIAL:
            self._started = time() 
            self.configure_logging()
            self._setup_event_loop()
            self.state = ACTOR_STATES.STARTING
            self._run()

    def send(self, target, action, *args, **params):
        '''Send a message to *target* to perform *action* with given
parameters *params*.'''
        target = self.monitor if target == 'monitor' else target
        mailbox = self.mailbox
        if isinstance(target, ActorProxyMonitor):
            mailbox = target.mailbox
        else:
            actor = self.get_actor(target)
            if isinstance(actor, Actor):
                # this occur when sending a message from arbiter to monitors or
                # viceversa.
                return command_in_context(action, self, actor, args, params)
            elif isinstance(actor, ActorProxyMonitor):
                mailbox = actor.mailbox
        if hasattr(mailbox, 'request'):
            #if not mailbox.closed:
            return mailbox.request(action, self, target, args, params)
        else:
            raise CommandError('Cannot execute "%s" in %s. Unknown actor '
                               '%s.' % (action, self, target))
    
    def io_poller(self):
        '''Return a IO poller instance which sets the :class:`EventLoop.io`
handler. By default it return nothing so that the best handler for the
system is chosen.'''
        return None
    
    def spawn(self, **params):
        raise RuntimeError('Cannot spawn an actor from an actor.')
    
    def stop(self, exc=None):
        '''Stop the actor by closing its :attr:`mailbox` which
in turns stops the :attr:`event_loop`. Once everything is closed
properly this actor will go out of scope.

:param exc: optional exception.
'''
        log_failure(exc)
        if self.state <= ACTOR_STATES.RUN:
            # The actor has not started the stopping process. Starts it now.
            self.exit_code = getattr(exc, 'exit_code', 1) if exc else 0
            self.state = ACTOR_STATES.STOPPING
            self.fire_event('stopping')
            self.close_thread_pool()
            self._stop()
        elif self.stopped():
            # The actor has finished the stopping process.
            #Remove itself from the actors dictionary
            remove_actor(self)
            self.fire_event('stop')
        return self.event('stop')
    
    def create_thread_pool(self, workers=1):
        '''Create a :class:`ThreadPool` for this :class:`Actor`
if not already present.

:param workers: number of threads to use in the :class:`ThreadPool`
'''
        if self._thread_pool is None:
            self._thread_pool = ThreadPool(processes=workers)
        return self._thread_pool
    
    def close_thread_pool(self):
        '''Close the :attr:`thread_pool`.'''
        if self._thread_pool:
            self._thread_pool.close()
    ###############################################################  STATES
    def running(self):
        '''``True`` if actor is running, that is when the :attr:`state`
is equal to :ref:`ACTOR_STATES.RUN <actor-states>`.'''
        return self.state == ACTOR_STATES.RUN
    
    def active(self):
        '''``True`` if actor is active by being both running and having
the :attr:`event_loop` running.'''
        return self.running()
    
    def started(self):
        '''``True`` if actor has started. It does not necessarily
mean it is running. Its state is greater or equal
:ref:`ACTOR_STATES.RUN <actor-states>`.'''
        return self.state >= ACTOR_STATES.RUN

    def closed(self):
        '''``True`` if actor has exited in an clean fashion. It :attr:`state`
is equal to :ref:`ACTOR_STATES.CLOSE <actor-states>`.'''
        return self.state == ACTOR_STATES.CLOSE

    def stopped(self):
        '''``True`` if actor has exited so that it :attr:`state` is greater
or equal to :ref:`ACTOR_STATES.CLOSE <actor-states>`.'''
        return self.state >= ACTOR_STATES.CLOSE

    def is_arbiter(self):
        '''Return ``True`` if ``self`` is the :class:`Arbiter`.'''
        return False

    def is_monitor(self):
        '''Return ``True`` if ``self`` is a :class:`Monitor`.'''
        return False

    def is_process(self):
        '''boolean indicating if this is an actor on a child process.'''
        return self.impl.kind == 'process'

    def __reduce__(self):
        raise pickle.PicklingError('{0} - Cannot pickle Actor instances'\
                                   .format(self))
    
    ############################################################################
    #    INTERNALS
    ############################################################################
    def _stop(self):
        '''Exit from the :class:`Actor` domain.'''
        self.bind_event('stop', self._bye)
        self.state = ACTOR_STATES.CLOSE
        self.mailbox.close()
    
    def _bye(self, r):
        self.logger.debug('Bye from "%s"', self)
        return r
    
    def periodic_task(self):
        if self.can_continue():
            if self.running():
                self.logger.debug('%s notifying the monitor', self)
                # if an error occurs, shut down the actor
                try:
                    r = self.send('monitor', 'notify', self.info())\
                            .add_errback(self.stop)
                except Exception as e:
                    self.stop(e)
                else:
                    secs = max(ACTOR_TIMEOUT_TOLE*self.cfg.timeout, MIN_NOTIFY)
                    next = min(secs, MAX_NOTIFY)
                    self.event_loop.call_later(next, self.periodic_task)
                    return r
            else:
                self.event_loop.call_soon_threadsafe(self.periodic_task)
        
    @stop_on_error
    def _got_notified(self, result):
        self.logger.info('%s started', self)
        self.fire_event('start')
        
    @stop_on_error
    def hand_shake(self):
        a = get_actor()
        if a is not self:
            set_actor(self)
        self.state = ACTOR_STATES.RUN
        r = self.periodic_task()
        if r:
            r.add_callback(self._got_notified)
        else:
            self.stop()
    
    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        if aid == self.aid:
            return self
        elif aid == 'monitor':
            return self.monitor or self

    def info(self):
        '''Return a dictionary of information related to the actor
status and performance.'''
        if not self.started():
            return
        isp = self.is_process()
        actor = {'name': self.name,
                 'state': self.info_state,
                 'actor_id': self.aid,
                 'uptime': time() - self._started,
                 'thread_id': self.tid,
                 'process_id': self.pid,
                 'is_process': isp,
                 'age': self.impl.age}
        events = {'callbacks': len(self.event_loop._callbacks),
                  'io_loops': self.event_loop.num_loops}
        data = {'actor': actor, 'events': events}
        if isp:
            data['system'] = system.system_info(self.pid)
        return data

    def _setup_event_loop(self):
        # Internal function called at the start of the actor. It builds the
        # event loop which will consume events on file descriptors
        # Build the mailbox first so that when the mailbox closes, it shut
        # down the eventloop.
        event_loop = EventLoop(io=self.io_poller(), logger=self.logger,
                               poll_timeout=self.params.poll_timeout)
        self.mailbox = self._mailbox(event_loop)
        # Inject self as the actor of this thread
        set_actor(self)
        if self.is_process():
            random.seed()
            proc_name = "%s-%s" % (self.cfg.proc_name, self)
            if system.set_proctitle(proc_name):
                self.logger.debug('Set process title to %s', proc_name)
            #system.set_owner_process(cfg.uid, cfg.gid)
            if is_mainthread() and signal and not platform.is_windows:
                self.logger.debug('Installing signals')
                self.signal_queue = ThreadQueue()
                for name in system.ALL_SIGNALS:
                    sig = getattr(signal, 'SIG%s' % name)
                    try:
                        handler = partial(self.signal_queue.put, sig)
                        self.event_loop.add_signal_handler(sig, handler)
                    except ValueError:
                        break
        
    def can_continue(self):
        if self.signal_queue is not None:
            while True:
                try:
                    sig = self.signal_queue.get(timeout=0.01)
                except (Empty, IOError):
                    break
                if sig not in system.SIG_NAMES:
                    self.logger.info("Ignoring unknown signal: %s", sig)
                else:
                    signame = system.SIG_NAMES.get(sig)
                    if sig in system.EXIT_SIGNALS:
                        self.logger.warning("Got signal %s. Stopping.", signame)
                        self.stop()
                        return False
                    else:
                        self.logger.debug('No handler for signal %s.', signame)
        return True
    
    def _run(self, initial=True):
        if initial:
            try:
                self.cfg.when_ready(self)
            except Exception:
                pass
        exc = None
        try:
            self.event_loop.run()
        except Exception as e:
            exc = e
        except HaltServer as e:
            exc = e
            if e.exit_code:
                log_failure(e, msg=str(e), level='critical')
            else:
                log_failure(e, msg='Exiting server.', level='info')
        finally:
            self.stop(exc)
        
    def _mailbox(self, event_loop):
        client = MailboxClient(self.monitor.address, self, event_loop)
        client.event_loop.call_soon_threadsafe(self.hand_shake)
        return client
