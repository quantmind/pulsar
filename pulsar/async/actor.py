import sys
import os
import atexit
import logging
from time import time
import random
from functools import partial
from multiprocessing.queues import Empty

try:    #pragma nocover
    import queue
except ImportError: #pragma nocover
    import Queue as queue
ThreadQueue = queue.Queue

from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   ActorAlreadyStarted, system, Config, platform
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils.pep import pickle, set_event_loop_policy, itervalues
from pulsar.utils.log import LogginMixin

from .eventloop import EventLoop, setid, signal
from .defer import Deferred, EventHandler, log_failure
from .proxy import ActorProxy, get_proxy, ActorProxyMonitor, ActorIdentity
from .mailbox import MailboxClient, command_in_context
from .access import set_actor, is_mainthread, get_actor, remove_actor, NOTHING


__all__ = ['is_actor', 'send', 'Actor', 'ACTOR_STATES', 'Pulsar', 'ThreadQueue']

ACTOR_STATES = AttributeDictionary(INITIAL=0X0,
                                   INACTIVE=0X1,
                                   STARTING=0x2,
                                   RUN=0x3,
                                   STOPPING=0x4,
                                   CLOSE=0x5,
                                   TERMINATE=0x6)
ACTOR_STATES.DESCRIPTION = {ACTOR_STATES.INACTIVE: 'inactive',
                            ACTOR_STATES.INITIAL: 'initial',
                            ACTOR_STATES.STARTING: 'starting',
                            ACTOR_STATES.RUN: 'running',
                            ACTOR_STATES.STOPPING: 'stopping',
                            ACTOR_STATES.CLOSE: 'closed',
                            ACTOR_STATES.TERMINATE:'terminated'}
SPECIAL_ACTORS = ('monitor', 'arbiter')
#
# LOW LEVEL CONSTANTS - NO NEED TO CHANGE THOSE ###########################
MIN_NOTIFY = 3     # DON'T NOTIFY BELOW THIS INTERVAL
MAX_NOTIFY = 30    # NOTIFY AT LEAST AFTER THESE SECONDS
ACTOR_TIMEOUT_TOLE = 0.3  # NOTIFY AFTER THIS TIMES THE TIMEOUT
ACTOR_TERMINATE_TIMEOUT = 2 # TIMEOUT WHEN JOINING A TERMINATING ACTOR

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

.. attribute:: cpubound

    Indicates if the :class:`Actor` is a :ref:`CPU-bound worker <cpubound>`
    or a I/O-bound one.

.. attribute:: requestloop

    The :class:`EventLoop` to listen for requests.
    
.. attribute:: ioloop

    An instance of :class:`EventLoop` which listen for input/output events
    on a socket.  This is different from the :attr:`requestloop` only
    for :ref:`CPU-bound actors <cpubound>`.

.. attribute:: mailbox

    Used to send and receive :ref:`actor messages <api-remote_commands>`.
    
.. attribute:: proxy

    Instance of a :class:`ActorProxy` holding a reference
    to this :class:`Actor`. The proxy is a lightweight representation
    of the actor which can be shared across different processes
    (i.e. it is pickable).

.. attribute:: params

    Contains parameters which are passed to actors spawned by this actor.
    
**METHODS**
'''
    ONE_TIME_EVENTS = ('stop', 'start')
    exit_code = None
    mailbox = None
    signal_queue = None
    
    def __init__(self, impl):
        super(Actor, self).__init__()
        self.state = ACTOR_STATES.INITIAL
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
    def impl(self):
        return self.__impl
    
    @property
    def aid(self):
        return self.__impl.aid
    
    @property
    def cfg(self):
        return self.__impl.cfg
    
    @property
    def name(self):
        return self.__impl.name
    
    @property
    def proxy(self):
        return ActorProxy(self)

    @property
    def address(self):
        return self.mailbox.address

    @property
    def ioloop(self):
        return self.mailbox.event_loop

    @property
    def cpubound(self):
        return getattr(self.requestloop, 'cpubound', False)

    @property
    def info_state(self):
        '''Current state description. One of ``initial``, ``running``,
 ``stopping``, ``closed`` and ``terminated``.'''
        return ACTOR_STATES.DESCRIPTION[self.state]

    ############################################################################
    ##    HIGH LEVEL API METHODS
    ############################################################################
    def start(self):
        '''Called after forking to start the actor's life. This is where
logging is configured, the :attr:`Actor.mailbox` is registered and the
:attr:`Actor.ioloop` is initialised and started.'''
        if self.state == ACTOR_STATES.INITIAL:
            self._started = time() 
            self.configure_logging()
            self._setup_ioloop()
            self.state = ACTOR_STATES.STARTING
            self._run()

    def send(self, target, action, *args, **params):
        '''Send a message to *target* to perform *action* with given
parameters *params*.'''
        target = self.monitor if target == 'monitor' else target
        if isinstance(target, ActorProxyMonitor):
            mailbox = target.mailbox
        else:
            actor = self.get_actor(target)
            if isinstance(actor, Actor):
                # this occur when sending a message from arbiter tomonitors or
                # viceversa. Same signature as mailbox.request
                return command_in_context(action, self, actor, args, params)
            mailbox = self.mailbox
        return mailbox.request(action, self, target, args, params)
    
    def io_poller(self):
        '''Return the :class:`EventLoop.io` handler. By default it return
nothing so that the best handler for the system is chosen.'''
        return None
    
    def spawn(self, **params):
        raise RuntimeError('Cannot spawn an actor from an actor.')
    
    def stop(self, exc=None):
        '''Stop the actor by stopping its :attr:`Actor.requestloop`
and closing its :attr:`Actor.mailbox`. Once everything is closed
properly this actor will go out of scope.'''
        log_failure(exc)
        if self.state <= ACTOR_STATES.RUN:
            # The actor has not started the stopping process. Starts it now.
            self.exit_code = 1 if exc else 0
            self.state = ACTOR_STATES.STOPPING
            # if CPU bound and the requestloop is still running, stop it
            if self.cpubound and self.ioloop.running:
                # shuts down the mailbox first
                self.ioloop.call_soon_threadsafe(self._stop)
                self.mailbox.close()
            else:
                self._stop()
        elif self.stopped():
            # The actor has finished the stopping process.
            #Remove itself from the actors dictionary
            remove_actor(self)
            self.fire_event('stop')
        return self.event('stop')
    
    ###############################################################  STATES
    def running(self):
        '''``True`` if actor is running.'''
        return self.state == ACTOR_STATES.RUN
    
    def active(self):
        '''``True`` if actor is active by being both running and having
the :attr:`ioloop` running.'''
        return self.running()
    
    def started(self):
        '''``True`` if actor has started. It does not necessarily
mean it is running.'''
        return self.state >= ACTOR_STATES.RUN

    def closed(self):
        '''``True`` if actor has exited in an clean fashion.'''
        return self.state == ACTOR_STATES.CLOSE

    def stopped(self):
        '''``True`` if actor has exited.'''
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
        if self.cpubound:
            self.requestloop.stop()
        else:
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
                    self.ioloop.call_later(next, self.periodic_task)
                    return r
            else:
                # The actor is not yet active, come back at the next requestloop
                self.requestloop.call_soon_threadsafe(self.periodic_task)
        
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
        requestloop  = self.requestloop
        actor = {'name': self.name,
                 'state': self.info_state,
                 'actor_id': self.aid,
                 'uptime': time() - self._started,
                 'thread_id': self.tid,
                 'process_id': self.pid,
                 'is_process': isp,
                 'age': self.impl.age}
        events = {'callbacks': len(self.ioloop._callbacks),
                  'io_loops': self.ioloop.num_loops}
        if self.cpubound:
            events['request_loops'] = requestloop.num_loops
        data = {'actor': actor, 'events': events}
        if isp:
            data['system'] = system.system_info(self.pid)
        return data

    def _setup_ioloop(self):
        # Internal function called at the start of the actor. It builds the
        # event loop which will consume events on file descriptors
        # Build the mailbox first so that when the mailbox closes, it shut
        # down the eventloop.
        self.requestloop = EventLoop(io=self.io_poller(), logger=self.logger,
                                     poll_timeout=self.params.poll_timeout)
        self.mailbox = self._mailbox()
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
                        self.requestloop.add_signal_handler(sig, handler)
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
                        self.logger.warn("Got signal %s. Stopping.", signame)
                        self.stop()
                        return False
                    else:
                        self.logger.debug('No handler for signal %s.', signame)
        return True
    
    def _run(self):
        try:
            self.cfg.when_ready(self)
        except Exception:
            pass
        exc = None
        try:
            self.requestloop.run()
        except Exception as e:
            self.logger.exception('Unhandled exception in %s', self.requestloop)
            exc = e
        finally:
            self.stop(exc)
        
    def _mailbox(self):
        client = MailboxClient(self.monitor.address, self)
        client.event_loop.call_soon_threadsafe(self.hand_shake)
        return client