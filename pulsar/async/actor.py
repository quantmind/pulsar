import sys
import os
import atexit
import socket
from time import time
import random
import threading
from functools import partial
from multiprocessing import current_process
from multiprocessing.queues import Empty
from threading import current_thread

try:    #pragma nocover
    import queue
except ImportError: #pragma nocover
    import Queue as queue
ThreadQueue = queue.Queue

from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   ActorAlreadyStarted, LogginMixin, system, Config
from pulsar.utils.structures import AttributeDictionary
from pulsar.utils.pep import pickle, set_event_loop_policy
from pulsar.utils.events import EventHandler
from pulsar.utils import events

from .eventloop import EventLoop, setid, signal
from .defer import Deferred, log_failure
from .proxy import ActorProxy, get_proxy, ActorProxyMonitor
from .mailbox import MailboxClient
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
MIN_NOTIFY = 5     # DON'T NOTIFY BELOW THIS INTERVAL
MAX_NOTIFY = 30    # NOTIFY AT LEAST AFTER THESE SECONDS
ACTOR_TERMINATE_TIMEOUT = 2 # TIMEOUT WHEN JOINING A TERMINATING ACTOR
ACTOR_TIMEOUT_TOLERANCE = 0.6
ACTOR_STOPPING_LOOPS = 10

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
        

class Actor(EventHandler, Pulsar):
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
    EVENTS = ('on_stop', 'on_start', 'on_info')
    exit_code = None
    mailbox = None
    signal_queue = None
    
    def __init__(self, impl):
        self.state = ACTOR_STATES.INITIAL
        self.__impl = impl
        for name in self.EVENTS:
            hook = impl.params.pop(name, None)
            if hook:
                self.hooks[name].append(hook)
        self.monitor = impl.params.pop('monitor', None)
        self.params = AttributeDictionary(**impl.params)
        del impl.params
        setid(self)
        self.on_exit = Deferred()

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
            events.fire('start', self)
            self.state = ACTOR_STATES.STARTING
            self._run()

    def send(self, target, action, *args, **params):
        '''Send a message to *target* to perform *action* with given
parameters *params*.'''
        target = self.monitor if target == 'monitor' else target
        if isinstance(target, ActorProxyMonitor):
            mailbox = target.mailbox
        else:
            mailbox = self.mailbox
        return mailbox.request(action, self, target, args, params)
    
    def io_poller(self):
        '''Return the :class:`EventLoop.io` handler. By default it return
nothing so that the best handler for the system is chosen.'''
        return None
    
    def spawn(self, **params):
        raise RuntimeError('Cannot spawn an actor from an actor.')
    
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
    ##  ACTOR HOOKS
    ############################################################################
    def on_start(self):
        '''The :ref:`actor callback <actor-callbacks>` run **once** just before
the actor starts (after forking) its event loop. Every attribute is available,
therefore this is a chance to setup to perform custom initialisation
before the actor starts running.'''
        pass

    def on_stop(self):
        '''The :ref:`actor callback <actor-callbacks>` run once just before
 the actor stops running.'''
        pass

    def on_info(self, data):
        '''An :ref:`actor callback <actor-callbacks>` executed when
 obtaining information about the actor. It can be used to add additional
 data to the *data* dictionary. Information about the actor is obtained
 via the :meth:`Actor.info` method which is also exposed
 as a remote function.

 :parameter data: dictionary of data with information about the actor.
 :rtype: a dictionary of pickable data.'''
        return data

    ############################################################################
    # STOPPING
    ############################################################################
    def stop(self, exc=None):
        '''Stop the actor by stopping its :attr:`Actor.requestloop`
and closing its :attr:`Actor.mailbox`. Once everything is closed
properly this actor will go out of scope.'''
        log_failure(exc)
        if self.state <= ACTOR_STATES.RUN:
            # The actor has not started the stopping process. Starts it now.
            events.fire('stop', self)
            self.exit_code = 1 if exc else 0
            self.state = ACTOR_STATES.STOPPING
            # if CPU bound and the requestloop is still running, stop it
            if self.cpubound and self.requestloop.running:
                self.requestloop.call_soon_threadsafe(self._stop)
                self.requestloop.stop()
            else:
                self._stop()
        else:
            # The actor has finished the stopping process.
            #Remove itself from the actors dictionary
            remove_actor(self)
            try:
                events.fire('exit', self)
                self.fire('on_stop', self)
                self.on_stop()
            finally:
                self.on_exit.callback(self)
        return self.on_exit
    
    def on_stop(self):
        self.logger.debug('%s exited', self)
        
    def _stop(self):
        '''Exit from the :class:`Actor` domain.'''
        if not self.stopped():
            self.state = ACTOR_STATES.CLOSE
            self.mailbox.close()
            
    ############################################################################
    #    INTERNALS
    ############################################################################
    def periodic_task(self):
        if self.can_continue():
            if self.running():
                self.send('monitor', 'notify', self.info())
                secs = max(ACTOR_TIMEOUT_TOLERANCE*self.cfg.timeout, MIN_NOTIFY)
                next = min(secs, MAX_NOTIFY)
                self.ioloop.call_later(next, self.periodic_task)
            else:
                # The actor is not yet active, come back at the next requestloop
                self.requestloop.call_soon_threadsafe(self.periodic_task)
    
    def hand_shake(self):
        try:
            a = get_actor()
            if a is not self:
                set_actor(self)
            self.logger.info('%s started', self)
            self.state = ACTOR_STATES.RUN
            self.on_start()
            self.fire('on_start', self)
            self.periodic_task()
        except Exception as e:
            self.stop(e)
    
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
        return self.on_info(data)

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
            if is_mainthread() and signal:
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
        except:
            pass
        exc = None
        try:
            self.requestloop.run()
        except Exception as e:
            self.logger.exception('Unhadled exception in %s', self.requestloop)
            exc = e
        finally:
            self.stop(exc)
        
    def _mailbox(self):
        client = MailboxClient(self.monitor.address, self)
        client.event_loop.call_soon_threadsafe(self.hand_shake)
        return client