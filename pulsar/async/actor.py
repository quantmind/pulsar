import sys
import os
import signal
import atexit
from time import time
import random
import threading
from multiprocessing import current_process
from threading import current_thread


from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   ActorAlreadyStarted, LogginMixin, system, Config
from pulsar.utils.structures import AttributeDictionary
from .eventloop import IOLoop, setid
from .proxy import ActorProxy, ActorMessage, get_command, get_proxy
from .defer import make_async, is_failure, iteritems, itervalues,\
                     pickle, safe_async, log_failure, make_async, is_async,\
                     EXIT_EXCEPTIONS
from .mailbox import IOQueue, mailbox
from .access import set_local_data, is_mainthread, get_actor, remove_actor


__all__ = ['is_actor', 'send', 'Actor', 'ACTOR_STATES', 'Pulsar']

ACTOR_STATES = AttributeDictionary(INITIAL=0X0, STARTING=0X1, RUN=0x2,
                                   STOPPING=0x3, CLOSE=0x4, TERMINATE=0x5)
ACTOR_STATES.DESCRIPTION = {ACTOR_STATES.INITIAL: 'initial',
                            ACTOR_STATES.STARTING: 'starting',
                            ACTOR_STATES.RUN: 'running',
                            ACTOR_STATES.STOPPING: 'stopping',
                            ACTOR_STATES.CLOSE: 'closed',
                            ACTOR_STATES.TERMINATE:'terminated'}
MINIMUM_ACTOR_TIMEOUT = 10
DEFAULT_ACTOR_TIMEOUT = 60
ACTOR_NOTIFY_PERIOD = 10
# Send messages to arbiter not before the difference of
# now and the last message time is greater than this tolerance
# times timeout. So for a timeout of 30 seconds, the messages will
# go after tolerance*30 seconds (18 secs for tolerance = 0.6).
ACTOR_TIMEOUT_TOLERANCE = 0.6
# Timeout of when joining an actor which has been terminated
ACTOR_TERMINATE_TIMEOUT = 2
ACTOR_STOPPING_LOOPS = 5
EMPTY_TUPLE = ()
EMPTY_DICT = {}


def is_actor(obj):
    return isinstance(obj, Actor)


def send(target, action, *args, **params):
    '''Send an *message* to *target* to perform a given *action*.

:parameter target: the :class:`Actor` id or an :class:`ActorProxy` or name of
    the target actor which will receive the message.
:parameter action: the name of the :ref:`remote command <api-remote_commands>`
    to perform in the *target* :class:`Actor`.
:parameter args: positional arguments to pass to the
    :ref:`remote command <api-remote_commands>` *action*.
:parameter params: dictionary of parameters to pass to
    :ref:`remote command <api-remote_commands>` *action*.
:rtype: an :class:`ActorMessage` which is a :class:`Deferred` and therefore
    can be used to attach callbacks.

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
        

class Actor(Pulsar):
    '''The base class for concurrent programming in pulsar. In computer science,
the **Actor model** is a mathematical model of concurrent computation that
treats *actors* as the universal primitives of computation.
In response to a message that it receives, an actor can make local decisions,
create more actors, send more messages, and determine how to respond to
the next message received.

Pulsar actors are slightly different from the general theory. They cannot
create other actors, unless they are of special kind.

The current implementation allows for actors to perform specific tasks such
as listening to a socket, acting as http server, consuming
a task queue and so forth.

To spawn a new actor::

    >>> from pulsar import Actor, spawn
    >>> a = spawn(Actor)
    >>> a.is_alive()
    True

Here ``a`` is actually a reference to the remote actor.

.. attribute:: actor_functions

    dictionary of remote functions exposed by the actor. This
    dictionary is filled by the :class:`pulsar.ActorMetaClass` during class
    construction.

.. attribute:: age

    The age of actor, used to access how long the actor has been created.

.. attribute:: ioqueue

    An optional distributed queue used as IO. Check server configuration with
    :ref:`io queue <configuration-ioqueue>` for information.

    Default ``None``.

.. attribute:: cpubound

    Indicates if the :class:`Actor` is a CPU-bound worker or a I/O-bound one.
    CPU-bound actors have a sperate event loop for handling I/O events.

.. attribute:: ioloop

    An instance of :class:`IOLoop`, the input/output event loop
    driving the actor. Some actors may share the ioloop with other actors
    depending on their concurrency implementation.
    
.. attribute:: requestloop

    The :class:`IOLoop` to listen for requests. This is different from
    :attr:`ioloop` for CPU-bound actors.

.. attribute:: request_processed

    The total number of requests served by the actor

.. attribute:: concurrent_requests

    The current number of concurrent requests the actor is serving.
    Depending on the actor type, this number can be very high or max 1
    (CPU bound actors).

.. attribute:: loglevel

    String indicating the logging level for the actor.
'''
    exit_code = None
    mailbox = None
    stopping_start = None
    stopping_end = None
    
    def __init__(self, impl):
        self.__impl = impl
        on_task = impl.params.pop('on_task', None)
        on_event = impl.params.pop('on_event', None)
        ioqueue = impl.params.pop('ioqueue', None)
        if not self.is_arbiter():
            if on_task:
                self.on_task = lambda : on_task(self)
            if on_event:
                self.on_event = lambda fd, event: on_event(self, fd, event)
        else:
            ioqueue = None
        self.request_processed = 0
        self.last_notified = 0
        self.concurrent_requests = 0
        self.state = ACTOR_STATES.INITIAL
        self.ioqueue = ioqueue
        self.linked_actors = impl.params.pop('linked_actors', {})
        self.monitors = impl.params.pop('monitors', {})
        self.arbiter = impl.params.pop('arbiter', None)
        self.monitor = impl.params.pop('monitor', None)
        self.proxy_mailboxes = {}
        self.params = AttributeDictionary(self.on_init(**impl.params))
        del impl.params

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
    def commands_set(self):
        return self.__impl.commands_set
    
    @property
    def proxy(self):
        '''Instance of a :class:`ActorProxy` holding a reference
to the actor. The proxy is a lightweight representation of the actor
which can be shared across different processes (i.e. it is pickable).'''
        return ActorProxy(self)

    @property
    def address(self):
        return self.mailbox.address

    @property
    def ioloop(self):
        '''The :class:`IOLoop` for I/O.'''
        return self.mailbox.ioloop

    @property
    def cpubound(self):
        if self.impl.kind != 'monitor':
            return self.ioqueue is not None
        else:
            return False

    @property
    def info_state(self):
        '''Current state description. One of ``initial``, ``running``,
 ``stopping``, ``closed`` and ``terminated``.'''
        return ACTOR_STATES.DESCRIPTION[self.state]

    ############################################################################
    ##    HIGH LEVEL API METHODS
    ############################################################################
    def command(self, action):
        '''Fetch the pulsar command for *action*.'''
        return get_command(action, self.commands_set)

    def send(self, target, action, *args, **params):
        '''Send a message to *target* to perform *action* with given
parameters *params*. It return a :class:`ActorMessage`.'''
        if not isinstance(target, ActorProxy):
            target = get_proxy(self.get_actor(target))
        return target.receive_from(self, action, *args, **params)

    def put(self, request):
        '''Put a *request* into the :attr:`ioqueue` if available.'''
        if self.ioqueue:
            self.logger.debug('Putting %s into IO queue', request)
            self.ioqueue.put(('request', request))
        else:
            self.logger.error("Trying to put a request on task queue,\
 but there isn't one!")
            
    ###############################################################  STATES
    def running(self):
        '''``True`` if actor is running.'''
        return self.state == ACTOR_STATES.RUN

    def started(self):
        '''``True`` if actor has started. It does not necessarily
mean it is running.'''
        return self.state >= ACTOR_STATES.RUN

    def stopping(self):
        '''``True`` if actor is stopping.'''
        return self.state == ACTOR_STATES.STOPPING

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

    def isprocess(self):
        '''boolean indicating if this is an actor on a child process.'''
        return self.impl.kind == 'process'

    def ready(self):
        return self.arbiter.aid in self.linked_actors

    def can_poll(self):
        '''Check if the actor can poll requests. This is used by CPU-bound
 actors only.'''
        m = self.cfg.backlog
        return self.concurrent_requests < m if m else True

    def __reduce__(self):
        raise pickle.PicklingError('{0} - Cannot pickle Actor instances'\
                                   .format(self))

    ############################################################################
    ##    EVENT HANDLING
    ############################################################################
    def handle_fd_event(self, fd, event):
        '''This function should be used when registering events
 on file descriptors. It is called by the :attr:`requestloop` when an event
 occurs on file descriptor *fd*.'''
        self.request_processed += 1
        self.concurrent_requests += 1
        msg = safe_async(self.on_event, args=(fd, event))
        msg.addBoth(self.end_event)

    def end_event(self, result):
        self.concurrent_requests -= 1
        log_failure(result)
        max_requests = self.cfg.max_requests
        should_stop = max_requests and self.request_processed >= max_requests
        if should_stop:
            self.logger.info("Shutting down %s. Max requests reached.", self)
            self.stop()

    ############################################################################
    ##    HOOKS
    ############################################################################
    def on_init(self, **kwargs):
        '''The :ref:`actor callback <actor-callbacks>` run once at the
end of initialisation (after forking).'''
        return kwargs

    def on_start(self):
        '''The :ref:`actor callback <actor-callbacks>` run once just before
the actor starts (after forking) its event loop. Every attribute is available,
therefore this is a chance to setup to perform custom initialization
before the actor starts running.'''
        pass

    def on_task(self):
        '''The :ref:`actor callback <actor-callbacks>` executed at each
iteration of the :attr:`Actor.ioloop`.'''
        pass

    def on_event(self, fd, event):
        '''handle and event on a filedescriptor *fd*.'''
        pass

    def on_stop(self):
        '''The :ref:`actor callback <actor-callbacks>` run once just before
 the actor stops running.'''
        pass

    def on_exit(self):
        '''The :ref:`actor callback <actor-callbacks>` run once when the actor
 has stopped running, just before it vanish in the garbage collector.'''
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

    def start(self):
        '''Called after forking to start the actor's life. This is where
logging is configured, the :attr:`Actor.mailbox` is registered and the
:attr:`Actor.ioloop` is initialised and started.'''
        if self.state == ACTOR_STATES.INITIAL:
            self.state = ACTOR_STATES.STARTING
            self.configure_logging()
            self._run()

    ############################################################################
    ##    INTERNALS
    ############################################################################
    def proxy_mailbox(self, address):
        m = self.proxy_mailboxes.get(address)
        if not m:
            m = mailbox(address=address)
            self.proxy_mailboxes[address] = m
        return m

    ############################################################################
    # STOPPING
    ############################################################################
    def stop(self, force=False, exit_code=None):
        '''Stop the actor by stopping its :attr:`Actor.requestloop`
and closing its :attr:`Actor.mailbox`. Once everything is closed
properly this actor will go out of scope.'''
        if force or self.state in (ACTOR_STATES.STARTING, ACTOR_STATES.RUN):
            self.stopping_start = time()
            self.state = ACTOR_STATES.STOPPING
            self.exit_code = exit_code
            try:
                res = self.on_stop()
            except:
                self.logger.error('Unhandle error while stopping',
                                  exc_info=True)
                res = None
            return res.addBoth(self.exit) if is_async(res) else self.exit()
    
    def exit(self, result=None):
        '''Exit from the :class:`Actor` domain.'''
        if not self.stopped():
            self.mailbox.close()
            # we don't want monitors to stop the request loop
            if not self.is_monitor():
                self.requestloop.stop()
            self.state = ACTOR_STATES.CLOSE
            self.stopping_end = time()
            self.logger.debug('%s exited', self)
            remove_actor(self)
            self.on_exit()

    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        if aid == self.aid:
            return self
        elif aid == 'arbiter':
            return self.arbiter or self
        elif self.arbiter and aid == self.arbiter.aid:
            return self.arbiter
        elif self.monitor and aid == self.monitor.aid:
            return self.monitor
        elif aid in self.linked_actors:
            return self.linked_actors[aid]
        else:
            return self.monitors.get(aid)

    def __call__(self):
        '''Called in the main eventloop to perform the following
actions:

* it notifies linked actors if required (arbiter only for now)
* it executes the :meth:`Actor.on_task` callback.
'''
        if self.running() and self.arbiter.aid in self.linked_actors:
            nt = time()
            if hasattr(self, 'last_notified'):
                timeout = self.cfg.timeout
                tole = min(ACTOR_TIMEOUT_TOLERANCE*timeout, ACTOR_NOTIFY_PERIOD)
                if nt - self.last_notified < tole:
                    nt = None
            if nt:
                self.last_notified = nt
                info = self.info()
                self.send('arbiter', 'notify', info)
            self.on_task()

    def info(self):
        '''return A dictionary of information related to the actor
status and performance.'''
        if not self.started():
            return
        isp = self.isprocess()
        requestloop  = self.requestloop
        actor = {'name': self.name,
                 'state': self.info_state,
                 'actor_id': self.aid,
                 'uptime': time() - requestloop._started,
                 'thread_id': self.tid,
                 'process_id': self.pid,
                 'is_process': isp,
                 'internal_connections': self.mailbox.active_connections,
                 'age': self.impl.age}
        events = {'request processed': self.request_processed,
                  'callbacks': len(self.ioloop._callbacks),
                  'io_loops': self.ioloop.num_loops}
        if self.cpubound:
            events['request_loops'] = requestloop.num_loops
        data = {'actor': actor, 'events': events}
        if isp:
            data['system'] = system.system_info(self.pid)
        return self.on_info(data)

    ############################################################################
    #    INTERNALS
    ############################################################################
    def link_actor(self, proxy, address=None):
        '''Add the *proxy* to the :attr:`` dictionary.
if *proxy* is not a class:`ActorProxy` instance raise an exception.'''
        if address:
            proxy.address = address
        if not proxy.address:
            raise ValueError('Linking with a actor without address')
        self.linked_actors[proxy.aid] = proxy
        if proxy.aid == self.arbiter.aid:
            self.requestloop.ready = True
        # If the proxy is the actor monitor, add the arbiter
        # if the monitor is not the arbiter itself.
        # This last check is crucial in order to recursive call
        # causing stack overflow!
        if self.monitor == proxy and self.monitor != self.arbiter:
            self.link_actor(self.arbiter, address)
        return proxy

    def _init_runner(self):
        '''Initialise the runner.'''
        if not self.isprocess():
            return
        random.seed()
        proc_name = "%s-%s" % (self.cfg.proc_name, self)
        if system.set_proctitle(proc_name):
            self.logger.debug('Set process title to %s', proc_name)
        #system.set_owner_process(cfg.uid, cfg.gid)
        if is_mainthread():
            self.logger.debug('Installing signals')
            # The default signal handling function in signal
            sfun = getattr(self, 'signal', None)
            for name in system.ALL_SIGNALS:
                func = sfun
                if not func:
                    func = getattr(self,'handle_{0}'.format(name.lower()),None)
                if func:
                    sig = getattr(signal,'SIG{0}'.format(name))
                    try:
                        signal.signal(sig, func)
                    except ValueError:
                        break
            #atexit.register(self.stop)

    def _on_run(self):
        '''Internal function called at the start of the actor. It builds the
event loop which will consume events on file descriptors.'''
        # Inject self as the actor of this thread
        ioq = self.ioqueue
        self.requestloop = IOLoop(io=IOQueue(ioq, self) if ioq else None,
                                  poll_timeout=self.params.poll_timeout,
                                  logger=self.logger,
                                  ready=not self.cpubound)
        # If CPU bound add the request handler to the request loop
        if self.cpubound:
            self.requestloop.add_handler('request',
                                          self.handle_fd_event,
                                          self.requestloop.READ)
        self._init_runner()
        self.mailbox = mailbox(self)
        set_local_data(self)
        setid(self)
        self.state = ACTOR_STATES.RUN
        self.on_start()
        self.logger.info('address %s', self.mailbox.address)

    def _run(self):
        '''The run implementation which must be done by a derived class.'''
        self._on_run()
        try:
            self.requestloop.start()
        finally:
            self.stop()
    