import sys
import os
from time import time
import pickle
from inspect import isgenerator
from threading import current_thread

from pulsar import HaltServer, CommandError, MonitorStarted, system
from pulsar.utils.log import WritelnDecorator

from .futures import in_loop, async, add_errback
from .events import EventHandler
from .threads import get_executor
from .proxy import ActorProxy, ActorProxyMonitor, ActorIdentity
from .mailbox import command_in_context
from .access import get_actor
from .cov import Coverage
from .consts import *


__all__ = ['is_actor', 'send', 'Actor', 'ACTOR_STATES', 'get_stream']


def is_actor(obj):
    return isinstance(obj, Actor)


def get_stream(cfg):
    '''Obtain the python stream handler given a config dictionary.
    '''
    stream = sys.stderr
    return WritelnDecorator(stream)


def send(target, action, *args, **params):
    '''Send a :ref:`message <api-remote_commands>` to ``target``

    The message is to perform a given ``action``. The actor sending the
    message is obtained via the :func:`get_actor` function.

    :parameter target: the :class:`Actor` id or an :class:`.ActorProxy` or
        name of the target actor which will receive the message.
    :parameter action: the :ref:`remote command <api-remote_commands>`
        to perform in the ``target`` :class:`Actor`.
    :parameter args: positional arguments to pass to the
        :ref:`remote command <api-remote_commands>` ``action``.
    :parameter params: dictionary of parameters to pass to
        :ref:`remote command <api-remote_commands>` ``action``.
    :return: an :class:`~asyncio.Future` if the action acknowledge the
        caller or `None`.

    Typical example::

        >>> r = send(p,'ping')
        >>> r.result()
        'pong'
    '''
    return get_actor().send(target, action, *args, **params)


class Actor(EventHandler, ActorIdentity, Coverage):
    '''The base class for parallel execution in pulsar.

    In computer science, the **Actor model** is a mathematical model
    of concurrent computation that treats *actors* as the universal primitives
    of computation.
    In response to a message that it receives, an actor can make local
    decisions, create more actors, send more messages, and determine how
    to respond to the next message received.

    The current implementation allows for actors to perform specific tasks
    such as listening to a socket, acting as http server, consuming
    a task queue and so forth.

    To spawn a new actor::

        >>> from pulsar import spawn
        >>> a = spawn()
        >>> a.is_alive()
        True

    Here ``a`` is actually a reference to the remote actor, it is
    an :class:`.ActorProxy`.

    **ATTRIBUTES**

    .. attribute:: name

        The name of this :class:`Actor`.

    .. attribute:: aid

        Unique ID for this :class:`Actor`.

    .. attribute:: impl

        The :class:`.Concurrency` implementation for this :class:`Actor`.

    .. attribute:: _loop

        An :ref:`event loop <asyncio-event-loop>` which listen
        for input/output events on sockets or socket-like objects.
        It is the driver of the :class:`Actor`.
        If the :attr:`_loop` stops, the :class:`Actor` stops
        running and goes out of scope.

    .. attribute:: mailbox

        Used to send and receive :ref:`actor messages <tutorials-messages>`.

    .. attribute:: address

        The socket address for this :attr:`Actor.mailbox`.

    .. attribute:: proxy

        Instance of a :class:`.ActorProxy` holding a reference
        to this :class:`Actor`. The proxy is a lightweight representation
        of the actor which can be shared across different processes
        (i.e. it is picklable).

    .. attribute:: state

        The actor :ref:`numeric state <actor-states>`.

    .. attribute:: extra

        A dictionary which can be populated with extra parameters useful
        for other actors. This dictionary is included in the dictionary
        returned by the :meth:`info` method.
        Check the :ref:`info command <actor_info_command>` for how to obtain
        information about an actor.

    .. attribute:: info_state

        Current state description string. One of ``initial``, ``running``,
        ``stopping``, ``closed`` and ``terminated``.

    .. attribute:: next_periodic_task

        The :class:`asyncio.Handle` for the next
        :ref:`actor periodic task <actor-periodic-task>`.

    .. attribute:: stream

        A ``stream`` handler to write information messages without using
        the :attr:`~.AsyncObject.logger`.
    '''
    ONE_TIME_EVENTS = ('start', 'stopping', 'stop')
    MANY_TIMES_EVENTS = ('on_info', 'on_params')
    exit_code = None
    mailbox = None
    monitor = None
    next_periodic_task = None

    def __init__(self, impl):
        EventHandler.__init__(self)
        self.state = ACTOR_STATES.INITIAL
        self.__impl = impl
        for name in self.events:
            hook = impl.params.pop(name, None)
            if hook:
                self.bind_event(name, hook)
        for name, value in impl.params.items():
            setattr(self, name, value)
        self.servers = {}
        self.extra = {}
        self.stream = get_stream(self.cfg)
        del impl.params
        self.tid = current_thread().ident
        self.pid = os.getpid()
        try:
            self.cfg.post_fork(self)
        except Exception:
            pass
        impl.setup_event_loop(self)

    def __repr__(self):
        return self.impl.unique_name

    def __str__(self):
        return self.__repr__()

    # ############################################################# PROPERTIES
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
    def _loop(self):
        return self.mailbox._loop

    @property
    def info_state(self):
        return ACTOR_STATES.DESCRIPTION[self.state]

    def executor(self):
        '''An executor for this actor

        Obtained from the :attr:`_loop` attribute
        '''
        return get_executor(self._loop)

    #######################################################################
    #    HIGH LEVEL API METHODS
    #######################################################################
    def start(self):
        '''Called after forking to start the actor's life.

        This is where logging is configured, the :attr:`mailbox` is
        registered and the :attr:`_loop` is initialised and
        started. Calling this method more than once does nothing.
        '''
        if self.state == ACTOR_STATES.INITIAL:
            self.__impl.before_start(self)
            self._started = self._loop.time()
            self.state = ACTOR_STATES.STARTING
            self._run()

    @in_loop
    def send(self, target, action, *args, **kwargs):
        '''Send a message to ``target`` to perform ``action`` with given
        positional ``args`` and key-valued ``kwargs``.
        Always return a :class:`asyncio.Future`.'''
        target = self.monitor if target == 'monitor' else target
        mailbox = self.mailbox
        if isinstance(target, ActorProxyMonitor):
            mailbox = target.mailbox
        else:
            actor = self.get_actor(target)
            if isinstance(actor, Actor):
                # this occur when sending a message from arbiter to monitors or
                # viceversa.
                return command_in_context(action, self, actor, args, kwargs)
            elif isinstance(actor, ActorProxyMonitor):
                mailbox = actor.mailbox
        if hasattr(mailbox, 'request'):
            # if not mailbox.closed:
            return mailbox.request(action, self, target, args, kwargs)
        else:
            raise CommandError('Cannot execute "%s" in %s. Unknown actor %s.'
                               % (action, self, target))

    def spawn(self, **params):
        raise RuntimeError('Cannot spawn an actor from an actor.')

    def stop(self, exc=None):
        '''Gracefully stop the :class:`Actor`.

        Implemented by the :meth:`.Concurrency.stop` method of the :attr:`impl`
        attribute.'''
        return self.__impl.stop(self, exc)

    def close_executor(self):
        '''Close the :meth:`executor`'''
        executor = self._loop._default_executor
        if executor:
            self.logger.debug('Waiting for executor shutdown')
            executor.shutdown()
            self._loop._default_executor = None

    # ##############################################################  STATES
    def is_running(self):
        '''``True`` if actor is running, that is when the :attr:`state`
        is equal to :ref:`ACTOR_STATES.RUN <actor-states>` and the loop is
        running.'''
        return self.state == ACTOR_STATES.RUN and self._loop.is_running()

    def started(self):
        '''``True`` if actor has started.

        It does not necessarily mean it is running.
        Its state is greater or equal :ref:`ACTOR_STATES.RUN <actor-states>`.
        '''
        return self.state >= ACTOR_STATES.RUN

    def closed(self):
        '''``True`` if actor has exited in an clean fashion.

        Its :attr:`state` is :ref:`ACTOR_STATES.CLOSE <actor-states>`.
        '''
        return self.state == ACTOR_STATES.CLOSE

    def stopped(self):
        '''``True`` if actor has exited.

        Its :attr:`state` is greater or equal to
        :ref:`ACTOR_STATES.CLOSE <actor-states>`.
        '''
        return self.state >= ACTOR_STATES.CLOSE

    def is_arbiter(self):
        '''Return ``True`` if ``self`` is the :class:`.Arbiter`.'''
        return self.__impl.is_arbiter()

    def is_monitor(self):
        '''Return ``True`` if ``self`` is a :class:`.Monitor`.'''
        return False

    def is_process(self):
        '''boolean indicating if this is an actor on a child process.'''
        return self.__impl.is_process()

    def __reduce__(self):
        raise pickle.PicklingError('{0} - Cannot pickle Actor instances'
                                   .format(self))

    #######################################################################
    #    INTERNALS
    #######################################################################
    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        if aid == self.aid:
            return self
        elif aid == 'monitor':
            return self.monitor or self

    def info(self):
        '''Return a nested dictionary of information related to the actor
        status and performance. The dictionary contains the following entries:

        * ``actor`` a dictionary containing information regarding the type of
          actor and its status.
        * ``events`` a dictionary of information about the
          :ref:`event loop <asyncio-event-loop>` running the actor.
        * ``extra`` the :attr:`extra` attribute (you can use it to add stuff).
        * ``system`` system info.

        This method is invoked when you run the
        :ref:`info command <actor_info_command>` from another actor.
        '''
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
        events = {'callbacks': len(self._loop._ready),
                  'scheduled': len(self._loop._scheduled)}
        data = {'actor': actor,
                'events': events,
                'extra': self.extra}
        if isp:
            data['system'] = system.process_info(self.pid)
        self.fire_event('on_info', info=data)
        return data

    def _run(self, initial=True):
        exc = None
        if initial:
            try:
                self.cfg.when_ready(self)
            except Exception:   # pragma    nocover
                self.logger.exception('Unhandled exception in when_ready hook')
        try:
            exc = self.__impl.run_actor(self)
        except MonitorStarted:
            return
        except (Exception, HaltServer) as exc:
            return self.stop(exc)
        except BaseException:
            pass
        self.stop()
