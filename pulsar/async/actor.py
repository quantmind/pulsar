import sys
from time import time

from pulsar import HaltServer, CommandError, system
from pulsar.utils.pep import pickle
from pulsar.utils.log import LogginMixin

from .eventloop import setid
from .defer import async, EventHandler, Failure
from .threads import ThreadPool
from .proxy import ActorProxy, ActorProxyMonitor, ActorIdentity
from .mailbox import command_in_context
from .access import get_actor
from .consts import *


__all__ = ['is_actor', 'send', 'Actor', 'ACTOR_STATES', 'Pulsar']


def is_actor(obj):
    return isinstance(obj, Actor)

def send(target, action, *args, **params):
    '''Send a :ref:`message <api-remote_commands>` to ``target`` to perform
a given ``action``. The actor sending the message is obtained via the
:func:`get_actor` function.

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
    (i.e. it is picklable).

.. attribute:: state

    The actor :ref:`numeric state <actor-states>`.
    
.. attribute:: thread_pool

    A :class:`ThreadPool` associated with this :class:`Actor`. This attribute
    is ``None`` unless one is created via the :meth:`create_thread_pool`
    method.

.. attribute:: params

    A :class:`pulsar.utils.structures.AttributeDictionary` which contains
    parameters which are passed to actors spawned by this actor.
    
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

    The :class:`TimedCall` for the next
    :ref:`actor periodic task <actor-periodic-task>`.
 
**PUBLIC METHODS**
'''
    ONE_TIME_EVENTS = ('start', 'stopping', 'stop')
    exit_code = None
    mailbox = None
    signal_queue = None
    next_periodic_task = None
    
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
        self.extra = {}
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
            self.__impl.before_start(self)
            self._started = time() 
            self.configure_logging()
            self.__impl.setup_event_loop(self)
            self.state = ACTOR_STATES.STARTING
            self._run()

    @async()
    def send(self, target, action, *args, **kwargs):
        '''Send a message to ``target`` to perform ``action`` with given
positional ``args`` and key-valued ``kwargs``.
Always return a :class:`Deferred`.'''
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
            #if not mailbox.closed:
            return mailbox.request(action, self, target, args, kwargs)
        else:
            raise CommandError('Cannot execute "%s" in %s. Unknown actor %s.'
                               % (action, self, target))
    
    def spawn(self, **params):
        raise RuntimeError('Cannot spawn an actor from an actor.')
    
    def stop(self, exc=None):
        '''Gracefully stop the :class:`Actor`.
        
        Implemented by the :meth:`Concurrency.stop` method of the :attr:`impl`
        attribute.'''
        return self.__impl.stop(self, exc)
    
    def create_thread_pool(self, workers=None):
        '''Create a :class:`ThreadPool` for this :class:`Actor`
if not already present.

:param workers: number of threads to use in the :class:`ThreadPool`. If not
    supplied, the value in the :ref:`setting-thread_workers` setting is used.
:return: a :class:`ThreadPool`.
'''
        if self._thread_pool is None:
            workers = workers or self.cfg.thread_workers
            self._thread_pool = ThreadPool(self, threads=workers)
        return self._thread_pool
    
    def close_thread_pool(self):
        '''Close the :attr:`thread_pool`.'''
        if self._thread_pool:
            timeout = 0.5*ACTOR_ACTION_TIMEOUT
            d = self._thread_pool.close(timeout)
            self.logger.debug('Waiting for thread pool to exit')
            d.add_errback(lambda r: self._thread_pool.terminate(timeout))
            self._thread_pool.join()
            self.logger.debug('Thread pool %s' % self._thread_pool.status)
            self._thread_pool = None
            
    ###############################################################  STATES
    def is_running(self):
        '''``True`` if actor is running, that is when the :attr:`state`
is equal to :ref:`ACTOR_STATES.RUN <actor-states>` and the event loop is
running.'''
        return self.state == ACTOR_STATES.RUN and self.event_loop.is_running()
    
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
        return self.__impl.is_arbiter()

    def is_monitor(self):
        '''Return ``True`` if ``self`` is a :class:`Monitor`.'''
        return False

    def is_process(self):
        '''boolean indicating if this is an actor on a child process.'''
        return self.__impl.is_process()

    def __reduce__(self):
        raise pickle.PicklingError('{0} - Cannot pickle Actor instances'\
                                   .format(self))
    
    ############################################################################
    #    INTERNALS
    ############################################################################   
    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        if aid == self.aid:
            return self
        elif aid == 'monitor':
            return self.monitor or self

    def info(self):
        '''Return a nested dictionary of information related to the actor
status and performance. The dictionary contains the following entries:

* ``actor`` a dictionary containing information regarding the type of actor
  and its status.
* ``events`` a dictionary of information about the event loop running the actor.
* ``extra`` the :attr:`extra` attribute (which you can use to add stuff).
* ``system`` system info.

This method is invoked when you run the :ref:`info command <actor_info_command>`
from another actor.'''
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
        data = {'actor': actor,
                'events': events,
                'extra': self.extra}
        if isp:
            data['system'] = system.system_info(self.pid)
        return data
    
    def _run(self, initial=True):
        exc = None
        if initial:
            try:
                self.cfg.when_ready(self)
            except Exception:
                pass
        try:
            exc = self.__impl.run_actor(self)
        except (Exception, HaltServer):
            exc = Failure(sys.exc_info())
        except:
            exc = Failure(sys.exc_info())
            exc.mute()
            raise
        finally:
            if exc != -1:
                self.stop(exc)
            
        