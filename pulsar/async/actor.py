import sys
import os
import signal
from time import time
import random
from multiprocessing import current_process
from threading import current_thread


from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   ActorAlreadyStarted, LogSelf, LogginMixin, system
from pulsar.utils.py2py3 import iteritems, itervalues, pickle


from .eventloop import IOLoop
from .proxy import ActorProxy, ActorMessage, process_message,\
                    DEFAULT_MESSAGE_CHANNEL
from .defer import make_async, raise_failure, async_pair
from .mailbox import IOQueue


__all__ = ['is_actor',
           'Actor',
           'ActorMetaClass',
           'ActorBase',
           'MAIN_THREAD']


EMPTY_TUPLE = ()
EMPTY_DICT = {}


def is_actor(obj):
    return isinstance(obj,Actor)

    
MAIN_THREAD = current_thread()


class ActorMetaClass(type):
    '''The actor metaclass performs a little amount of magic
by collecting functions prefixed with ``actor_`` and placing them into
the :class:`Actor.actor_functions` dictionary.
These are the remote functions exposed by the actor.

Each remote function must at least accept one argument which is represented
by the remote actor calling the function. For example::

    import pulsar
    
    class MyArbiter(pulsar.Arbiter):
    
        def actor_dosomething(self, caller, ...):
            ...
            
'''
    def __new__(cls, name, bases, attrs):
        make = super(ActorMetaClass, cls).__new__
        fprefix = 'actor_'
        attrib  = '{0}functions'.format(fprefix)
        rattrib = 'remotes'
        cont = {}
        remotes = {}
        for base in bases[::-1]:
            if hasattr(base,attrib) and hasattr(base,rattrib):
                cont.update(getattr(base,attrib))
                remotes.update(getattr(base,rattrib))
        
        docs = os.environ.get('BUILDING-PULSAR-DOCS') == 'yes'
        for key, method in list(attrs.items()):
            if hasattr(method,'__call__') and key.startswith(fprefix):
                if docs:
                    method = attrs[key]
                else:
                    method = attrs.pop(key)
                meth_name = key[len(fprefix):]
                ack = getattr(method,'ack',True)
                cont[meth_name] = method
                remotes[meth_name] = ack
            for base in bases[::-1]:
                if hasattr(base, attrib):
                    rbase = getattr(base,attrib)
                    for key,method in rbase.items():
                        if not key in cont:
                            cont[key] = method
                        
        attrs.update({attrib: cont,
                      rattrib: remotes})
        return make(cls, name, bases, attrs)

    
ActorBase = ActorMetaClass('BaseActor',(object,),{})


class Actor(ActorBase,LogginMixin):
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
    
.. attribute:: ioloop

    An instance of :class:`IOLoop`, the input/output event loop
    driving the actor. Some actors may share the ioloop with other actors
    depending on their concurrency implementation.

.. attribute:: loglevel

    String indicating the logging level for the actor.
'''
    INITIAL = 0X0
    RUN = 0x1
    STOPPING = 0x2
    CLOSE = 0x3
    TERMINATE = 0x4
    STATE_DESCRIPTION = {0x0:'initial',
                         0x1:'running',
                         0x2:'stopping',
                         0x3:'closed',
                         0x4:'terminated'}
    DEFAULT_IMPLEMENTATION = 'process'
    MINIMUM_ACTOR_TIMEOUT = 10
    DEFAULT_ACTOR_TIMEOUT = 60
    # Send messages to arbiter not before the difference of
    # now and the last message time is greater than this tolerance
    # times timeout. So for a timeout of 30 seconds, the messages will
    # go after tolerance*30 seconds (18 secs for tolerance = 0.6).
    ACTOR_TIMEOUT_TOLERANCE = 0.6
    DEF_PROC_NAME = 'pulsar'
    SIG_QUEUE = None
    _name = 'actor'
    
    def __init__(self, impl, arbiter = None, monitor = None,
                 on_task = None, ioqueue = None,
                 monitors = None, name = None, params = None,
                 age = 0, pool_timeout = None, ppid = None,
                 **kwargs):
        # Call on_init
        self.__ppid = ppid
        self._impl = impl
        self._linked_actors = {}
        self.age = age
        self.nr = 0
        self._pool_timeout = pool_timeout
        self._name = name or self._name
        self.arbiter = arbiter
        self.monitor = monitor
        self._state = self.INITIAL
        if impl.loglevel is None and arbiter:
            self.loglevel = arbiter.loglevel
        else:
            self.loglevel = impl.loglevel
        self._params = params or {}
        self._monitors = monitors or {}
        actor_links = {}
        for a in itervalues(self._monitors):
            self._linked_actors[a.aid] = a
        
        # If arbiter available
        if arbiter:
            self._linked_actors[arbiter.aid] = arbiter
            self._repr = '{0} {1}'.format(self._name,self.aid)
            if on_task:
                self.on_task = on_task
            
        self.ioqueue = ioqueue
        #
        self.on_init(**kwargs)
    
    @property
    def proxy(self):
        '''Instance of a :class:`ActorProxy` holding a reference
to the actor. The proxy is a lightweight representation of the actor
which can be shared across different processes (i.e. it is pickable).'''
        return ActorProxy(self)
    
    @property
    def aid(self):
        '''Actor unique identifier'''
        return self._impl.aid
    
    @property
    def ppid(self):
        '''Parent process id.'''
        return self.__ppid
    
    @property
    def impl(self):
        '''String indicating actor concurrency implementation
("monitor", "thread", "process" or "greenlet").'''
        return self._impl.impl
    
    @property
    def timeout(self):
        '''Timeout in seconds. If ``0`` the actor has no timeout, otherwise
it will be stopped if it fails to notify itself for a period
longer that timeout.'''
        return self._impl.timeout
    
    @property
    def pid(self):
        '''Operative system process ID where the actor is running.'''
        return self.__pid
    
    @property
    def tid(self):
        '''Operative system thread name where the actor is running.'''
        return self.__tid
    
    @property
    def name(self):
        'Actor name'
        if self._name:
            return self._name
        else:
            return self._make_name()
        
    def __repr__(self):
        return self._repr
        
    @property
    def inbox(self):
        '''Messages inbox :class:`Mailbox`.'''
        return self._impl.inbox
    
    @property
    def outbox(self):
        '''Messages outbox :class:`Mailbox`.'''
        return self._impl.outbox
        
    @property
    def fullname(self):
        return self._repr
    
    @property
    def monitors(self):
        '''Dictionary of all :class:`Monitor` instances
registered with the actor. The keys are given by the monitor names rather than
their ids.'''
        return self._monitors
    
    @property
    def pool_timeout(self):
        '''Timeout in seconds for waiting for events in the eventloop.
 A small number is suitable for :class:`Actor` performing CPU-bound
 operation on the :meth:`Actor.on_task` method, a larger number is better for
 I/O bound actors.
'''
        return self.ioloop.POLL_TIMEOUT
    
    def is_arbiter(self):
        '''Return ``True`` if ``self`` is the :class:`Arbiter`.'''
        return False
    
    def __reduce__(self):
        raise pickle.PicklingError('{0} - Cannot pickle Actor instances'\
                                   .format(self))
    
    # HOOKS
    
    def on_init(self, **kwargs):
        '''The :ref:`actor callback <actor-callbacks>` run once at the
end of initialisation (after forking).'''
        pass
    
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
    
    def on_stop(self):
        '''The :ref:`actor callback <actor-callbacks>` run once just before
 the actor stops running.'''
        pass
    
    def on_exit(self):
        '''The :ref:`actor callback <actor-callbacks>` run once when the actor
 is exting the framework (and vanish in the garbage collector).'''
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
    
    @property
    def state(self):
        '''Current state description. One of ``initial``, ``running``,
 ``stopping``, ``closed`` and ``terminated``.'''
        return self.STATE_DESCRIPTION[self._state]
    
    def running(self):
        '''``True`` if actor is running.'''
        return self.ioloop.running()
        #return self._state == self.RUN
    
    def started(self):
        '''``True`` if actor has started. It does not necessarily
mean it is running.'''
        return self._state >= self.RUN
    
    def stopping(self):
        '''``True`` if actor is stopping.'''
        return self._state == self.STOPPING
    
    def closed(self):
        '''``True`` if actor has exited in an clean fashion.'''
        return self._state == self.CLOSE
    
    def stopped(self):
        '''``True`` if actor has exited.'''
        return self._state >= self.CLOSE
    
    def handle_request(self, request):
        raise NotImplementedError
            
    def start(self):
        '''Called after forking to start the life of the actor. This is where
logging is configured, the :attr:`Actor.inbox` and :attr:`Actor.outbox`
are registered and the :attr:`Actor.ioloop` is initialised and started.'''
        if self._state == self.INITIAL:
            self._state = self.RUN
            self.configure_logging()
            # wrap the logger
            if self.arbiter:
                self.setlog(log = LogSelf(self,self.log))
            self.log.info('Starting')
            # ADD SELF TO THE EVENT LOOP TASKS
            self.ioloop = self.get_eventloop()
            self.ioloop.add_loop_task(self)
            if self.outbox:
                self.outbox.register(self,inbox=False)
            if self.inbox:
                self.inbox.register(self)
            self.__tid = current_thread().ident
            self.__pid = os.getpid()
            self.on_start()
            self._run()
    
    def get_eventloop(self):
        ioq = self.ioqueue
        ioimpl = IOQueue(ioq) if ioq else None
        ioloop = IOLoop(io = ioimpl,
                        pool_timeout = self._pool_timeout,
                        logger = self.log,
                        name = self.name)
        
        #add the ioqueue handler if needed
        if ioq:
            ioloop.add_handler('request',
                        lambda fd, request : self.handle_request(request),
                        ioloop.READ)
        self._init_runner()
        return ioloop
    
    def message_arrived(self, message):
        '''A new :class:`ActorMessage` has arrived in the :attr:`Actor.inbox`.
Here we check the sender and the receiver (it may be not ``self`` if
``self`` is the  :class:`Arbiter`) and perform the message action.
If the message needs acknowledgment, send the result back.'''
        sender = self.get_actor(message.sender)
        receiver = self.get_actor(message.receiver)
        if not sender or not receiver:
            if not sender:
                self.log.warn('message "{0}" from an unknown actor "{1}"'\
                              .format(message,message.sender))
            if not receiver:
                self.log.warn('message "{0}" for an unknown actor "{1}"'\
                              .format(message,message.receiver))
        else:
            return process_message(receiver,sender,message)
        
    def handle_message(self, sender, message):
        '''Handle a *message* from a *sender*.'''
        pass
        
    def put(self, request):
        '''Put a request into the actor :attr:`ioqueue` if available.'''
        if self.ioqueue:
            self.log.debug('Put {0} on IO queue'.format(request))
            self.ioqueue.put(('request',request))
        else:
            self.log.error("Trying to put a request on task queue,\
 but there isn't one!")        
    def get(self, key, default = None):
        try:
            return self._params[key]
        except KeyError:
            return default
        
    def set(self, key, value):
        self._params[key] = value
        
    def run_on_arbiter(self, callable):
        '''Run a *callable* in the arbiter event loop.

:parameter callable: a pickable, therefore it must be a pickable callable object
    or a function.
:rtype: a tuple of :class:`Deferred` instances'''
        res = self.arbiter.send(self,'run',callable)
        return async_pair(res)
        
    ############################################################################
    # STOPPING
    ############################################################################
    
    def stop(self):
        '''Stop the actor by stopping its :attr:`Actor.ioloop`
and closing its :attr:`Actor.inbox` orderly. Once everything is closed
properly this actor will go out of scope.'''
        if self._state == self.RUN:
            self._state = self.STOPPING
            self.log.info('stopping')
            stp = self.on_stop()
            if not stp:
                stp = self.ioloop.stop()
            make_async(stp).add_callback(lambda r : self.close())\
                           .add_callback(raise_failure)
        
    def close(self):
        if self.stopping():
            self._state = self.CLOSE
            self.on_exit()
            if not self.ioloop.remove_loop_task(self):
                self.log.warn('"{0}" could not be removed from\
 eventloop'.format(self.fullname))
            if self.inbox:
                self.inbox.close()
            if self.outbox:
                self.outbox.close()
            self.log.info('exited')
            
    # LOW LEVEL API
    
    def _make_name(self):
        return '{0}({1})'.format(self.class_code,self.aid[:8])
    
    def linked_actors(self):
        '''Iterator over linked-actor proxies (no moitors are yielded).'''
        for a in itervalues(self._linked_actors):
            if isinstance(a,ActorProxy):
                yield a
            
    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        if aid == self.aid:
            return self
        elif aid in self._linked_actors:
            return self._linked_actors[aid]
        else:
            return self._monitors.get(aid)

    def __call__(self):
        '''Called in the main eventloop to perform the following
actions:

* it notifies linked actors if required (arbiter only for now)
* it executes the :meth:`Actor.on_task` callback.
'''
        # If this is not a monitor
        # we notify to the arbiter we are still alive
        # self.log.debug(self.state)
        if not self.running():
            return
        
        if self.impl != 'monitor':
            nt = time()
            if hasattr(self,'last_notified'):
                if not self.timeout:
                    tole = self.DEFAULT_ACTOR_TIMEOUT
                else:
                    tole = self.ACTOR_TIMEOUT_TOLERANCE*self.timeout
                if nt - self.last_notified < tole:
                    nt = None
            if nt:
                self.last_notified = nt
                info = self.info(True)
                info['last_notified'] = nt
                self.arbiter.send(self,'notify',info)

        self.on_task()
    
    def current_thread(self):
        '''Return the current thread'''
        return current_thread()
    
    def current_process(self):
        return current_process()
    
    def isprocess(self):
        '''boolean indicating if this is an actor on a child process.'''
        return self.impl == 'process'
    
    def info(self, full = False):
        '''return A dictionary of information related to the actor
status and performance.'''
        data = {'name':self.name,
                'aid':self.aid[:8],
                'pid':self.pid,
                'ppid':self.ppid,
                'thread':self.tid,
                'process':self.pid,
                'isprocess':self.isprocess(),
                'age':self.age}
        ioloop = self.ioloop
        if ioloop:
            data.update({'uptime': time() - ioloop._started,
                         'event_loops': ioloop.num_loops})
        return self.on_info(data)
        
    ############################################################################
    # BUILT IN REMOTE FUNCTIONS
    ############################################################################
    
    def action_message(self, request):
        # Do nothing for now
        return
        msg = request.msg
        name = request.name or DEFAULT_MESSAGE_CHANNEL
        if name not in self.channels:
            self.channels[name] = []
        ch = self.channels[name]
        ch.append(request)
    
    def actor_callback(self, caller, rid, result):
        '''Actor :ref:`remote function <remote-functions>` which sends
the a results back to an actor which previously accessed another remote
function. Essentially this is the return statement in the pulsar concurrent
framework'''
        ActorMessage.actor_callback(rid,result)
    actor_callback.ack = False
    
    def actor_stop(self, caller):
        '''Actor :ref:`remote function <remote-functions>` which stops
the actor by invoking the local :meth:`Actor.stop` method. This
method only works if self is not the arbiter.'''
        if self.arbiter:
            return self.stop()
        
    def actor_notify(self, caller, info):
        '''Actor :ref:`remote function <remote-functions>` for notifying
the actor information to the caller.'''
        caller.info = info
    actor_notify.ack = False
    
    def actor_info(self, caller, full = False):
        '''Get server Info and send it back.'''
        return self.info(full)
    
    def actor_ping(self, caller):
        return 'pong'
    
    def actor_run(self, caller, callable):
        '''Execute a callable in the actor process domain. The callable
must accept one positional argument, the :class:`Actor` executing the
function.'''
        return callable(self)
    
    def actor_kill_actor(self, caller, aid):
        return self.arbiter.kill_actor(aid)

    ############################################################################
    #    INTERNALS
    ############################################################################
    
    def _init_runner(self):
        '''Initialise the runner.'''
        if not self.isprocess():
            return
        
        random.seed()
        proc_name = self.DEF_PROC_NAME
        cfg = self.get('cfg')
        if cfg:
            proc_name = cfg.proc_name or cfg.default_proc_name or proc_name
        proc_name = "{0} - {1}".format(proc_name,self)
        if system.set_proctitle(proc_name):
            self.log.debug('Set process title to {0}'.format(proc_name))
        #system.set_owner_process(cfg.uid, cfg.gid)
        current = self.current_thread()
        if current == MAIN_THREAD:
            self.log.debug('Installing signals')
            # The default signal handling function in signal
            sfun = getattr(self,'signal',None)
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
    
    def _run(self):
        '''The run implementation which must be done by a derived class.'''
        try:
            self.ioloop.start()
        except:
            self.log.critical("Unhandled exception exiting.", exc_info = True)
        finally:
            self.stop()

                    
    def _signal_stop(self, sig, frame):
        signame = system.SIG_NAMES.get(sig,None)
        self.log.warning('got signal {0}. Exiting.'.format(signame))
        self.stop()
            
    handle_int  = _signal_stop
    handle_quit = _signal_stop
    handle_term = _signal_stop