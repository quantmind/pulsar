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
                   ActorAlreadyStarted, LogSelf, LogginMixin, system

from .eventloop import IOLoop
from .proxy import ActorProxy, ActorMessage
from .defer import make_async, is_failure, iteritems, itervalues,\
                     pickle, safe_async, async, thread_local_data
from .mailbox import IOQueue, mailbox


__all__ = ['is_actor',
           'get_actor',
           'send',
           'Actor',
           'RemoteMethods',
           'RemoteMetaClass',
           'is_mainthread']


EMPTY_TUPLE = ()
EMPTY_DICT = {}


def is_actor(obj):
    return isinstance(obj, Actor)


def is_mainthread(thread=None):
    '''Check if thread is the main thread. If *thread* is not supplied check
the current thread'''
    thread = thread if thread is not None else current_thread() 
    return isinstance(thread, threading._MainThread)


def get_actor(value=None):
    '''Returns the actor running the current thread.'''
    return thread_local_data('actor', value=value)


def send(target, action, *args, **params):
    '''Send a message to *target* to perform a given *action*.
    
:parameter target: the :class:`Actor` id or an :class:`ActorProxy` or name of
    the target actor receiving the message.
:parameter action: the action to perform on the remote :class:`Actor`.
:parameter args: positional arguments to pass to the remote action function.
:parameter params: dictionary of parameters to pass to the remote *action*.
:rtype: an :class:`ActorMessage` which is a :class:`Deferred` and therefore
    can be used to attach callbacks.

Typical example::

    >>> a = spawn()
    >>> r = a.add_callback(lambda p: send(p,'ping'))
    >>> r.result
    'pong'
'''
    return get_actor().send(target, action, *args, **params)


class RemoteMetaClass(type):
    '''The :class:`Actor` metaclass performs a little amount of magic
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
        make = super(RemoteMetaClass, cls).__new__
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
    
RemoteMethods = RemoteMetaClass('RemoteMethods',(object,),{})

class Actor(RemoteMethods, LogginMixin):
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

.. attribute:: nr

    The total number of requests served by the actor
    
.. attribute:: concurrent_requests

    The current number of concurrent requests the actor is serving.
    Depending on the actor type, this number can be very high or max 1
    (CPU bound actors).
     
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
    ACTOR_NOTIFY_PERIOD = 10
    # Send messages to arbiter not before the difference of
    # now and the last message time is greater than this tolerance
    # times timeout. So for a timeout of 30 seconds, the messages will
    # go after tolerance*30 seconds (18 secs for tolerance = 0.6).
    ACTOR_TIMEOUT_TOLERANCE = 0.6
    DEF_PROC_NAME = 'pulsar'
    SIG_QUEUE = None
    _name = 'actor'
    
    def __init__(self, impl, arbiter=None, monitor=None,
                 on_task=None, ioqueue=None, monitors=None,
                 name=None, params=None, age=0,
                 pool_timeout=None, ppid=None, on_event=None,
                 linked_actors=None, cfg=None,
                 **kwargs):
        self.cfg = cfg
        self.__ppid = ppid
        self._impl = impl
        self.__mailbox = None
        self._linked_actors = linked_actors or {}
        self.age = age
        self.nr = 0
        self.concurrent_requests = 0
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
        if not self.is_arbiter():
            self._repr = '{0} {1}'.format(self._name,self.aid)
            if on_task:
                self.on_task = on_task
            if on_event:
                self.on_event = on_event
        self.ioqueue = ioqueue
        self.on_init(**kwargs)
        if self.cfg is None:
            self.cfg = {}
    
    def __repr__(self):
        return self._repr
    
    ############################################################### PROPERTIES
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
("monitor", "thread", "process").'''
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
        
    @property
    def mailbox(self):
        '''Messages inbox :class:`Mailbox`.'''
        return self.__mailbox
    
    @property
    def address(self):
        return self.__mailbox.address
    
    @property
    def ioloop(self):
        '''The :class:`IOLoop` for I/O.'''
        return self.mailbox.ioloop
    
    @property
    def requestloop(self):
        '''The :class:`IOLoop` to listen for requests. This is different from
 :attr:`ioloop` for CPU-bound actors.'''
        return self.__requestloop
        
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
    def cpubound(self):
        if self.impl != 'monitor':
            return self.ioqueue is not None
        else:
            return False
    
    @property
    def pool_timeout(self):
        '''Timeout in seconds for waiting for events in the eventloop.
 A small number is suitable for :class:`Actor` performing CPU-bound
 operation on the :meth:`Actor.on_task` method, a larger number is better for
 I/O bound actors.
'''
        return self.requestloop.POLL_TIMEOUT
    
    @property
    def state(self):
        '''Current state description. One of ``initial``, ``running``,
 ``stopping``, ``closed`` and ``terminated``.'''
        return self.STATE_DESCRIPTION[self._state]
    
    ############################################################################
    ##    HIGH LEVEL API METHODS
    ############################################################################
    def get(self, parameter, default=None):
        '''retrive *parameter* form this :class:`Actor`.'''
        return self._params.get(parameter, default)
        
    def set(self, parameter, value):
        '''Set *parameter* value on this :class:`Actor`.'''
        self._params[parameter] = value
        
    def send(self, target, action, *args, **params):
        '''Send a message to *target* to perform *action* with given
parameters *params*. It return a :class:`ActorMessage`.'''
        if not isinstance(target, ActorProxy):
            tg = self.get_actor(target)
        else:
            tg = target
        if not tg:
            raise ValueError('Cannot send message to {0}'.format(target))
        return tg.receive_from(self, action, *args, **params)
        
    def put(self, request):
        '''Put a *request* into the :attr:`ioqueue` if available.'''
        if self.ioqueue:
            self.log.debug('Put %s into IO queue' % request)
            self.ioqueue.put(('request',request))
        else:
            self.log.error("Trying to put a request on task queue,\
 but there isn't one!")
        
    def run_on_arbiter(self, callable):
        '''Run a *callable* in the arbiter event loop.

:parameter callable: a pickable, therefore it must be a pickable callable object
    or a function.
:rtype: a :class:`Deferred`'''
        return self.send('arbiter', 'run', callable)
    
    ###############################################################  STATES
    def running(self):
        '''``True`` if actor is running.'''
        return self._state == self.RUN
    
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
    
    def is_arbiter(self):
        '''Return ``True`` if ``self`` is the :class:`Arbiter`.'''
        return False
    
    def is_monitor(self):
        '''Return ``True`` if ``self`` is a :class:`Monitor`.'''
        return False
    
    def isprocess(self):
        '''boolean indicating if this is an actor on a child process.'''
        return self.impl == 'process'
    
    def ready(self):
        return self.arbiter.aid in self._linked_actors
    
    def can_poll(self):
        '''Check if the actor can poll requests. This is used by CPU-bound
 actors only.'''
        m = self.cfg.get('backlog', 0)
        return self.concurrent_requests < m if m else True
        
    def __reduce__(self):
        raise pickle.PicklingError('{0} - Cannot pickle Actor instances'\
                                   .format(self))
    
    ############################################################################
    ##    EVENT HANDLING
    ############################################################################
    def handle_fd_event(self, fd, event):
        '''This function should be used when registering events
 on file descriptors'''
        self.nr += 1
        self.concurrent_requests += 1
        msg = safe_async(self.on_event, args=(fd, event))
        msg.addBoth(self.end_event) 
        
    def end_event(self, result):
        self.concurrent_requests -= 1
        if is_failure(result):
            result.log(self.log)
        return result
    
    ############################################################################
    ##    HOOKS
    ############################################################################
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
    
    def on_event(self, fd, event):
        '''handle and event on a filedescriptor *fd*.'''
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
    
    def on_message(self, message):
        '''The :ref:`actor callback <actor-callbacks>` run when a new
:class:`ActorMessage` *message* has been received by the :attr:`inbox`.'''
        pass
    
    def on_message_processed(self, message, result):
        '''The :ref:`actor callback <actor-callbacks>` run when an
:class:`ActorMessage` *message* has been processed.'''
        pass
    
    ############################################################################
    ##    INTERNALS
    ############################################################################
    def start(self):
        '''Called after forking to start the actor's life. This is where
logging is configured, the :attr:`Actor.mailbox` is registered and the
:attr:`Actor.ioloop` is initialised and started.'''
        if self._state == self.INITIAL:
            ct = current_thread()
            self._state = self.RUN
            self.configure_logging()
            # wrap the logger
            if self.arbiter:
                self.setlog(log = LogSelf(self,self.log))
            self.log.info('Starting')
            # GET REQUESTS EVENT LOOP
            self.__requestloop = self._get_requestloop()
            # Initialize mailbox. It will also initialize the ioloop
            self.__mailbox = mailbox(self)
            self.__tid = ct.ident
            self.__pid = os.getpid()
            # inject the IO loop into the current thread object if this is
            # a CPU bound actor. CPU bound actors runs two thread, one for the
            # request loop and one for IO event loop. The request loop is used
            # to pass task to the actor, while the IO loop for socket
            # communication with other actors or with external networks.
            self.on_start()
            self._run()
    
    def _get_requestloop(self):
        '''Internal function called at the start of the actor. It builds the
event loop which will consume events on file descriptors.
This function is overridden by :class:`Monitor` to perform nothing.'''
        ioq = self.ioqueue
        reqloop = IOLoop(io=IOQueue(ioq, self) if ioq else None,
                         pool_timeout=self._pool_timeout,
                         logger=self.log,
                         name=self.name,
                         ready=not self.cpubound)
        # If CPU bound add the request handler to the request loop
        if self.cpubound:
            reqloop.add_handler('request',
                                self.handle_fd_event,
                                reqloop.READ)
        self._init_runner()
        return reqloop
        
    ############################################################################
    # STOPPING
    ############################################################################
    @async
    def stop(self, force=False):
        '''Stop the actor by stopping its :attr:`Actor.requestloop`
and closing its :attr:`Actor.mailbox`. Once everything is closed
properly this actor will go out of scope.'''
        if force or self._state == self.RUN:
            self.set('stopping_start', time())
            self._state = self.STOPPING
            self.log.debug('stopping')
            # make safe user defined callbacks
            yield safe_async(self.on_stop)
            if not self.is_monitor():
                # shutdown mailbox
                yield self.mailbox.close()
                # shutdown the request loop
                yield self.requestloop.stop()
            self._state = self.CLOSE
            # make safe user defined callbacks
            yield safe_async(self.on_exit)
            self.log.info('exited')
            self.set('stopping_end', time())
            
    def _make_name(self):
        return '%s(%s)' % (self.class_code, self.aid)
    
    def linked_actors(self):
        '''Iterator over linked-actor proxies (no moitors are yielded).'''
        for a in itervalues(self._linked_actors):
            if isinstance(a,ActorProxy):
                yield a
            
    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        if aid == self.aid:
            return self
        elif aid == 'arbiter':
            return self.arbiter
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
        if self.running():
            nt = time()
            if hasattr(self,'last_notified'):
                timeout = self.timeout or self.DEFAULT_ACTOR_TIMEOUT
                tole = min(self.ACTOR_TIMEOUT_TOLERANCE*timeout,
                           self.ACTOR_NOTIFY_PERIOD)
                if nt - self.last_notified < tole:
                    nt = None
            if nt:
                self.last_notified = nt
                info = self.info(True)
                info['last_notified'] = nt
                self.send('arbiter', 'notify', info)
            self.on_task()
    
    def info(self, full=False):
        '''return A dictionary of information related to the actor
status and performance.'''
        isp = self.isprocess()
        data = {'name': self.name,
                'actor_id': self.aid[:8],
                'ppid': self.ppid,
                'thread_id': self.tid,
                'process_id': self.pid,
                'isprocess': isp,
                'age': self.age}
        if isp:
            data.update(system.system_info(self.pid))
        requestloop = self.requestloop
        data.update({'uptime': time() - requestloop._started,
                     'event_loops': requestloop.num_loops,
                     'io_loops': self.ioloop.num_loops})
        return self.on_info(data)
        
    ############################################################################
    # BUILT IN REMOTE FUNCTIONS
    ############################################################################
    def handle_message(self, sender, message, *args, **kwargs):
        '''Handle a *message* from a *sender*.'''
        message_handler = self.get_message_handler('message')
        if message_handler:
            return message_handler(sender, *args, **kwargs)
        else:
            return None
        
    def actor_mailbox_address(self, actor, address):
        '''The *actor* register its mailbox ``address``.'''
        if address:
            self.log.debug('Registering actor {0} inbox address {1}'
                           .format(actor, address))
            actor.address = address
            return self.proxy
        return False
    
    def actor_callback(self, caller, rid, result):
        '''Actor :ref:`remote function <remote-functions>` which sends
the a results back to an actor which previously accessed another remote
function. Essentially this is the return statement in the pulsar concurrent
framework'''
        ActorMessage.actor_callback(rid, result)
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
    def link_actor(self, proxy):
        '''Add the *proxy* to the :attr:`linked_actors` dictionary.
if *proxy* is not a class:`ActorProxy` instance raise an exception.'''
        self._linked_actors[proxy.aid] = proxy
        if proxy == self.arbiter:
            self.requestloop.ready = True
        return proxy
    
    def spawn_failure(self, failure):
        '''A problem occured while spawning a new actor. Log the error.'''
        failure.log()
        
    def _init_runner(self):
        '''Initialise the runner.'''
        if not self.isprocess():
            return
        
        random.seed()
        proc_name = self.DEF_PROC_NAME
        cfg = self.cfg
        proc_name = cfg.get('proc_name') or cfg.get('default_proc_name')\
                         or proc_name
        proc_name = "{0} - {1}".format(proc_name, self)
        if system.set_proctitle(proc_name):
            self.log.debug('Set process title to {0}'.format(proc_name))
        #system.set_owner_process(cfg.uid, cfg.gid)
        if is_mainthread():
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
            #atexit.register(self.stop)
    
    def _run(self):
        '''The run implementation which must be done by a derived class.'''
        try:
            self.requestloop.start()
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
    handle_break = _signal_stop