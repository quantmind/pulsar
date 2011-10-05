import sys
import os
import signal
from time import time
from multiprocessing import current_process
from threading import current_thread


from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   ActorAlreadyStarted,\
                   logerror, LogSelf, LogginMixin, system
from pulsar.utils.py2py3 import iteritems, itervalues, pickle


from .eventloop import IOLoop
from .proxy import ActorProxy, ActorMessage, ActorCallBack,\
                    DEFAULT_MESSAGE_CHANNEL
from .defer import is_async, Deferred
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
    

class Runner(LogginMixin):
    '''Base class with event loop powered
    '''
    DEF_PROC_NAME = 'pulsar'
    SIG_QUEUE = None
    
    def init_runner(self):
        '''Initialise the runner.'''
        self._set_proctitle()
        self._setup()
        self._install_signals()
        
    def _set_proctitle(self):
        '''Set the process title'''
        if self.isprocess():
            proc_name = self.DEF_PROC_NAME
            if hasattr(self,'cfg'):
                proc_name = self.cfg.proc_name or self.cfg.default_proc_name
            else:
                proc_name = self.DEF_PROC_NAME
            proc_name = "{0} - {1}".format(proc_name,self)
            if system.set_proctitle(proc_name):
                self.log.debug('Set process title to {0}'.format(proc_name))
    
    def _install_signals(self):
        '''Initialise signals for correct signal handling.'''
        current = self.current_thread()
        if current == MAIN_THREAD and self.isprocess():
            self.log.info('Installing signals')
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
    
    def _setup(self):
        pass
    
    # SIGGNALS HANDLING
    
    def signal_stop(self, sig, frame):
        signame = system.SIG_NAMES.get(sig,None)
        self.log.warning('{0} got signal {1}. Exiting.'\
                         .format(self.fullname,signame))
        self.stop()
            
    handle_int  = signal_stop
    handle_quit = signal_stop
    handle_term = signal_stop
    


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


class Actor(ActorBase,Runner):
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

    An instance of :class:`pulsar.IOLoop`, the input/output event loop
    driving the actor. Some actors may share the ioloop with other actors
    depending on their concurrency implementation.

'''
    INITIAL = 0X0
    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3
    status = {0x0:'not started',
              0x1:'started',
              0x2:'closed',
              0x3:'terminated'}
    INBOX_TIMEOUT = 0.02
    DEFAULT_IMPLEMENTATION = 'process'
    MINIMUM_ACTOR_TIMEOUT = 10
    DEFAULT_ACTOR_TIMEOUT = 60
    # Send messages to arbiter not before the difference of
    # now and the last message time is greater than this tolerance
    # times timeout. So for a timeout of 30 seconds, the messages will
    # go after tolerance*30 seconds (18 secs for tolerance = 0.6).
    ACTOR_TIMEOUT_TOLERANCE = 0.6
    _stopping = False
    _ppid = None
    _name = None
    
    def __init__(self,impl,*args,**kwargs):
        self.__repr = ''
        self._impl = impl.impl
        self._aid = impl.aid
        self._inbox = impl.inbox
        self._outbox = impl.outbox
        self._timeout = impl.timeout
        self._init(impl,*args,**kwargs)
        
    @property
    def proxy(self):
        '''Instance of an :class:`pulsar.ActorProxy` holding a reference
to the actor. The proxy is a lightweight representation of the actor
which can be shared across different processes (i.e. it is pickable).'''
        return ActorProxy(self)
    
    @property
    def aid(self):
        '''Actor unique identifier'''
        return self._aid
    
    @property
    def ppid(self):
        '''Parent process id.'''
        return self._ppid
    
    @property
    def impl(self):
        '''String indicating actor concurrency implementation
("monitor", "thread", "process" or "greenlet").'''
        return self._impl
    
    @property
    def timeout(self):
        '''Timeout in seconds. If ``0`` the actor has no timeout, otherwise
it will be stopped if it fails to notify itself for a period
longer that timeout.'''
        return self._timeout
    
    @property
    def pid(self):
        '''Operative system process ID where the actor is running.'''
        return os.getpid()
    
    @property
    def tid(self):
        '''Operative system process thread name where the actor is running.'''
        return self.current_thread().name
    
    @property
    def name(self):
        'Actor name'
        if self._name:
            return self._name
        else:
            return self._make_name()
        
    def __repr__(self):
        return self.__repr
        
    @property
    def inbox(self):
        '''Messages inbox'''
        return self._inbox
    
    @property
    def outbox(self):
        '''Messages outbox'''
        return self._outbox
        
    @property
    def fullname(self):
        return self.__repr
    
    def __reduce__(self):
        raise pickle.PicklingError('{0} - Cannot pickle Actor instances'\
                                   .format(self))
    
    # HOOKS
    
    def on_start(self):
        '''The :ref:`actor callback <actor-callbacks>` run once just before
the actor starts (after forking).'''
        pass
    
    def on_task(self):
        '''The :ref:`actor callback <actor-callbacks>` executed at each
iteration of the :attr:`pulsar.Actor.ioloop`.'''
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
 via the :meth:`pulsar.Actor.info` method which is also exposed
 as a remote function.
 
 :parameter data: dictionary of data with information about the actor.
 :rtype: a dictionary of pickable data.'''
        return data
    
    def on_manage_actor(self, actor):
        pass
    
    def is_alive(self):
        '''``True`` if actor is running.'''
        return self._state == self.RUN
    
    def started(self):
        '''``True`` if actor has started.'''
        return self._state >= self.RUN
    
    def closed(self):
        '''``True`` if actor has exited in an clean fashion.'''
        return self._state == self.CLOSE
    
    def stopped(self):
        '''``True`` if actor has exited.'''
        return self._state >= self.CLOSE
    
    # INITIALIZATION AFTER FORKING FOR ALL ACTORS
    
    def _init(self, impl, arbiter = None, monitor = None,
              on_task = None, ioqueue = None,
              actor_links = None, name = None, socket = None,
              age = 0):
        # This function is called just after forking (if the concurrency model
        # is process, otherwise just after the concurrent model has started).
        self.arbiter = arbiter
        self.monitor = monitor
        self.age = age
        self.nr = 0
        self.loglevel = impl.loglevel
        self._name = name or self._name
        self._state = self.INITIAL
        self.log = self.getLogger()
        
        # If arbiter available
        if arbiter:
            self.__repr = '{0} {1}'.format(self._name,self._aid)
            self.log = LogSelf(self,self.log)
            if on_task:
                self.on_task = on_task
        else:
            self.__repr = self._name
            
        self.channels = {}
        self.ACTOR_LINKS = actor_links or {}
        self._linked_actors = {}
        for a in itervalues(self.ACTOR_LINKS):
            self._linked_actors[a.aid] = a
        self.ioqueue = ioqueue
        self.ioloop = self.get_eventloop(impl)
        # ADD SELF TO THE EVENT LOOP TASKS
        self.ioloop.add_loop_task(self)
        self.set_socket(socket)
                
        inbox = self.inbox
        if inbox:
            inbox.set_actor(self)
    
    def handle_request(self, request):
        pass
    
    def set_socket(self, socket):
        self.socket = socket
        self.address = None if not self.socket else self.socket.getsockname()
        if self.socket:
            self.log.info('"{0}" listening at {1}'\
                          .format(self.fullname,self.socket))
            
    def start(self):
        '''Called after forking to start the life of the actor.'''
        if self._state == self.INITIAL:
            if self.isprocess():
                self.configure_logging()
            # if monitor and a ioqueue available, register
            # the event loop handler
            self.on_start()
            self.init_runner()
            self.log.info('Booting')
            self._state = self.RUN
            self._run()
            return self
    
    def get_eventloop(self, impl):
        ioq = self.ioqueue
        ioimpl = IOQueue(ioq) if ioq else None
        ioloop = IOLoop(io = ioimpl,
                        logger = self.log,
                        name = self.name)
        #add the handler
        if ioq and self.monitor:
            ioloop.add_handler('request',
                        lambda fd, request : self.handle_request(request),
                        ioloop.READ)
        return ioloop
    
    def message_arrived(self, request):
        '''A new message has arrived in the actor inbox.'''
        try:
            sender = self.get_actor(request.sender)
            receiver = self.get_actor(request.receiver)
            if not sender or not receiver:
                if not sender:
                    self.log.warn('message from an unknown actor "{0}"'\
                                  .format(request.sender))
                if not receiver:
                    self.log.warn('message for an unknown actor "{0}"'\
                                  .format(request.receiver))
            else:                
                func = receiver.actor_functions.get(request.action,None)
                if func:
                    ack = getattr(func,'ack',True)
                    args,kwargs = request.msg
                    result = func(self, sender, *args, **kwargs)
                else:
                    result = self.channel_messsage(sender, request)
        except Exception as e:
            #self.handle_request_error(request,e)
            result = e
            if self.log:
                self.log.critical('Unhandled error while processing worker\
 request: {0}'.format(e), exc_info=True)
        finally:
            if ack:
                ActorCallBack(self,result).\
                    add_callback(request.make_actor_callback(self,caller))
        
    def put(self, request):
        '''Put a request into the actor :attr:`ioqueue` if available.'''
        if self.ioqueue:
            self.log.debug('Put a request on task queue')
            self.ioqueue.put(('request',request))
        else:
            self.log.warning('Trying to put a request on task queue,\
 but {0} does not have one')
        
    # STOPPING TERMINATIONG AND STARTING
    
    def stop(self):
        '''Stop the actor by stopping its :attr:`pulsar.Actor.ioloop`
and closing its :attr:`pulsar.Actor.inbox` orderly. Once everything is closed
properly this actor will go out of scope.'''
        # This may be called on a different process domain.
        # In that case there is no ioloop and therefore skip altogether
        if hasattr(self,'ioloop'):
            if self.is_alive() and not self._stopping:
                self._stopping = True
                if not self.on_stop():
                    self._stop_ioloop().add_callback(lambda r : self._stop())
        
    def _stop(self):
        #Callback after the event loop has stopped.
        if self._stopping:
            self.on_exit()
            self._state = self.CLOSE
            if not self.ioloop.remove_loop_task(self):
                self.log.warn('"{0}" could not be removed from\
 eventloop'.format(self.fullname))
            if self.impl != 'monitor':
                self.arbiter.send(self,'on_actor_exit')
            self._stopping = False
            if self.inbox:
                self.inbox.close()
            if self.outbox:
                self.outbox.close()
            self.log.info('exited')
        
    def terminate(self):
        self.stop()
        
    def shut_down(self):
        '''Called by ``self`` to shut down the arbiter'''
        if self.arbiter:
            self.proxy.stop(self.arbiter)
            
    # LOW LEVEL API
    
    def _make_name(self):
        return '{0}({1})'.format(self.class_code,self.aid[:8])
    
    def _stop_ioloop(self):
        return self.ioloop.stop()
        
    def _run(self):
        '''The run implementation which must be done by a derived class.'''
        try:
            self.ioloop.start()
        except SystemExit:
            raise
        except Exception as e:
            self.log.exception("Exception in worker {0}: {1}"\
                               .format(self.fullname,e))
        finally:
            self.log.debug('exiting "{0}"'.format(self.fullname))
            self._stop()
    
    def linked_actors(self):
        '''Iterator over linked-actor proxies'''
        return itervalues(self._linked_actors)
    
    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        if aid == self.aid:
            return self.proxy
        elif aid in self._linked_actors:
            return self._linked_actors[aid]
        elif self.arbiter and aid == self.arbiter.aid:
            return self.arbiter
        elif self.monitor and aid == self.monitor.aid:
            return self.monitor

    def __call__(self):
        '''Called in the main eventloop to perform the following
actions:

* it notifies linked actors if required (arbiter only for now)
* it executes the :meth:`pulsar.Actor.on_task` callback.
'''
        # If this is not a monitor
        # we notify to the arbiter we are still alive
        if self.is_alive() and self.arbiter and self.impl != 'monitor':
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

        if not self._stopping:
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
                'thread':self.current_thread().name,
                'process':self.current_process().name,
                'isprocess':self.isprocess(),
                'age':self.age}
        ioloop = self.ioloop
        if ioloop:
            data.update({'uptime': time() - ioloop._started,
                         'event_loops': ioloop.num_loops})
        return self.on_info(data)
        
    def configure_logging(self):
        if not self.loglevel:
            if self.arbiter:
                self.loglevel = self.arbiter.loglevel
        super(Actor,self).configure_logging()
        
    #################################################################
    # BUILT IN REMOTE FUNCTIONS
    #################################################################
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
the actor by invoking the local :meth:`pulsar.Actor.stop` method. This
function does not acknowledge its caller since the actor will stop running.'''
        self.stop()
    actor_stop.ack = False
    
    def actor_notify(self, caller, info):
        '''Actor :ref:`remote function <remote-functions>` for notifying
the actor information to the caller.'''
        caller.info = info
    actor_notify.ack = False
    
    def actor_on_actor_exit(self, caller, reason = None):
        if caller:
            self._linked_actors.pop(caller.aid,None)
    actor_on_actor_exit.ack = False
    
    def actor_info(self, caller, full = False):
        '''Get server Info and send it back.'''
        return self.info(full)
    
    def actor_ping(self, caller):
        return 'pong'
    
    def actor_kill_actor(self, caller, aid):
        return self.arbiter.kill_actor(aid)


