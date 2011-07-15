import sys
import os
import signal
from time import time
from multiprocessing import current_process
from multiprocessing.queues import Empty
from threading import current_thread


from pulsar import AlreadyCalledError, AlreadyRegistered,\
                   ActorAlreadyStarted,\
                   logerror, LogSelf, LogginMixin, system
from pulsar.http import get_httplib
from pulsar.utils.py2py3 import iteritems, itervalues, pickle


from .eventloop import IOLoop
from .proxy import ActorProxy, ActorRequest, ActorCallBack, DEFAULT_MESSAGE_CHANNEL
from .impl import ActorProcess, ActorThread, ActorMonitorImpl
from .defer import is_async, Deferred


__all__ = ['is_actor',
           'Actor',
           'ActorRequest',
           'MAIN_THREAD',
           'Empty']


EMPTY_TUPLE = ()
EMPTY_DICT = {}


def is_actor(obj):
    return isinstance(obj,Actor)


class HttpMixin(object):
    
    @property
    def http(self):
        return get_httplib(self.cfg)

    
MAIN_THREAD = current_thread()

   
class Runner(LogginMixin,HttpMixin):
    '''Base class for classes with an event loop.
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
            if hasattr(self,'cfg'):
                proc_name = self.cfg.proc_name or self.cfg.default_proc_name
            else:
                proc_name = self.DEF_PROC_NAME
            system.set_proctitle("{0} - {1}".format(proc_name,self))
    
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
        self.log.warning('Received signal {0}. Exiting.'.format(signame))
        self.stop()
            
    handle_int  = signal_stop
    handle_quit = signal_stop
    handle_term = signal_stop
    


class ActorMetaClass(type):
    
    def __new__(cls, name, bases, attrs):
        make = super(ActorMetaClass, cls).__new__
        fprefix = 'actor_'
        attrib  = '{0}functions'.format(fprefix)
        cont = {}
        for key, method in attrs.items():
            if hasattr(method,'__call__') and key.startswith(fprefix):
                meth_name = key[len(fprefix):]
                ack = getattr(method,'ack',True)
                cont[meth_name] = ack
            for base in bases[::-1]:
                if hasattr(base, attrib):
                    rbase = getattr(base,attrib)
                    for key,method in rbase.items():
                        if not key in cont:
                            cont[key] = method
                        
        attrs[attrib] = cont
        return make(cls, name, bases, attrs)

    
ActorBase = ActorMetaClass('BaseActor',(object,),{})


class Actor(ActorBase,Runner):
    '''A python implementation of the **Actor primitive**. This is computer science,
model that treats ``actors`` as the atoms of concurrent digital computation.
In response to a message that it receives, an actor can make local decisions,
create more actors, send more messages, and determine how to respond to
the next message received.
The current implementation allows for actor to perform specific tasks such as listening to a socket,
acting as http server and so forth.

To spawn a new actor::

    >>> from pulsar import Actor, spawn
    >>> a = spawn(Actor)
    >>> a.is_alive()
    True
    
Here ``a`` is actually a reference to the remote actor.

.. attribute:: age

    The age of actor, used to access how long the actor has been created.
    
.. attribute:: task_queue

    The task queue where the actor's monitor add tasks to be processed by ``self``.
    This queue is used by a subsets of workers only.
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
    MINIMUM_ACTOR_TIMEOUT = 1
    DEFAULT_ACTOR_TIMEOUT = 60
    ACTOR_TIMEOUT_TOLERANCE = 0.2
    FLASH_LOOPS = 3
    _stopping = False
    _ppid = None
    _name = None
    _listening = True
    _runner_impl = {'monitor':ActorMonitorImpl,
                    'thread':ActorThread,
                    'process':ActorProcess}
    
    def __init__(self,impl,*args,**kwargs):
        self._impl = impl.impl
        self._aid = impl.aid
        self._inbox = impl.inbox
        self._timeout = impl.timeout
        self._init(impl,*args,**kwargs)
        
    @property
    def proxy(self):
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
        '''Actor concurrency implementation ("thread", "process" or "greenlet").'''
        return self._impl
    
    @property
    def timeout(self):
        '''Timeout in seconds. If ``0`` the actor has no timeout, otherwise
it will be stopped if it fails to notify itself for a period longer that timeout.'''
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
        'Actor unique name'
        if self._name:
            return self._name
        else:
            return self._make_name()
        
    def __repr__(self):
        return self.name
        
    @property
    def inbox(self):
        '''Message inbox'''
        return self._inbox
        
    def __reduce__(self):
        raise pickle.PicklingError('{0} - Cannot pickle Actor instances'.format(self))
    
    # HOOKS
    
    def on_start(self):
        '''Callback just before the actor starts
(after forking before :meth:`_run` method).'''
        pass
    
    def on_task(self):
        '''Callback executed at each actor event loop.'''
        pass
    
    def on_stop(self):
        '''Callback executed before stopping the actor.'''
        pass
    
    def on_exit(self):
        '''Called just before the actor is exting.'''
        pass
    
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
    
    # INITIALIZATION AFTER FORKING
    def _init(self, impl, arbiter = None, monitor = None,
              on_task = None, task_queue = None,
              actor_links = None, name = None, socket = None,
              age = 0):
        self.arbiter = arbiter
        self.monitor = monitor
        self.age = age
        self.nr = 0
        self.loglevel = impl.loglevel
        self._name = name or self._name
        self._state = self.INITIAL
        self.log = self.getLogger()
        self.channels = {}
        self.ACTOR_LINKS = actor_links or {}
        self._linked_actors = {}
        for a in itervalues(self.ACTOR_LINKS):
            self._linked_actors[a.aid] = a
        self.task_queue = task_queue
        self.ioloop = self._get_eventloop(impl)
        self.ioloop.add_loop_task(self)
        self.set_socket(socket)
        if on_task:
            self.on_task = on_task
    
    def set_socket(self, socket):
        if not hasattr(self,'socket'):
            self.socket = socket
        self.address = None if not self.socket else self.socket.getsockname()
        if self.socket and self._listening:
            self.log.info('"{0}" listening at {1}'.format(self,self.socket))
            
    def start(self):
        if self._state == self.INITIAL:
            if self.isprocess():
                self.configure_logging()
            self.on_start()
            self.init_runner()
            self.log.info('Booting "{0}"'.format(self.name))
            self._state = self.RUN
            self._run()
            return self
    
    def _get_eventloop(self, impl):
        ioimpl = impl.get_ioimpl()
        return IOLoop(impl = ioimpl, logger = LogSelf(self,self.log))
    
    # STOPPING TERMINATIONG AND STARTING
    
    def stop(self):
        # This may be called on a different process domain.
        # In that case there is no ioloop and therefore skip altogether
        if hasattr(self,'ioloop'):
            if self.is_alive() and not self._stopping:
                self._stopping = True
                if not self.on_stop():
                    self._stop_ioloop().add_callback(lambda r : self._stop())
        
    def _stop(self):
        '''Callback after the event loop has stopped.'''
        if self._stopping:
            self.on_exit()
            self._state = self.CLOSE
            if not self.ioloop.remove_loop_task(self):
                self.log.warn('"{0}" could not be removed from eventloop'.format(self))
            if self.impl != 'monitor':
                self.proxy.on_actor_exit(self.arbiter)
            self._stopping = False
            self._inbox.close()
            self.log.info('exited "{0}"'.format(self))
        
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
            self.log.exception("Exception in worker {0}: {1}".format(self,e))
        finally:
            self.log.debug('exiting "{0}"'.format(self))
            self._stop()
    
    def linked_actors(self):
        '''Iterator over linked-actor proxies'''
        return itervalues(self._linked_actors)
    
    @logerror
    def flush(self, closing = False):
        '''Flush one message from the inbox and runs callbacks.
This function should live on a event loop.'''
        inbox = self._inbox
        timeout = self.INBOX_TIMEOUT
        flashed = 0
        while closing or flashed < self.FLASH_LOOPS:
            request = None
            ack = False
            try:
                request = inbox.get(timeout = timeout)
            except Empty:
                break
            except IOError:
                break
            flashed += 1
            try:
                caller = self.get_actor(request.aid)
                if not caller and not closing:
                    self.log.warn('"{0}" got a message from an un-linked\
 actor "{1}"'.format(self,request.aid[:8]))
                else:
                    func = getattr(self,'actor_{0}'.format(request.name),None)
                    if func:
                        ack = getattr(func,'ack',True)
                    result = self.handle_request_from_actor(caller,request,func)
            except Exception as e:
                #self.handle_request_error(request,e)
                result = e
                if self.log:
                    self.log.error('Error while processing worker request: {0}'.format(e),
                                   exc_info=sys.exc_info())
            finally:
                if ack:
                    ActorCallBack(self,result).\
                        add_callback(request.make_actor_callback(self,caller))
    
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
    
    def handle_request_from_actor(self, caller, request, func):
        '''Handle a request from a linked actor'''
        if func:
            args = request.msg[0]
            kwargs = request.msg[1]
            return func(caller, *args, **kwargs)
        else:
            msg = request.msg
            name = request.name or DEFAULT_MESSAGE_CHANNEL
            if name not in self.channels:
                self.channels[name] = []
            ch = self.channels[name]
            ch.append(request)

    def __call__(self):
        '''Called in the main eventloop, It flush the inbox queue and notified linked actors'''
        self.flush()
        # If this is not a monitor, we notify to the arbiter we are still alive
        if self.is_alive():
            if self.arbiter and self.impl != 'monitor':
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
                    self.proxy.notify(self.arbiter,nt)
            if not self._stopping:
                self.on_task()
    
    def current_thread(self):
        '''Return the current thread'''
        return current_thread()
    
    def current_process(self):
        return current_process()
    
    def isprocess(self):
        return self.impl == 'process'
    
    def info(self, full = False):
        data = {'aid':self.aid[:8],
                'pid':self.pid,
                'ppid':self.ppid,
                'thread':self.current_thread().name,
                'process':self.current_process().name,
                'isprocess':self.isprocess()}
        ioloop = self.ioloop
        if ioloop:
            data.update({'uptime': time() - ioloop._started,
                         'event_loops': ioloop.num_loops})
        return data
        
    def configure_logging(self):
        if not self.loglevel:
            if self.arbiter:
                self.loglevel = self.arbiter.loglevel
        super(Actor,self).configure_logging()
        
    # BUILT IN ACTOR FUNCTIONS
    
    def actor_callback(self, caller, rid, result):
        #self.log.debug('Received Callaback {0}'.format(rid))
        ActorRequest.actor_callback(rid,result)
    actor_callback.ack = False
    
    def actor_stop(self, caller):
        self.stop()
    actor_stop.ack = False
    
    def actor_notify(self, caller, t):
        '''An actor notified itself'''
        caller.notified = t
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


    # CLASS METHODS
    
    @classmethod
    def modify_arbiter_loop(cls, wp):
        '''Called by an instance of :class:`pulsar.WorkerPool`, it modify the 
event loop of the arbiter if required.

:parameter wp: Instance of :class:`pulsar.WorkerPool`
:parameter ioloop: Arbiter event loop
'''
        pass
    
    @classmethod
    def clean_arbiter_loop(cls, wp):
        pass

    @classmethod
    def get_task_queue(cls, monitor):
        return None

