import sys
from functools import partial
from multiprocessing import Process, current_process

from pulsar import system, platform
from pulsar.utils.security import gen_unique_id
from pulsar.utils.pep import itervalues

from .proxy import ActorProxyMonitor, get_proxy
from .access import get_actor, set_actor, get_actor_from_id, remove_actor
from .threads import KillableThread, ThreadQueue, Empty
from .mailbox import MailboxClient, MailboxConsumer, ProxyMailbox
from .defer import async, multi_async, log_failure
from .eventloop import new_event_loop, signal
from .servers import TcpServer
from .consts import *


__all__ = ['Concurrency', 'concurrency']


def concurrency(kind, actor_class, monitor, cfg, **params):
    '''Function invoked by the :class:`Arbiter` or a :class:`Monitor` when
spawning a new :class:`Actor`. It created a :class:`Concurrency` instance
which handle the contruction and the lif of an :class:`Actor`.

:paramater kind: Type of concurrency
:paramater monitor: The monitor (or arbiter) managing the :class:`Actor`.
:rtype: a :class:`Councurrency` instance
'''
    maker = concurrency_models.get(kind)
    if maker:
        c = maker()
        return c.make(kind, actor_class, monitor, cfg, **params)
    else:
        raise ValueError('Concurrency %s not supported in pulsar' % kind)
    return c.make(kind, actor_class, monitor, cfg, **params)


class Concurrency(object):
    '''Actor :class:`Concurrency` is responsible for the actual spawning of
actors according to a concurrency implementation. Instances are pickable
and are shared between the :class:`Actor` and its
:class:`ActorProxyMonitor`.
This is an abstract class, derived classes must implement the ``start`` method.

:parameter concurrency: string indicating the concurrency implementation.
    Valid choices are ``monitor``, ``process``, ``thread``.
:parameter actor_class: :class:`Actor` or one of its subclasses.
:parameter timeout: timeout in seconds for the actor.
:parameter kwargs: additional key-valued arguments to be passed to the actor
    constructor.
'''
    _creation_counter = 0
    def make(self, kind, actor_class, monitor, cfg, name=None, aid=None, **kw):
        self.__class__._creation_counter += 1
        self.aid = aid or gen_unique_id()[:8]
        self.age = self.__class__._creation_counter
        self.name = name or actor_class.__name__.lower()
        self.kind = kind
        self.cfg = cfg
        self.actor_class = actor_class
        self.params = kw
        self.params['monitor'] = monitor
        return self.get_actor()

    @property
    def unique_name(self):
        return '%s(%s)' % (self.name, self.aid)

    def __repr__(self):
        return self.unique_name
    __str__ = __repr__

    def before_start(self, actor):
        pass
    
    def is_process(self):
        return False
    
    def is_arbiter(self):
        return False
    
    def io_poller(self):
        '''Return a IO poller instance which sets the :class:`EventLoop.io`
handler. By default it return nothing so that the best handler for the
system is chosen.'''
        pass
    
    def run_actor(self, actor):
        '''Start running the ``actor``.'''
        actor.event_loop.run()
    
    def setup_event_loop(self, actor):
        '''Set up the event loop for ``actor``. Must be
implemented by subclasses.'''
        raise NotImplementedError
    
    def can_continue(self, actor):
        '''Check if ``actor`` can continue its life.'''
        return True
    
    def hand_shake(self, actor):
        try:
            a = get_actor()
            if a is not actor:
                set_actor(actor)
            actor.state = ACTOR_STATES.RUN
            r = self.periodic_task(actor)
            if r:
                r.add_callback(partial(self.started, actor))
            else:
                actor.stop()
        except Exception as e:
            actor.stop(e)
            
    def started(self, actor, result=None):
        actor.logger.info('%s started', actor)
        actor.fire_event('start')
    
    def get_actor(self):
        self.daemon = True
        self.params['monitor'] = get_proxy(self.params['monitor'])
        # make sure these parameters are pickable
        #pickle.dumps(self.params)
        return ActorProxyMonitor(self)
    
    def create_mailbox(self, actor, event_loop):
        set_actor(actor)
        client = MailboxClient(actor.monitor.address, actor, event_loop)
        client.event_loop.call_soon_threadsafe(self.hand_shake, actor)
        return client

    def periodic_task(self, actor):
        '''Internal method called periodically by the :attr:`event_loop` to
ping the actor monitor.'''
        if self.can_continue(actor):
            if actor.running():
                actor.logger.debug('%s notifying the monitor', actor)
                # if an error occurs, shut down the actor
                try:
                    r = actor.send('monitor', 'notify', actor.info())\
                            .add_errback(actor.stop)
                except Exception as e:
                    actor.stop(e)
                else:
                    secs = max(ACTOR_TIMEOUT_TOLE*actor.cfg.timeout, MIN_NOTIFY)
                    next = min(secs, MAX_NOTIFY)
                    actor.event_loop.call_later(next, self.periodic_task, actor)
                    return r
            else:
                actor.event_loop.call_soon_threadsafe(self.periodic_task, actor)
                    
    def stop(self, actor, exc):
        if exc != -1:
            log_failure(exc)
            if actor.state <= ACTOR_STATES.RUN:
                # The actor has not started the stopping process. Starts it now.
                actor.exit_code = getattr(exc, 'exit_code', 1) if exc else 0
                actor.state = ACTOR_STATES.STOPPING
                actor.fire_event('stopping')
                actor.close_thread_pool()
                self._stop_actor(actor)
            elif actor.stopped():
                # The actor has finished the stopping process.
                #Remove itself from the actors dictionary
                remove_actor(actor)
                actor.fire_event('stop')
            return actor.event('stop')
        
    def _stop_actor(self, actor):
        '''Exit from the :class:`Actor` domain.'''
        actor.bind_event('stop', actor._bye)
        actor.state = ACTOR_STATES.CLOSE
        actor.mailbox.close()


class ProcessMixin(object):
    
    def is_process(self):
        return True
    
    def setup_event_loop(self, actor):
        event_loop = new_event_loop(io=self.io_poller(), logger=actor.logger,
                                    poll_timeout=actor.params.poll_timeout)
        actor.mailbox = self.create_mailbox(actor, event_loop)
        proc_name = "%s-%s" % (actor.cfg.proc_name, actor)
        if system.set_proctitle(proc_name):
            actor.logger.debug('Set process title to %s', proc_name)
        #system.set_owner_process(cfg.uid, cfg.gid)
        if signal and not platform.is_windows:
            actor.logger.debug('Installing signals')
            actor.signal_queue = ThreadQueue()
            for name in system.ALL_SIGNALS:
                sig = getattr(signal, 'SIG%s' % name)
                try:
                    handler = partial(actor.signal_queue.put, sig)
                    actor.event_loop.add_signal_handler(sig, handler)
                except ValueError:
                    break
                
    def can_continue(self, actor):
        if actor.signal_queue is not None:
            while True:
                try:
                    sig = actor.signal_queue.get(timeout=0.01)
                except (Empty, IOError):
                    break
                if sig not in system.SIG_NAMES:
                    actor.logger.info("Ignoring unknown signal: %s", sig)
                else:
                    signame = system.SIG_NAMES.get(sig)
                    if sig in system.EXIT_SIGNALS:
                        actor.logger.warning("Got signal %s. Stopping.", signame)
                        actor.stop()
                        return False
                    else:
                        actor.logger.debug('No handler for signal %s.', signame)
        return True
                
                
class MonitorMixin(object):
    
    def get_actor(self):
        return self.actor_class(self)

    def start(self):
        pass

    def is_active(self):
        return self.actor.is_alive()

    def hand_shake(self, actor):
        ''':class:`MonitorMixin` doesn't do hand shakes'''
        try:
            actor.state = ACTOR_STATES.RUN
            actor.fire_event('start')
            self.periodic_task(actor)
        except Exception:
            actor.stop(sys.exc_info())
            
    @property
    def pid(self):
        return current_process().pid

################################################################################
##    CONCURRENCY IMPLEMENTATIONS

class MonitorConcurrency(MonitorMixin, Concurrency):
    ''':class:`Concurrency` class for :class:`Monitor`. Monitors live in
the **mainthread** of the master process and therefore do not require
to be spawned.'''
    def setup_event_loop(self, actor):
        actor.mailbox = ProxyMailbox(actor)
        actor.mailbox.event_loop.call_soon_threadsafe(self.hand_shake, actor)
        
    def run_actor(self, actor):
        return -1
    
    def create_mailbox(self, actor, event_loop):
        pass
    
    def periodic_task(self, actor):
        interval = 0
        if actor.running():
            interval = MONITOR_TASK_PERIOD
            actor.manage_actors()
            actor.spawn_actors()
            actor.stop_actors()
            actor.monitor_task()
        actor.event_loop.call_later(interval, self.periodic_task, actor)
        
    @async()
    def _stop_actor(self, actor):
        try:
            yield actor.close_actors()
        finally:
            if not actor.terminated_actors:
                actor.monitor._remove_actor(actor)
            actor.fire_event('stop')          


class ArbiterConcurrency(MonitorMixin, ProcessMixin, Concurrency):
    
    def is_arbiter(self):
        return True
    
    def before_start(self, actor):
        '''Daemonise the system if required.'''
        if actor.cfg.daemon: #pragma    nocover
            system.daemonize()
                
    def create_mailbox(self, actor, event_loop):
        '''Override :meth:`Concurrency.create_mailbox` to create the
mailbox server.'''
        mailbox = TcpServer(event_loop, '127.0.0.1', 0,
                            consumer_factory=MailboxConsumer,
                            name='mailbox')
        # when the mailbox stop, close the event loop too
        mailbox.bind_event('stop', lambda exc: event_loop.stop())
        mailbox.bind_event('start', lambda exc: \
            event_loop.call_soon_threadsafe(self.hand_shake, actor))
        mailbox.start_serving()
        return mailbox
    
    def periodic_task(self, actor):
        # Arbiter periodic task
        interval = 0
        if self.can_continue(actor) and actor.running():
            # managed actors job
            interval = MONITOR_TASK_PERIOD
            actor.manage_actors()
            for m in list(itervalues(actor.monitors)):
                if m.started():
                    if not m.running():
                        actor._remove_actor(m)
                else:
                    m.start()
        actor.event_loop.call_later(interval, self.periodic_task, actor)

    def _stop_actor(self, actor):
        '''Stop the pools the message queue and remaining actors.'''
        if actor.event_loop.running:
            self._exit_arbiter(actor)
        else:
            actor.event_loop.call_soon_threadsafe(self._exit_arbiter, actor)
            actor._run(False)
        
    def _exit_arbiter(self, actor, res=None):
        if res:
            actor.state = ACTOR_STATES.CLOSE
            actor.mailbox.close()
        else:
            active = multi_async((actor.close_monitors(), actor.close_actors()),
                                 log_failure=True)
            active.add_both(partial(self._exit_arbiter, actor))
    
    
class ActorProcess(ProcessMixin, Concurrency, Process):
    '''Actor on a Operative system process. Created using the
python multiprocessing module.'''
    def run(self):
        actor = self.actor_class(self)
        actor.start()


class ActorThread(Concurrency, KillableThread):
    '''Actor on a thread in the master process.'''
    def run(self):
        actor = self.actor_class(self)
        actor.start()
        
    def setup_event_loop(self, actor):
        '''Create the event loop but don't install signals.'''
        event_loop = new_event_loop(io=self.io_poller(), logger=actor.logger,
                                    poll_timeout=actor.params.poll_timeout)
        actor.mailbox = self.create_mailbox(actor, event_loop)
        
    def terminate(self, kill=False):
        if self.is_alive():
            actor = get_actor_from_id(self.aid)
            me = get_actor()
            if not kill and actor and actor != me:
                actor.stop(1)
                me.event_loop.call_later(1, self.terminate, True)
            else:
                KillableThread.terminate(self)


class ActorCoroutine(MonitorConcurrency):
    '''Actor sharing the :class:`Arbiter` event loop.'''
    def start(self):
        actor = self.actor_class(self)
        actor.start()
        
    def periodic_task(self, actor):
        pass


concurrency_models = {'arbiter': ArbiterConcurrency,
                      'monitor': MonitorConcurrency,
                      'thread': ActorThread,
                      'process': ActorProcess,
                      'coroutine': ActorCoroutine}
    