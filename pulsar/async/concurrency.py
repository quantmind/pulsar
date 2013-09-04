from functools import partial
from multiprocessing import Process, current_process

from pulsar import system, platform
from pulsar.utils.security import gen_unique_id
from pulsar.utils.pep import new_event_loop, itervalues

from .proxy import ActorProxyMonitor, get_proxy
from .access import get_actor, set_actor, remove_actor, logger
from .threads import Thread, ThreadQueue, Empty
from .mailbox import MailboxClient, MailboxConsumer, ProxyMailbox
from .defer import multi_async, maybe_failure, Failure
from .eventloop import signal
from .stream import TcpServer
from .pollers import POLLERS
from .consts import *


__all__ = ['Concurrency', 'concurrency']


def concurrency(kind, actor_class, monitor, cfg, **params):
    '''Function invoked by the :class:`Arbiter` or a :class:`Monitor` when
spawning a new :class:`Actor`. It created a :class:`Concurrency` instance
which handle the contruction and the lif of an :class:`Actor`.

:parameter kind: Type of concurrency.
:parameter monitor: The monitor (or arbiter) managing the :class:`Actor`.
:return: a :class:`Councurrency` instance.
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
actors according to a concurrency implementation. Instances are picklable
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
        return POLLERS[self.cfg.poller]()
    
    def run_actor(self, actor):
        '''Start running the ``actor``.'''
        actor.event_loop.run_forever()
    
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
        # make sure these parameters are picklable
        #pickle.dumps(self.params)
        return ActorProxyMonitor(self)
    
    def create_mailbox(self, actor, event_loop):
        '''Create the mailbox for ``actor``.'''
        set_actor(actor)
        client = MailboxClient(actor.monitor.address, actor, event_loop)
        client.event_loop.call_soon_threadsafe(self.hand_shake, actor)
        client.bind_event('finish', lambda s: event_loop.stop())
        return client

    def periodic_task(self, actor):
        '''Implement the :ref:`actor period task <actor-periodic-task>`.
        
This is an internal method called periodically by the :attr:`Actor.event_loop`
to ping the actor monitor. If successful return a :class:`Deferred` called back
with the acknowledgement from the monitor.'''
        actor.next_periodic_task = None
        if self.can_continue(actor):
            ack = None
            if actor.is_running():
                actor.logger.debug('%s notifying the monitor', actor)
                # if an error occurs, shut down the actor
                ack = actor.send('monitor', 'notify', actor.info())\
                           .add_errback(actor.stop)
                next = max(ACTOR_TIMEOUT_TOLE*actor.cfg.timeout, MIN_NOTIFY)
            else:
                next = 0
            actor.next_periodic_task = actor.event_loop.call_later(
                min(next, MAX_NOTIFY), self.periodic_task, actor)
            return ack
    
    def stop(self, actor, exc):
        '''Gracefully stop the ``actor``.'''
        failure = maybe_failure(exc)
        if actor.state <= ACTOR_STATES.RUN:
            # The actor has not started the stopping process. Starts it now.
            actor.state = ACTOR_STATES.STOPPING
            if isinstance(failure, Failure):
                actor.exit_code = getattr(failure.error, 'exit_code', 1)
                if actor.exit_code == 1:
                    failure.log(msg='Stopping %s' % actor,
                                log=actor.logger)
                elif actor.exit_code:
                    failure.mute()
                    print(str(failure.error))
                else:
                    failure.log(msg='Stopping %s' % actor,
                                log=actor.logger,
                                level='info')
            else:
                if actor.logger:
                    actor.logger.debug('stopping')
                actor.exit_code = 0
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
        actor.state = ACTOR_STATES.CLOSE
        if actor.event_loop.is_running():
            actor.mailbox.close()
        else:
            actor.exit_code = 1
            actor.mailbox.abort()
            self.stop(actor, 0)


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
                    actor.event_loop.add_signal_handler(sig, self.handle_signal,
                                                        actor)
                except ValueError:
                    break
    
    def handle_signal(self, actor, sig, frame):
        actor.signal_queue.put(sig)
        # call the periodic task of the actor
        actor.event_loop.call_soon(self.periodic_task, actor)
        
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
                        actor.logger.warning("Got %s. Stopping.", signame)
                        actor.event_loop.stop()
                        return False
                    else:
                        actor.logger.debug('No handler for %s.', signame)
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
        actor.state = ACTOR_STATES.RUN
        actor.fire_event('start')
        self.periodic_task(actor)
            
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
        '''Override the :meth:`Concurrency.periodic_task` to implement
        the :class:`Monitor` :ref:`periodic task <actor-periodic-task>`.'''
        interval = 0
        actor.next_periodic_task = None
        if actor.is_running():
            interval = MONITOR_TASK_PERIOD
            actor.manage_actors()
            actor.spawn_actors()
            actor.stop_actors()
            actor.monitor_task()
        actor.next_periodic_task = actor.event_loop.call_later(
            interval, self.periodic_task, actor)
        
    def _stop_actor(self, actor):
        def _cleanup(result):
            if not actor.terminated_actors:
                actor.monitor._remove_actor(actor)
            actor.fire_event('stop')
        return actor.close_actors().add_both(_cleanup)          


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
        '''Override the :meth:`Concurrency.periodic_task` to implement
        the :class:`Arbiter` :ref:`periodic task <actor-periodic-task>`.'''
        interval = 0
        actor.next_periodic_task = None
        if self.can_continue(actor):
            if actor.is_running():
                # managed actors job
                interval = MONITOR_TASK_PERIOD
                actor.manage_actors()
                for m in list(itervalues(actor.monitors)):
                    if m.started():
                        if not m.is_running():
                            actor._remove_actor(m)
                    else:
                        m.start()
            actor.next_periodic_task = actor.event_loop.call_later(
                interval, self.periodic_task, actor)

    def _stop_actor(self, actor):
        '''Stop the pools the message queue and remaining actors.'''
        if actor.event_loop.running:
            self._exit_arbiter(actor)
        else:
            actor.logger.debug('Restarts event loop to removing actors')
            actor.event_loop.call_soon_threadsafe(self._exit_arbiter, actor)
            actor._run(False)
        
    def _exit_arbiter(self, actor, res=None):
        if res:
            actor.logger.debug('Closing mailbox server')
            actor.state = ACTOR_STATES.CLOSE
            actor.mailbox.close()
        else:
            actor.logger.debug('Close monitors and actors')
            active = multi_async((actor.close_monitors(), actor.close_actors()),
                                 log_failure=True)
            active.add_both(partial(self._exit_arbiter, actor))
    

def run_actor(self):
    self._actor = actor = self.actor_class(self)
    try:
        actor.start()
    finally:
        try:
            actor.cfg.when_exit(actor)
        except Exception:
            pass
        log = actor.logger or logger()
        log.info('Bye from "%s"', actor)
        
class ActorProcess(ProcessMixin, Concurrency, Process):
    '''Actor on a Operative system process. Created using the
python multiprocessing module.'''
    def run(self):
        run_actor(self)


class TerminateActorThread(Exception):
    pass


class ActorThread(Concurrency, Thread):
    '''Actor on a thread in the master process.'''
    _actor = None
    
    def run(self):
        run_actor(self)
    
    def loop(self):
        if self._actor:
            return self._actor.event_loop
    
    def setup_event_loop(self, actor):
        '''Create the event loop but don't install signals.'''
        event_loop = new_event_loop(io=self.io_poller(), logger=actor.logger,
                                    poll_timeout=actor.params.poll_timeout)
        actor.mailbox = self.create_mailbox(actor, event_loop)


concurrency_models = {'arbiter': ArbiterConcurrency,
                      'monitor': MonitorConcurrency,
                      'thread': ActorThread,
                      'process': ActorProcess}
    