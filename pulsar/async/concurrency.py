import sys
from functools import partial
from multiprocessing import Process, current_process

from pulsar import system, HaltServer, MonitorStarted
from pulsar.utils.security import gen_unique_id
from pulsar.utils.pep import itervalues

from .proxy import ActorProxyMonitor, get_proxy
from .access import get_actor, set_actor, logger, _StopError, SELECTORS
from .threads import Thread
from .mailbox import MailboxClient, MailboxProtocol, ProxyMailbox
from .futures import multi_async, Future, add_errback
from .eventloop import EventLoop
from .protocols import TcpServer
from .consts import *


if sys.platform == 'win32':     # pragma    nocover
    signal = None
else:
    import signal


__all__ = ['Concurrency', 'concurrency']


def concurrency(kind, actor_class, monitor, cfg, **params):
    '''Function invoked by the :class:`.Arbiter` or a :class:`.Monitor` when
spawning a new :class:`.Actor`. It created a :class:`.Concurrency` instance
which handle the initialisation and the life of an :class:`.Actor`.

:parameter kind: Type of concurrency.
:parameter monitor: The monitor (or arbiter) managing the :class:`.Actor`.
:return: a :class:`.Councurrency` instance.
'''
    maker = concurrency_models.get(kind)
    if maker:
        c = maker()
        return c.make(kind, actor_class, monitor, cfg, **params)
    else:
        raise ValueError('Concurrency %s not supported in pulsar' % kind)


class Concurrency(object):
    '''Actor :class:`.Concurrency`.

    Responsible for the actual spawning of actors according to a
    concurrency implementation. Instances are picklable
    and are shared between the :class:`.Actor` and its
    :class:`.ActorProxyMonitor`.
    This is an abstract class, derived classes must implement the
    ``start`` method.

    :param concurrency: string indicating the concurrency implementation.
        Valid choices are ``monitor``, ``process``, ``thread``.
    :param actor_class: :class:`.Actor` or one of its subclasses.
    :param timeout: timeout in seconds for the actor.
    :param kwargs: additional key-valued arguments to be passed to the actor
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

    def selector(self):
        '''Return a selector instance.

        By default it return nothing so that the best handler for the
        system is chosen.
        '''
        return SELECTORS[self.cfg.selector]()

    def run_actor(self, actor):
        '''Start running the ``actor``.
        '''
        set_actor(actor)
        actor.mailbox.start_serving()
        actor._loop.run_forever()

    def setup_event_loop(self, actor):
        '''Set up the event loop for ``actor``.
        '''
        actor._logger = self.cfg.configured_logger(actor.name)
        loop = EventLoop(self.selector(), logger=actor._logger,
                         iothreadloop=True, cfg=actor.cfg)
        actor.mailbox = self.create_mailbox(actor, loop)

    def hand_shake(self, actor):
        '''Perform the hand shake for ``actor``

        The hand shake occurs when the ``actor`` is in starting state.
        It performs the following actions:

        * set the ``actor`` as the actor of the current thread
        * bind two additional callbacks to the ``start`` event
        * fire the ``start`` event

        If the hand shake is successful, the actor will eventually
        results in a running state.
        '''
        try:
            assert actor.state == ACTOR_STATES.STARTING
            if actor.cfg.debug:
                actor.logger.debug('starting handshake')
            a = get_actor()
            if a is not actor and a is not actor.monitor:
                set_actor(actor)
            actor.bind_event('start', self._switch_to_run)
            actor.bind_event('start', self.periodic_task)
            actor.bind_event('start', self._acknowledge_start)
            actor.fire_event('start')
        except Exception as exc:
            actor.stop(exc)

    def get_actor(self):
        self.daemon = False
        self.params['monitor'] = get_proxy(self.params['monitor'])
        # make sure these parameters are picklable
        # pickle.dumps(self.params)
        return ActorProxyMonitor(self)

    def create_mailbox(self, actor, loop):
        '''Create the mailbox for ``actor``.'''
        client = MailboxClient(actor.monitor.address, actor, loop)
        loop.call_soon_threadsafe(self.hand_shake, actor)
        client.bind_event('finish', lambda _, **kw: loop.stop())
        return client

    def _install_signals(self, actor):
        proc_name = "%s-%s" % (actor.cfg.proc_name, actor)
        if system.set_proctitle(proc_name):
            actor.logger.debug('Set process title to %s', proc_name)
        system.set_owner_process(actor.cfg.uid, actor.cfg.gid)
        if signal:
            actor.logger.debug('Installing signals')
            for sig in system.EXIT_SIGNALS:
                try:
                    actor._loop.add_signal_handler(
                        sig, self.handle_exit_signal, actor, sig)
                except ValueError:
                    pass

    def periodic_task(self, actor, **kw):
        '''Implement the :ref:`actor period task <actor-periodic-task>`.

        This is an internal method called periodically by the
        :attr:`.Actor._loop` to ping the actor monitor.
        If successful return a :class:`asyncio.Future` called
        back with the acknowledgement from the monitor.
        '''
        actor.next_periodic_task = None
        ack = None
        if actor.is_running():
            if actor.cfg.debug:
                actor.logger.debug('notify monitor')
            # if an error occurs, shut down the actor
            ack = actor.send('monitor', 'notify', actor.info())
            add_errback(ack, actor.stop)
            next = max(ACTOR_TIMEOUT_TOLE*actor.cfg.timeout, MIN_NOTIFY)
        else:
            next = 0
        actor.next_periodic_task = actor._loop.call_later(
            min(next, MAX_NOTIFY), self.periodic_task, actor)
        return ack

    def stop(self, actor, exc):
        '''Gracefully stop the ``actor``.
        '''
        if actor.state <= ACTOR_STATES.RUN:
            # The actor has not started the stopping process. Starts it now.
            actor.state = ACTOR_STATES.STOPPING
            actor.event('start').clear()
            if exc:
                actor.exit_code = getattr(exc, 'exit_code', 1)
                if actor.exit_code == 1:
                    actor.logger.critical('Stopping', exc_info=True)
                elif actor.exit_code:
                    actor.stream.writeln(str(exc))
                else:
                    actor.logger.info('Stopping')
            else:
                if actor.logger:
                    actor.logger.debug('stopping')
                actor.exit_code = 0
            stopping = actor.fire_event('stopping')
            actor.close_executor()
            if not stopping.done() and actor._loop.is_running():
                actor.logger.debug('async stopping')
                stopping.add_done_callback(lambda _: self._stop_actor(actor))
            else:
                self._stop_actor(actor)
        elif actor.stopped():
            # The actor has finished the stopping process.
            actor.fire_event('stop')
        return actor.event('stop')

    def _stop_actor(self, actor):
        '''Exit from the :class:`.Actor` domain.'''
        actor.state = ACTOR_STATES.CLOSE
        if actor._loop.is_running():
            actor.logger.debug('Closing mailbox')
            actor.mailbox.close()
        else:
            actor.exit_code = 1
            actor.mailbox.abort()
            self.stop(actor, 0)

    def _switch_to_run(self, actor, exc=None):
        if exc is None and actor.state < ACTOR_STATES.RUN:
            actor.state = ACTOR_STATES.RUN
        elif exc:
            actor.stop(exc)

    def _acknowledge_start(self, actor, exc=None):
        if exc is None:
            actor.logger.info('started')
        else:
            actor.stop(exc)


class ProcessMixin(object):

    def is_process(self):
        return True

    def before_start(self, actor):  # pragma    nocover
        actor.start_coverage()
        self._install_signals(actor)

    def handle_exit_signal(self, actor, sig):
        actor.logger.warning("Got %s. Stopping.", system.SIG_NAMES.get(sig))
        actor._loop.exit_signal = sig
        raise _StopError


class MonitorMixin(object):

    def get_actor(self):
        return self.actor_class(self)

    def start(self):
        '''does nothing'''
        pass

    def is_active(self):
        return self.actor.is_alive()

    @property
    def pid(self):
        return current_process().pid


############################################################################
#    CONCURRENCY IMPLEMENTATIONS
class MonitorConcurrency(MonitorMixin, Concurrency):
    ''':class:`.Concurrency` class for a :class:`.Monitor`.

    Monitors live in the **main thread** of the master process and
    therefore do not require to be spawned.
    '''
    def setup_event_loop(self, actor):
        actor._logger = self.cfg.configured_logger(actor.name)
        actor.mailbox = ProxyMailbox(actor)
        actor.mailbox._loop.call_soon(actor.start)

    def run_actor(self, actor):
        actor._loop.call_soon(self.hand_shake, actor)
        raise MonitorStarted

    def create_mailbox(self, actor, loop):
        raise NotImplementedError

    def periodic_task(self, actor, **kw):
        '''Override the :meth:`.Concurrency.periodic_task` to implement
        the :class:`.Monitor` :ref:`periodic task <actor-periodic-task>`.'''
        interval = 0
        actor.next_periodic_task = None
        if actor.is_running():
            interval = MONITOR_TASK_PERIOD
            actor.manage_actors()
            actor.spawn_actors()
            actor.stop_actors()
            actor.monitor_task()
        actor.next_periodic_task = actor._loop.call_later(
            interval, self.periodic_task, actor)

    def _stop_actor(self, actor):

        def _cleanup(_):
            if not actor.terminated_actors:
                actor.monitor._remove_actor(actor)
            actor.fire_event('stop')

        return actor.close_actors().add_done_callback(_cleanup)


class ArbiterConcurrency(MonitorMixin, ProcessMixin, Concurrency):
    '''Concurrency implementation for the :class:`.Arbiter`
    '''
    def is_arbiter(self):
        return True

    def before_start(self, actor):  # pragma    nocover
        '''Daemonise the system if required.
        '''
        if actor.cfg.daemon:
            system.daemonize()
        actor.start_coverage()
        self._install_signals(actor)

    def create_mailbox(self, actor, loop):
        '''Override :meth:`.Concurrency.create_mailbox` to create the
        mailbox server.
        '''
        mailbox = TcpServer(MailboxProtocol, loop, ('127.0.0.1', 0),
                            name='mailbox')
        # when the mailbox stop, close the event loop too
        mailbox.bind_event('stop', lambda _, **kw: loop.stop())
        mailbox.bind_event(
            'start',
            lambda _, **kw: loop.call_soon(self.hand_shake, actor))
        return mailbox

    def periodic_task(self, actor, **kw):
        '''Override the :meth:`.Concurrency.periodic_task` to implement
        the :class:`.Arbiter` :ref:`periodic task <actor-periodic-task>`.'''
        interval = 0
        actor.next_periodic_task = None
        if actor.is_running():
            # managed actors job
            interval = MONITOR_TASK_PERIOD
            actor.manage_actors()
            for m in list(itervalues(actor.monitors)):
                if m.started() and not m.is_running():
                    actor._remove_actor(m)
        actor.next_periodic_task = actor._loop.call_later(
            interval, self.periodic_task, actor)

    def _stop_actor(self, actor):
        '''Stop the pools the message queue and remaining actors.'''
        if actor._loop.is_running():
            self._exit_arbiter(actor)
        else:
            actor.logger.debug('Restarts event loop to stop actors')
            loop = actor._loop
            actor._loop.call_soon(self._exit_arbiter, actor)
            actor._run(False)

    def _exit_arbiter(self, actor, fut=None):
        if fut:
            actor.logger.debug('Closing mailbox server')
            actor.state = ACTOR_STATES.CLOSE
            actor.mailbox.close()
        else:
            actor.logger.debug('Close monitors and actors')
            active = multi_async((actor.close_monitors(),
                                  actor.close_actors()))
            active.add_done_callback(partial(self._exit_arbiter, actor))


def run_actor(self):
    self._actor = actor = self.actor_class(self)
    try:
        actor.start()
    finally:
        try:
            actor.cfg.when_exit(actor)
        except Exception:   # pragma    nocover
            pass
        log = actor.logger or logger()
        self.stop_coverage(actor)
        log.info('Bye from "%s"', actor)


class ActorProcess(ProcessMixin, Concurrency, Process):
    '''Actor on a Operative system process.

    Created using the python multiprocessing module.
    '''
    def run(self):  # pragma    nocover
        # The coverage for this process has not yet started
        run_actor(self)

    def stop_coverage(self, actor):
        actor.stop_coverage()


class TerminateActorThread(Exception):
    pass


class ActorThread(Concurrency, Thread):
    '''Actor on a thread in the master process.'''
    _actor = None

    def run(self):
        run_actor(self)

    def stop_coverage(self, actor):
        pass

    def loop(self):
        if self._actor:
            return self._actor._loop


concurrency_models = {'arbiter': ArbiterConcurrency,
                      'monitor': MonitorConcurrency,
                      'thread': ActorThread,
                      'process': ActorProcess}
