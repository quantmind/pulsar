import os
import sys
import itertools
import asyncio
import pickle
from time import time
from collections import OrderedDict
from multiprocessing import Process, current_process
from multiprocessing.reduction import ForkingPickler

import pulsar
from pulsar import system, MonitorStarted, HaltServer, Config
from pulsar.utils.log import logger_fds
from pulsar.utils.tools import Pidfile
from pulsar.utils import autoreload

from .proxy import ActorProxyMonitor, get_proxy, actor_proxy_future
from .access import get_actor, set_actor, logger, EventLoopPolicy
from .threads import Thread
from .mailbox import MailboxClient, MailboxProtocol, ProxyMailbox, create_aid
from .futures import ensure_future, add_errback, chain_future, create_future
from .protocols import TcpServer
from .actor import Actor
from .consts import (ACTOR_STATES, ACTOR_TIMEOUT_TOLE, MIN_NOTIFY, MAX_NOTIFY,
                     MONITOR_TASK_PERIOD)
from .process import ProcessMixin, signal_from_exitcode

__all__ = ['arbiter']


SUBPROCESS = os.path.join(os.path.dirname(__file__), '_subprocess.py')


def arbiter(**params):
    '''Obtain the ``arbiter``.

    It returns the arbiter instance only if we are on the arbiter
    context domain, otherwise it returns nothing.
    '''
    arbiter = get_actor()
    if arbiter is None:
        # Create the arbiter
        return set_actor(_spawn_actor('arbiter', None, **params))
    elif arbiter.is_arbiter():
        return arbiter


class Concurrency:
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
    monitors = None
    managed_actors = None
    registered = None
    actor_class = Actor

    @classmethod
    def make(cls, kind, cfg, name, aid, **kw):
        self = cls()
        self.aid = aid
        self.age = next(_actor_counter)
        self.name = name or self.actor_class.__name__.lower()
        self.kind = kind
        self.cfg = cfg
        self.params = kw
        return self.create_actor()

    def identity(self, actor):
        return actor.aid

    @property
    def unique_name(self):
        return '%s.%s' % (self.name, self.aid)

    def __repr__(self):
        return self.unique_name
    __str__ = __repr__

    def before_start(self, actor):
        pass

    def is_process(self):
        return False

    def is_arbiter(self):
        return False

    def is_monitor(self):
        return False

    def get_actor(self, actor, aid, check_monitor=True):
        if aid == actor.aid:
            return actor
        elif aid == 'monitor':
            return actor.monitor

    def spawn(self, actor, aid=None, **params):
        '''Spawn a new actor from ``actor``.
        '''
        aid = aid or create_aid()
        future = actor.send('arbiter', 'spawn', aid=aid, **params)
        return actor_proxy_future(aid, future)

    def run_actor(self, actor):
        '''Start running the ``actor``.
        '''
        set_actor(actor)
        if not actor.mailbox.address:
            ensure_future(actor.mailbox.start_serving(), loop=actor._loop)
        actor._loop.run_forever()

    def add_monitor(self, actor, monitor_name, **params):
        raise RuntimeError('Cannot add monitors to %s' % actor)

    def setup_event_loop(self, actor):
        '''Set up the event loop for ``actor``.
        '''
        actor._logger = self.cfg.configured_logger('pulsar.%s' % actor.name)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            if self.cfg and self.cfg.concurrency == 'thread':
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            else:
                raise
        if not hasattr(loop, 'logger'):
            loop.logger = actor._logger
        actor.mailbox = self.create_mailbox(actor, loop)
        return loop

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
            actor.bind_event('start', self._switch_to_run)
            actor.bind_event('start', self.periodic_task)
            actor.bind_event('start', self._acknowledge_start)
            actor.fire_event('start')
        except Exception as exc:
            actor.stop(exc)

    def create_actor(self):
        self.daemon = True
        self.params['monitor'] = get_proxy(self.params['monitor'])
        return ActorProxyMonitor(self)

    def create_mailbox(self, actor, loop):
        '''Create the mailbox for ``actor``.'''
        client = MailboxClient(actor.monitor.address, actor, loop)
        loop.call_soon_threadsafe(self.hand_shake, actor)
        return client

    def periodic_task(self, actor, **kw):
        '''Implement the :ref:`actor period task <actor-periodic-task>`.

        This is an internal method called periodically by the
        :attr:`.Actor._loop` to ping the actor monitor.
        If successful return a :class:`~asyncio.Future` called
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
            actor.fire_event('periodic_task')
            next = max(ACTOR_TIMEOUT_TOLE*actor.cfg.timeout, MIN_NOTIFY)
        else:
            next = 0
        actor.next_periodic_task = actor._loop.call_later(
            min(next, MAX_NOTIFY), self.periodic_task, actor)
        return ack

    def stop(self, actor, exc=None, exit_code=None):
        '''Gracefully stop the ``actor``.
        '''
        if actor.state <= ACTOR_STATES.RUN:
            # The actor has not started the stopping process. Starts it now.
            actor.state = ACTOR_STATES.STOPPING
            actor.event('start').clear()
            if exc:
                if not exit_code:
                    exit_code = getattr(exc, 'exit_code', 1)
                if exit_code == 1:
                    exc_info = sys.exc_info()
                    if exc_info[0] is not None:
                        actor.logger.critical('Stopping', exc_info=exc_info)
                    else:
                        actor.logger.critical('Stopping: %s', exc)
                elif exit_code == 2:
                    actor.logger.error(str(exc))
                elif exit_code:
                    actor.stream.writeln(str(exc))
            else:
                if not exit_code:
                    exit_code = getattr(actor._loop, 'exit_code', 0)
            #
            actor.exit_code = exit_code
            stopping = actor.fire_event('stopping')
            if not stopping.done() and actor._loop.is_running():
                actor.logger.debug('asynchronous stopping')

                def cbk(_):
                    return self._stop_actor(actor)

                return chain_future(stopping, callback=cbk, errback=cbk)
            else:
                if actor.logger:
                    actor.logger.debug('stopping')
                return self._stop_actor(actor)
        elif actor.stopped():
            return self._stop_actor(actor, True)

    def _remove_signals(self, actor):
        pass

    def _stop_actor(self, actor, finished=False):
        # Stop the actor if finished is True
        # otherwise starts the stopping process
        if finished:
            self._remove_signals(actor)
            return True
        #
        if actor._loop.is_running():
            actor.logger.debug('Closing mailbox')
            actor.mailbox.close()
        else:
            actor.state = ACTOR_STATES.CLOSE
            actor.logger.debug('Exiting actor with exit code 1')
            actor.exit_code = 1
            actor.mailbox.abort()
            return actor.stop()

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

    def _remove_actor(self, monitor, actor, log=True):
        raise RuntimeError('Cannot remove actor')


class MonitorMixin:

    def identity(self, actor):
        return actor.name

    def create_actor(self):
        self.managed_actors = {}
        actor = self.actor_class(self)
        actor.bind_event('on_info', self._info_monitor)
        return actor

    def start(self):
        '''does nothing,'''
        pass

    def get_actor(self, actor, aid, check_monitor=True):
        # Delegate get_actor to the arbiter
        if aid == actor.aid:
            return actor
        elif aid == 'monitor':
            return actor.monitor or actor
        elif aid in self.managed_actors:
            return self.managed_actors[aid]
        elif actor.monitor and check_monitor:
            return actor.monitor.get_actor(aid)

    def spawn(self, monitor, kind=None, **params):
        '''Spawn a new :class:`Actor` and return its
        :class:`.ActorProxyMonitor`.
        '''
        proxy = _spawn_actor(kind, monitor, **params)
        # Add to the list of managed actors if this is a remote actor
        if isinstance(proxy, Actor):
            self._register(proxy)
            return proxy
        else:
            proxy.monitor = monitor
            self.managed_actors[proxy.aid] = proxy
            future = actor_proxy_future(proxy)
            proxy.start()
            return future

    def manage_actors(self, monitor, stop=False):
        '''Remove :class:`Actor` which are not alive from the
        :class:`PoolMixin.managed_actors` and return the number of actors
        still alive.

        :parameter stop: if ``True`` stops all alive actor.
        '''
        alive = 0
        if self.managed_actors:
            for aid, actor in list(self.managed_actors.items()):
                alive += self.manage_actor(monitor, actor, stop)
        return alive

    def manage_actor(self, monitor, actor, stop=False):
        '''If an actor failed to notify itself to the arbiter for more than
        the timeout, stop the actor.

        :param actor: the :class:`Actor` to manage.
        :param stop: if ``True``, stop the actor.
        :return: if the actor is alive 0 if it is not.
        '''
        if not monitor.is_running():
            stop = True
        if not actor.is_alive():
            if not actor.should_be_alive() and not stop:
                return 1
            actor.join()
            monitor._remove_actor(actor)
            return 0
        timeout = None
        started_stopping = bool(actor.stopping_start)
        # if started_stopping is True, set stop to True
        stop = stop or started_stopping
        if not stop and actor.notified:
            gap = time() - actor.notified
            stop = timeout = gap > actor.cfg.timeout
        if stop:   # we are stopping the actor
            dt = actor.should_terminate()
            if not actor.mailbox or dt:
                if not actor.mailbox:
                    monitor.logger.warning('kill %s - no mailbox.', actor)
                else:
                    monitor.logger.warning('kill %s - could not stop '
                                           'after %.2f seconds.', actor, dt)
                actor.kill()
                self._remove_actor(monitor, actor)
                return 0
            elif not started_stopping:
                if timeout:
                    monitor.logger.warning('Stopping %s. Timeout %.2f',
                                           actor, timeout)
                else:
                    monitor.logger.info('Stopping %s.', actor)
                actor.stop()
        return 1

    def spawn_actors(self, monitor):
        '''Spawn new actors if needed.
        '''
        to_spawn = monitor.cfg.workers - len(self.managed_actors)
        if monitor.cfg.workers and to_spawn > 0:
            for _ in range(to_spawn):
                monitor.spawn()

    def stop_actors(self, monitor):
        """Maintain the number of workers by spawning or killing as required
        """
        if monitor.cfg.workers:
            num_to_kill = len(self.managed_actors) - monitor.cfg.workers
            for i in range(num_to_kill, 0, -1):
                w, kage = 0, sys.maxsize
                for worker in self.managed_actors.values():
                    age = worker.impl.age
                    if age < kage:
                        w, kage = w, age
                self.manage_actor(monitor, w, True)

    def _close_actors(self, monitor):
        #
        # Close all managed actors at once and wait for completion
        sig = signal_from_exitcode(monitor.exit_code)
        for worker in self.managed_actors.values():
            worker.stop(sig)

        waiter = create_future(monitor._loop)

        def _check(_, **kw):
            if not self.managed_actors:
                monitor.remove_callback('periodic_task', _check)
                waiter.set_result(None)

        monitor.bind_event('periodic_task', _check)
        return waiter

    def _remove_actor(self, monitor, actor, log=True):
        removed = self.managed_actors.pop(actor.aid, None)
        if log and removed:
            log = False
            monitor.logger.warning('Removed %s', actor)
        if monitor.monitor:
            monitor.monitor._remove_actor(actor, log)
        return removed

    def _info_monitor(self, actor, info=None):
        if actor.started():
            info['actor'].update({'concurrency': actor.cfg.concurrency,
                                  'workers': len(self.managed_actors)})
            info['workers'] = [a.info for a in self.managed_actors.values()
                               if a.info]
        return info

    def _register(self, arbiter):
        raise HaltServer('Critical error')


############################################################################
#    CONCURRENCY IMPLEMENTATIONS
class MonitorConcurrency(MonitorMixin, Concurrency):
    ''':class:`.Concurrency` class for a :class:`.Monitor`.

    Monitors live in the **main thread** of the master process and
    therefore do not require to be spawned.
    '''
    def is_monitor(self):
        return True

    def setup_event_loop(self, actor):
        actor._logger = self.cfg.configured_logger('pulsar.%s' % actor.name)
        actor.mailbox = ProxyMailbox(actor)
        loop = actor.mailbox._loop
        loop.call_soon(actor.start)
        return loop

    def run_actor(self, actor):
        actor._loop.call_soon(self.hand_shake, actor)
        raise MonitorStarted

    def create_mailbox(self, actor, loop):
        raise NotImplementedError

    def periodic_task(self, monitor, **kw):
        '''Override the :meth:`.Concurrency.periodic_task` to implement
        the :class:`.Monitor` :ref:`periodic task <actor-periodic-task>`.'''
        interval = 0
        monitor.next_periodic_task = None
        if monitor.started():
            interval = MONITOR_TASK_PERIOD
            self.manage_actors(monitor)
            #
            if monitor.is_running():
                self.spawn_actors(monitor)
                self.stop_actors(monitor)
            elif monitor.cfg.debug:
                monitor.logger.debug('still stopping')
            #
            monitor.fire_event('periodic_task')
        #
        if not monitor.closed():
            monitor.next_periodic_task = monitor._loop.call_later(
                interval, self.periodic_task, monitor)

    def _stop_actor(self, actor, finished=False):
        # remove all workers from this monitor
        if finished:
            return

        def _cleanup(_):
            if actor.cfg.debug:
                actor.logger.debug('monitor is now stopping')
            actor.state = ACTOR_STATES.CLOSE
            if actor.next_periodic_task:
                actor.next_periodic_task.cancel()
            self.stop(actor)

        return chain_future(self._close_actors(actor), callback=_cleanup,
                            errback=_cleanup)


class ArbiterConcurrency(MonitorMixin, ProcessMixin, Concurrency):
    '''Concurrency implementation for the ``arbiter``
    '''
    pid_file = None

    def is_arbiter(self):
        return True

    def create_actor(self):
        cfg = self.cfg
        policy = EventLoopPolicy(cfg.event_loop, cfg.thread_workers,
                                 cfg.debug)
        asyncio.set_event_loop_policy(policy)
        if cfg.daemon:     # pragma    nocover
            # Daemonize the system
            if not cfg.pid_file:
                cfg.set('pid_file', 'pulsar.pid')
            system.daemonize(keep_fds=logger_fds())
        self.aid = self.name
        actor = super().create_actor()
        self.monitors = OrderedDict()
        self.registered = {self.identity(actor): actor}
        actor.bind_event('start', self._start_arbiter)
        return actor

    def get_actor(self, actor, aid, check_monitor=True):
        '''Given an actor unique id return the actor proxy.'''
        a = super().get_actor(actor, aid)
        if a is None:
            if aid in self.monitors:  # Check in monitors aid
                return self.monitors[aid]
            elif aid in self.managed_actors:
                return self.managed_actors[aid]
            elif aid in self.registered:
                return self.registered[aid]
            else:  # Finally check in workers in monitors
                for m in self.monitors.values():
                    a = m.get_actor(aid, check_monitor=False)
                    if a is not None:
                        return a
        else:
            return a

    def add_monitor(self, actor, monitor_name, **params):
        '''Add a new ``monitor``.

        :param monitor_class: a :class:`.Monitor` class.
        :param monitor_name: a unique name for the monitor.
        :param kwargs: dictionary of key-valued parameters for the monitor.
        :return: the :class:`.Monitor` added.
        '''
        if monitor_name in self.registered:
            raise KeyError('Monitor "%s" already available' % monitor_name)
        params.update(actor.actorparams())
        params['name'] = monitor_name
        params['kind'] = 'monitor'
        return actor.spawn(**params)

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
        #
        if actor.started():
            # managed actors job
            self.manage_actors(actor)
            for m in list(self.monitors.values()):
                if m.closed():
                    actor._remove_actor(m)

            interval = MONITOR_TASK_PERIOD
            if not actor.is_running() and actor.cfg.debug:
                actor.logger.debug('still stopping')
            #
            actor.fire_event('periodic_task')

        if not actor.closed():
            actor.next_periodic_task = actor._loop.call_later(
                interval, self.periodic_task, actor)

        if actor.cfg.reload and autoreload.check_changes():
            # reload changes
            actor.stop(exit_code=autoreload.EXIT_CODE)

    def _stop_actor(self, actor, finished=False):
        # remove all actors and monitors
        if finished:
            self._stop_arbiter(actor)
        elif actor._loop.is_running():
            self._exit_arbiter(actor)
        else:
            actor.logger.debug('Restarts event loop to stop actors')
            actor._loop.call_soon(self._exit_arbiter, actor)
            actor._run(False)

    def _exit_arbiter(self, actor, done=False):
        if done:
            actor.logger.debug('Closing mailbox server')
            actor.state = ACTOR_STATES.CLOSE
            actor._loop.create_task(actor.mailbox.close())
        else:
            monitors = len(self.monitors)
            managed = len(self.managed_actors)
            if monitors or managed:
                actor.logger.debug('Closing %d monitors and %d actors',
                                   monitors, managed)
                ensure_future(self._close_all(actor))
            else:
                self._exit_arbiter(actor, True)

    async def _close_all(self, actor):
        # Close monitors and actors
        try:
            waiters = []
            for m in tuple(self.monitors.values()):
                stop = m.stop(exit_code=actor.exit_code)
                if stop:
                    waiters.append(stop)
            waiters.append(self._close_actors(actor))
            await asyncio.gather(*waiters)
        except Exception:
            actor.logger.exception('Exception while closing arbiter')
        self._exit_arbiter(actor, True)

    def _remove_actor(self, arbiter, actor, log=True):
        a = super()._remove_actor(arbiter, actor, False)
        b = self.registered.pop(self.identity(actor), None)
        c = self.monitors.pop(self.identity(actor), None)
        removed = a or b or c
        if removed and log:
            arbiter.logger.warning('Removed %s', actor)
        return removed

    def _stop_arbiter(self, actor):     # pragma    nocover
        actor.stop_coverage()
        self._remove_signals(actor)
        p = self.pid_file
        if p is not None:
            actor.logger.debug('Removing %s' % p.fname)
            p.unlink()
            self.pid_file = None
        if self.managed_actors:
            actor.state = ACTOR_STATES.TERMINATE
        actor.exit_code = actor.exit_code or 0
        if actor.exit_code == autoreload.EXIT_CODE:
            actor.logger.info("Code changed, reloading server")
            actor._exit = True
        else:
            # actor.logger.info("Bye (exit code = %s)", exit_code)
            actor.stream.writeln("\nBye (exit code = %s)" % actor.exit_code)
        try:
            actor.cfg.when_exit(actor)
        except Exception:
            pass
        if actor.exit_code and actor._exit:
            sys.exit(actor.exit_code)

    def _start_arbiter(self, actor, exc=None):
        if not os.environ.get('SERVER_SOFTWARE'):
            os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        pid_file = actor.cfg.pid_file
        if pid_file is not None:
            actor.logger.info('Create pid file %s', pid_file)
            try:
                p = Pidfile(pid_file)
                p.create(actor.pid)
            except RuntimeError as e:
                raise HaltServer('ERROR. %s' % str(e), exit_code=2)
            self.pid_file = p

    def _info_monitor(self, actor, info=None):
        data = info
        monitors = {}
        for m in self.monitors.values():
            info = m.info()
            if info:
                actor = info['actor']
                monitors[actor['name']] = info
        server = data.pop('actor')
        server.update({'version': pulsar.__version__,
                       'python_version': sys.version,
                       'name': pulsar.SERVER_NAME,
                       'number_of_monitors': len(self.monitors),
                       'number_of_actors': len(self.managed_actors)})
        server.pop('is_process', None)
        server.pop('ppid', None)
        server.pop('actor_id', None)
        server.pop('age', None)
        data['server'] = server
        data['workers'] = [a.info for a in self.managed_actors.values()]
        data['monitors'] = monitors
        return data

    def _register(self, actor):
        aid = self.identity(actor)
        self.registered[aid] = actor
        self.monitors[aid] = actor


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
        actor.stop_coverage()
        log.info('Bye from "%s"', actor)


class ActorThread(Concurrency, Thread):
    '''Actor on a thread of the master process
    '''
    _actor = None

    def before_start(self, actor):
        self.set_loop(actor._loop)

    def run(self):
        run_actor(self)


class ActorMultiProcess(ProcessMixin, Concurrency, Process):
    '''Actor on a Operative system process.
    Created using the python multiprocessing module.
    '''
    def run(self):  # pragma    nocover
        # The coverage for this process has not yet started
        run_actor(self)

    def kill(self, sig):
        system.kill(self.pid, sig)


class ActorProcess(ProcessMixin, Concurrency):
    '''Actor on a Operative system process.
    '''
    process = None

    def start(self):
        loop = asyncio.get_event_loop()
        return ensure_future(self._start(loop), loop=loop)

    async def _start(self, loop):
        import inspect

        data = {
            'path': sys.path.copy(),
            'impl': bytes(ForkingPickler.dumps(self)),
            'main': inspect.getfile(sys.modules['__main__']),
            'authkey': bytes(current_process().authkey)
        }

        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            SUBPROCESS,
            stdin=asyncio.subprocess.PIPE,
            loop=loop
        )
        await self.process.communicate(pickle.dumps(data))

    def is_alive(self):
        if self.process:
            code = self.process.returncode
            return code is None
        return False

    def join(self, timeout=None):
        pass

    def kill(self, sig):
        self.process.send_signal(sig)


class ActorCoroutine(Concurrency):

    def start(self):
        run_actor(self)

    def kill(self, sig):
        self._actor.exit_code = int(sig)

    def is_alive(self):
        actor = self._actor
        return actor.exit_code is None and actor._loop.is_running()

    def join(self, timeout=None):
        pass

    def run_actor(self, actor):
        raise MonitorStarted


concurrency_models = {
    'arbiter': ArbiterConcurrency,
    'monitor': MonitorConcurrency,
    'coroutine': ActorCoroutine,
    'thread': ActorThread,
    'process': ActorProcess,
    'multi': ActorMultiProcess
}


def _spawn_actor(kind, monitor, cfg=None, name=None, aid=None, **kw):
    # Internal function which spawns a new Actor and return its
    # ActorProxyMonitor.
    # *cls* is the Actor class
    # *monitor* can be either the arbiter or a monitor
    if monitor:
        params = monitor.actorparams()
        name = params.pop('name', name)
        aid = params.pop('aid', aid)
        cfg = params.pop('cfg', cfg)

    # get config if not available
    if cfg is None:
        if monitor:
            cfg = monitor.cfg.copy()
        else:
            cfg = Config()

    if not aid:
        aid = create_aid()

    if not monitor:  # monitor not available, this is the arbiter
        assert kind == 'arbiter'
        name = kind
        params = {}
        if not cfg.exc_id:
            cfg.set('exc_id', aid)
    #
    for key, value in kw.items():
        if key in cfg.settings:
            cfg.set(key, value)
        else:
            params[key] = value
    #
    if monitor:
        kind = kind or cfg.concurrency
    if not kind:
        raise TypeError('Cannot spawn')

    model = concurrency_models.get(kind)
    if model:
        return model.make(kind, cfg, name, aid, monitor=monitor, **params)
    else:
        raise ValueError('Concurrency %s not supported in pulsar' % kind)


_actor_counter = itertools.count(1)
