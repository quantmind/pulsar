import os
import sys
import asyncio
from time import time
from collections import OrderedDict

import pulsar
from .proxy import actor_proxy_future
from .actor import Actor
from .access import get_actor, set_actor, EventLoopPolicy
from .mailbox import create_aid
from .consts import ACTOR_STATES, MONITOR_TASK_PERIOD
from ..utils.exceptions import HaltServer
from ..utils.config import Config
from ..utils import system
from ..utils.log import logger_fds
from ..utils.tools import Pidfile
from ..utils import autoreload


concurrency_models = {}


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


class MonitorMixin:
    monitors = None
    registered = None
    managed_actors = None

    def identity(self, actor):
        return actor.name

    def is_monitor(self):
        return True

    def create_actor(self):
        self.managed_actors = {}
        return self.actor_class(self)

    def add_events(self, actor):
        actor.event('start').bind(_start_monitor)
        actor.event('on_info').bind(_info_monitor)
        actor.event('stopping').bind(_stop_monitor)

    def start(self):
        '''does nothing'''
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
            self._remove_monitored_actor(monitor, actor)
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
                self._remove_monitored_actor(monitor, actor)
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
                        w, kage = worker, age
                self.manage_actor(monitor, w, True)

    async def periodic_task(self, monitor, **kw):
        while not monitor.stopped():
            interval = 0
            if monitor.started():
                interval = MONITOR_TASK_PERIOD
                self.manage_actors(monitor)
                if monitor.is_running():
                    self.spawn_actors(monitor)
                    self.stop_actors(monitor)
                monitor.event('periodic_task').fire()
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break

    def _remove_monitored_actor(self, monitor, actor, log=True):
        if log and self.managed_actors.pop(actor.aid, None):
            monitor.logger.warning('Removed %s', actor)

    def _stop_actor(self, actor, finished=False):
        actor.state = ACTOR_STATES.CLOSE
        if actor.managed_actors:
            actor.state = ACTOR_STATES.TERMINATE
        _remove_monitor(actor)
        actor.logger.warning('Bye')

    def _register(self, monitor):   # pragma nocover
        raise HaltServer('Monitors cannot register monitor')


class ArbiterMixin(MonitorMixin):
    pid_file = None

    def is_arbiter(self):
        return True

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

    def create_actor(self):
        self.aid = self.name
        self.monitors = OrderedDict()
        self.registered = {}

        # Set asyncio event-loop policy
        cfg = self.cfg
        policy = EventLoopPolicy(cfg.event_loop, cfg.thread_workers,
                                 cfg.debug)
        asyncio.set_event_loop_policy(policy)

        if cfg.daemon:     # pragma    nocover
            # Daemonize the system
            if not cfg.pid_file:
                cfg.set('pid_file', 'pulsar.pid')
            system.daemonize(keep_fds=logger_fds())

        actor = super().create_actor()

        self.registered[self.identity(actor)] = actor
        return actor

    async def periodic_task(self, actor, **kw):
        """Override the :meth:`.Concurrency.periodic_task` to implement
        the :class:`.Arbiter` :ref:`periodic task <actor-periodic-task>`.
        """
        while True:
            interval = 0
            #
            if actor.started():
                # managed actors job
                self.manage_actors(actor)
                for m in list(self.monitors.values()):
                    _remove_monitor(m)

                interval = MONITOR_TASK_PERIOD
                #
                actor.event('periodic_task').fire()

            if actor.cfg.reload and autoreload.check_changes():
                # reload changes
                actor.stop(exit_code=autoreload.EXIT_CODE)

            if not actor.stopped():
                try:
                    await asyncio.sleep(interval)
                except asyncio.CancelledError:
                    break

    def _register(self, actor):
        aid = actor.identity
        self.registered[aid] = actor
        self.monitors[aid] = actor

    def _stop_actor(self, actor, finished=False):
        if finished:
            self._stop_arbiter(actor)
        else:
            actor._loop.create_task(self._exit_arbiter(actor))
            if not actor._loop.is_running():
                actor.logger.debug('Restarts event loop to stop actors')
                actor._run()

    async def _exit_arbiter(self, actor):
        await self._wait_stopping(actor)
        actor.state = ACTOR_STATES.CLOSE
        if actor.managed_actors:
            actor.state = ACTOR_STATES.TERMINATE
        await actor.mailbox.close()

    def _stop_arbiter(self, actor):  # pragma    nocover
        actor.stop_coverage()
        self._remove_signals(actor)
        p = self.pid_file
        if p is not None:
            actor.logger.debug('Removing %s' % p.fname)
            p.unlink()
            actor.pid_file = None
        if actor.managed_actors:
            actor.state = ACTOR_STATES.TERMINATE
        actor.exit_code = actor.exit_code or 0
        if actor.exit_code == autoreload.EXIT_CODE:
            actor.logger.info("Code changed, reloading server")
            actor._exit = True
        else:
            # actor.logger.info("Bye (exit code = %s)", exit_code)
            actor.stream.writeln(
                "\nBye (exit code = %s)" % actor.exit_code)
        try:
            actor.cfg.when_exit(actor)
        except Exception:
            pass
        if actor.exit_code and actor._exit:
            sys.exit(actor.exit_code)


# Monitor & arbiter internals

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


def _start_monitor(actor, **kw):
    if not actor.is_arbiter():
        return

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
        actor.pid_file = p


def _info_monitor(actor, data=None, **kw):
    if not actor.started():
        return

    server = data.pop('actor')

    if actor.monitors:
        monitors = {}

        for name, m in actor.monitors.items():
            info = m.info()
            if info:
                monitors[name] = info

        data['monitors'] = monitors
        server['number_of_monitors'] = len(monitors)

    if actor.managed_actors:
        server['number_of_actors'] = len(actor.managed_actors)
        data['workers'] = [a.info for a in actor.managed_actors.values()
                           if a.info]

    if actor.is_arbiter():
        server.update({'version': pulsar.__version__,
                       'python_version': sys.version,
                       'name': pulsar.SERVER_NAME})
        server.pop('is_process', None)
        server.pop('ppid', None)
        server.pop('actor_id', None)
        server.pop('age', None)

    data['server'] = server
    return data


def _stop_monitor(actor, **kw):
    waiters = actor.stopping_waiters

    if actor.monitors:
        actor.logger.debug('Closing %d monitors', len(actor.monitors))
        for m in tuple(actor.monitors.values()):
            stop = m.stop(exit_code=actor.exit_code)
            if stop:
                waiters.append(stop)

    if actor.managed_actors:
        actor.logger.debug('Closing %d actors', len(actor.managed_actors))
        sig = actor.exit_code
        for worker in actor.managed_actors.values():
            worker.stop(sig)

    waiters.append(_join_monitor(actor))


async def _join_monitor(actor):
    while True:
        if actor.concurrency.manage_actors(actor, True):
            await asyncio.sleep(0.05)
        else:
            periodic_task = actor.concurrency.running_periodic_task
            periodic_task.cancel()
            await periodic_task
            _remove_monitor(actor)
            break


def _remove_monitor(monitor):
    if monitor.stopped():
        arbiter = monitor.monitor
        arbiter.registered.pop(monitor.identity, None)
        arbiter.monitors.pop(monitor.identity, None)
