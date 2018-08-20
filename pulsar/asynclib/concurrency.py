import os
import sys
import itertools
import asyncio
import pickle
from multiprocessing import Process, current_process
from multiprocessing.reduction import ForkingPickler

from ..utils.exceptions import ActorStarted
from ..utils import system

from .proxy import ActorProxyMonitor, get_proxy, actor_proxy_future
from .access import set_actor, logger
from .threads import Thread
from .timeout import timeout
from .mailbox import MailboxClient, mailbox_protocol, ProxyMailbox, create_aid
from .protocols import TcpServer
from .actor import Actor
from .consts import ACTOR_STATES, ACTOR_TIMEOUT_TOLE, MIN_NOTIFY, MAX_NOTIFY
from .process import ProcessMixin
from .monitor import MonitorMixin, ArbiterMixin, concurrency_models


SUBPROCESS = os.path.join(os.path.dirname(__file__), '_subprocess.py')


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
    actor_class = Actor
    running_periodic_task = None

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

    def add_events(self, actor):
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
            address = ('127.0.0.1', 0)
            actor._loop.create_task(
                actor.mailbox.start_serving(address=address)
            )
        actor._loop.run_forever()

    def add_monitor(self, actor, monitor_name, **params):
        raise RuntimeError('Cannot add monitors to %s' % actor)

    def setup_event_loop(self, actor):
        '''Set up the event loop for ``actor``.
        '''
        actor.logger = self.cfg.configured_logger('pulsar.%s' % actor.name)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            if self.cfg and self.cfg.concurrency == 'thread':
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            else:
                raise
        if not hasattr(loop, 'logger'):
            loop.logger = actor.logger
        actor.mailbox = self.create_mailbox(actor, loop)
        return loop

    def hand_shake(self, actor, run=True):
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
            actor.event('start').fire()
            if run:
                self._switch_to_run(actor)
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

    async def periodic_task(self, actor, **kw):
        while True:
            if actor.is_running():
                if actor.cfg.debug:
                    actor.logger.debug('notify monitor')
                # if an error occurs, shut down the actor
                try:
                    await actor.send('monitor', 'notify', actor.info())
                except Exception as exc:
                    actor.stop(exc=exc)
                actor.event('periodic_task').fire()
                next = max(ACTOR_TIMEOUT_TOLE*actor.cfg.timeout, MIN_NOTIFY)
            else:
                next = 0

            await asyncio.sleep(min(next, MAX_NOTIFY))

    def stop(self, actor, exc=None, exit_code=None):
        """Gracefully stop the ``actor``.
        """
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
            # Fire stopping event
            actor.exit_code = exit_code
            actor.stopping_waiters = []
            actor.event('stopping').fire()

            if actor.stopping_waiters and actor._loop.is_running():
                actor.logger.info('asynchronous stopping')
                # make sure to return the future (used by arbiter for waiting)
                return actor._loop.create_task(self._async_stopping(actor))
            else:
                if actor.logger:
                    actor.logger.info('stopping')
                self._stop_actor(actor)

        elif actor.stopped():
            return self._stop_actor(actor, True)

    def _remove_signals(self, actor):
        pass

    async def _async_stopping(self, actor):
        await self._wait_stopping(actor)
        self._stop_actor(actor)

    async def _wait_stopping(self, actor):
        waiters = actor.stopping_waiters
        if waiters:
            actor.stopping_waiters = None
            loop = actor._loop
            try:
                start = loop.time()
                with timeout(loop, self.cfg.exit_timeout):
                    await asyncio.gather(*waiters, return_exceptions=True,
                                         loop=loop)
            except asyncio.TimeoutError:
                actor.logger.warning('force stopping after %.2f' %
                                     (loop.time() - start))
            except Exception:
                actor.logger.exception('Critical exception while stopping')

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
            self.running_periodic_task = actor._loop.create_task(
                self.periodic_task(actor)
            )
        elif exc:
            actor.stop(exc)


############################################################################
#    CONCURRENCY IMPLEMENTATIONS
class MonitorConcurrency(MonitorMixin, Concurrency):
    ''':class:`.Concurrency` class for a :class:`.Monitor`.

    Monitors live in the **main thread** of the master process and
    therefore do not require to be spawned.
    '''
    def setup_event_loop(self, actor):
        actor.logger = self.cfg.configured_logger('pulsar.%s' % actor.name)
        actor.mailbox = ProxyMailbox(actor)
        loop = actor.mailbox._loop
        loop.call_soon(actor.start)
        return loop

    def run_actor(self, actor):
        if actor.start_event:
            actor.start_event.add_done_callback(
                lambda f: self._switch_to_run(actor)
            )
            self.hand_shake(actor, False)
        else:
            self.hand_shake(actor)
        raise ActorStarted

    def create_mailbox(self, actor, loop):
        raise NotImplementedError


class ArbiterConcurrency(ArbiterMixin, ProcessMixin, Concurrency):
    '''Concurrency implementation for the ``arbiter``
    '''
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

    def create_mailbox(self, actor, loop):
        '''Override :meth:`.Concurrency.create_mailbox` to create the
        mailbox server.
        '''
        mailbox = TcpServer(mailbox_protocol, loop=loop, name='mailbox')
        # when the mailbox stop, close the event loop too
        mailbox.event('stop').bind(lambda _, **kw: loop.stop())
        mailbox.event('start').bind(
            lambda _, **kw: loop.call_soon(self.hand_shake, actor)
        )
        return mailbox


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
        actor.logger.info('Booting')
        self.set_loop(actor._loop)

    def run(self):
        run_actor(self)


class ActorMultiProcess(ProcessMixin, Concurrency, Process):
    '''Actor on a Operative system process.
    Created using the python multiprocessing module.
    '''
    def run(self):  # pragma    nocover
        # The coverage for this process has not yet started
        try:
            from asyncio.events import _set_running_loop
            _set_running_loop(None)
        except ImportError:
            pass
        run_actor(self)

    def kill(self, sig):
        try:
            system.kill(self.pid, sig)
        except ProcessLookupError:
            pass


class ActorSubProcess(ProcessMixin, Concurrency):
    '''Actor on a Operative system process.
    '''
    process = None

    def start(self):
        loop = asyncio.get_event_loop()
        return loop.create_task(self._start(loop))

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
        raise ActorStarted


concurrency_models.update({
    'arbiter': ArbiterConcurrency,
    'monitor': MonitorConcurrency,
    'coroutine': ActorCoroutine,
    'thread': ActorThread,
    'process': ActorMultiProcess,
    'subprocess': ActorSubProcess
})


_actor_counter = itertools.count(1)
