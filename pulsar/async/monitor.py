import os
import sys
from time import time

import pulsar
from pulsar.utils.structures import AttributeDictionary

from . import proxy
from .actor import Actor, ACTOR_STATES, ACTOR_TERMINATE_TIMEOUT,\
                     ACTOR_STOPPING_LOOPS
from .eventloop import setid
from .concurrency import concurrency
from .defer import async, iteritems, itervalues, range, NOT_DONE
from .mailbox import Queue, mailbox


__all__ = ['Monitor', 'PoolMixin']


def _spawn_actor(cls, commands_set=None, monitor=None, cfg=None, name=None,
                 aid=None, **kw):
    # Internal function which spawns a new Actor and return its
    # ActorProxyMonitor.
    # *cls* is the Actor class
    # *monitor* can be either the ariber or a monitor
    kind = None
    if issubclass(cls, PoolMixin):
        kind = 'monitor'
    if cfg is None:
        if monitor:
            cfg = monitor.cfg.copy()
        else:
            cfg = pulsar.Config()
    if monitor:
        params = monitor.actorparams()
        name = params.pop('name', name)
        aid = params.pop('aid', aid)
        commands_set = params.pop('commands_set', commands_set)
    else:
        if kind != 'monitor':
            raise TypeError('class %s not a valid monitor' % cls)
        params = {}
    commands_set = set(commands_set or proxy.actor_commands)
    for key, value in iteritems(kw):
        if key in cfg.settings:
            cfg.set(key, value)
        else:
            params[key] = value
    #
    if monitor:
        if not kind:
            if not issubclass(cls, Actor):
                raise TypeError('Class %s not a valid actor.' % cls)
            kind = cfg.concurrency
    if not kind:
        raise TypeError('Cannot spawn class %s. not a valid concurrency.' % cls)
    actor_proxy = concurrency(kind, cls, monitor, commands_set, cfg,
                              name=name, aid=aid, **params)
    # Add to the list of managed actors if this is a remote actor
    if isinstance(actor_proxy, Actor):
        return actor_proxy
    else:
        actor_proxy.monitor = monitor
        monitor.spawning_actors[actor_proxy.aid] = actor_proxy
        deferred = proxy.ActorProxyDeferred(actor_proxy)
        actor_proxy.start()
        return deferred


class PoolMixin(object):
    '''Not an actor per se, this is a mixin for :class:`Actor`
which manages a pool (group) of actors. Given an :attr:`actor_class`
it makes sure there are always :attr:`cfg.workers` alive.
It is used by both the :class:`Arbiter` and the :class:`Monitor` classes.

.. attribute:: managed_actors

    dictionary with keys given by actor's ids and values by
    :class:`ActorProxyMonitor` instances. These are the actors managed by the
    pool.
    
.. attribute:: spawning_actors

    A dictionary of :class:`ActorProxyMonitor` which are in the process of
    being spawned.

'''
    CLOSE_TIMEOUT = 30000000000000
    actor_class = Actor
    '''The class derived form :class:`Actor` which the monitor manages
during its life time.

    Default: :class:`Actor`'''

    def on_start(self):
        self.spawning_actors = {}
        self.managed_actors = {}
        self.actor_class = self.params.pop('actor_class') or self.actor_class

    def active(self):
        return self.running()

    def spawn(self, actor_class=None, linked_actors=None, montitor=None,
              **params):
        '''Spawn a new :class:`Actor` and return its
:class:`ActorProxyMonitor`.'''
        actor_class = actor_class or self.actor_class
        if linked_actors:
            params['linked_actors'] =\
                dict(((aid, p.proxy) for aid, p in iteritems(linked_actors)))
        return _spawn_actor(actor_class, monitor=self, **params)

    def actorparams(self):
        '''Return a dictionary of parameters to be passed to the
spawn method when creating new actors.'''
        arbiter = self.arbiter or self
        params = dict(self.params)
        params['monitors'] = arbiter.get_all_monitors()
        return params

    def get_actor(self, aid):
        a = Actor.get_actor(self, aid)
        if not a:
            a = self.spawning_actors.get(aid)
        return a

    def manage_actors(self, terminate=False, stop=False, manage=True):
        '''Remove :class:`Actor` which are not alive from the
:class:`PoolMixin.managed_actors` and return the number of actors still alive.

:parameter terminate: if ``True`` force termination of alive actors.
:parameter stop: if ``True`` stops all alive actor.
:parameter manage: if ``True`` it checks if alive actors are still responsive.
'''
        MANAGED = self.managed_actors
        SPAWNING = self.spawning_actors
        LINKED = self.linked_actors
        alive = 0
        ACTORS = list(iteritems(MANAGED))
        # WHEN TERMINATING OR STOPPING WE INCLUDE THE ACTORS WHICH ARE SPAWNING
        shutting_down = terminate or stop
        if shutting_down:
            ACTORS.extend(iteritems(SPAWNING))
        # Loop over MANAGED ACTORS PROXY MONITORS
        for aid, actor in ACTORS:
            if not actor.is_alive():
                actor.join(ACTOR_TERMINATE_TIMEOUT)
                MANAGED.pop(aid, None)
                SPAWNING.pop(aid, None)
                LINKED.pop(aid, None)
            else:
                alive += 1
                if shutting_down:
                    # terminate if there is not mailbox (ther actor has
                    # registered with its monitor yet).
                    if terminate or actor.mailbox is None:
                        actor.terminate()
                        actor.join(ACTOR_TERMINATE_TIMEOUT)
                    else:
                        actor.stop(self)
                elif manage:
                    self.manage_actor(actor)
        return alive

    def manage_actor(self, actor):
        '''If an actor failed to notify itself to the arbiter for more than
the timeout. Stop the arbiter.'''
        stopping_loops = actor.stopping_loops
        if self.running() and (stopping_loops or actor.notified):
            gap = time() - actor.notified
            if gap > actor.cfg.timeout or stopping_loops:
                if stopping_loops < ACTOR_STOPPING_LOOPS:
                    if not stopping_loops:
                        self.logger.info('Stopping %s. Timeout.', actor)
                        self.send(actor, 'stop')
                    actor.stopping_loops += 1
                else:
                    self.logger.warn('Terminating %s. Timeout.', actor)
                    actor.terminate()
                    actor.join(ACTOR_TERMINATE_TIMEOUT)

    def spawn_actors(self):
        '''Spawn new actors if needed. If the :class:`PoolMixin` is spawning
do nothing.'''
        to_spawn = self.cfg.workers - len(self.managed_actors)
        if self.cfg.workers and to_spawn > 0 and not self.spawning_actors:
            for _ in range(to_spawn):
                self.spawn()

    def stop_actors(self):
        """Maintain the number of workers by spawning or killing
as required."""
        if self.cfg.workers:
            num_to_kill = len(self.managed_actors) - self.cfg.workers
            for i in range(num_to_kill, 0, -1):
                w, kage = 0, sys.maxsize
                for worker in itervalues(self.managed_actors):
                    age = worker.impl.age
                    if age < kage:
                        w, kage = w, age
                self.stop_actor(w)

    def stop_actor(self, actor):
        raise NotImplementedError()

    def link_actor(self, proxy, address):
        # Override the link_actor from Actor class
        self.logger.debug('Registering %s address %s', proxy, address)
        if proxy.aid not in self.spawning_actors:
            raise RuntimeError('Could not retrieve proxy %s.' % proxy)
        proxy_monitor = self.spawning_actors.pop(proxy.aid)
        proxy_monitor.info['last_notified'] = time()
        on_address = proxy_monitor.on_address
        delattr(proxy_monitor,'on_address')
        try:
            if not address:
                raise ValueError('No address received')
        except:
            on_address.callback(sys.exc_info())
        else:
            proxy_monitor.address = address
            self.managed_actors[proxy.aid] = proxy_monitor
            self.linked_actors[proxy.aid] = proxy
            if self.arbiter:
                # link also the arbiter
                self.arbiter.linked_actors[proxy.aid] = proxy
            on_address.callback(proxy_monitor.proxy)
        return proxy_monitor.proxy

    @async()
    def close_actors(self):
        '''Close all managed :class:`Actor`.'''
        start = time()
        # Stop all of them
        to_stop = self.manage_actors(stop=True)
        while to_stop:
            yield NOT_DONE
            to_stop = self.manage_actors(manage=False)
            dt = time() - start
            if dt > self.CLOSE_TIMEOUT:
                self.logger.warn('Cannot stop %s actors.', to_stop)
                to_stop = self.manage_actors(terminate=True)
                self.logger.warn('terminated %s actors.', to_stop)
                to_stop = 0


class Monitor(PoolMixin, Actor):
    '''A monitor is a **very** special :class:`Actor` and :class:`PoolMixin`
which shares the same :class:`IOLoop` with the :class:`Arbiter` and
therefore lives in the main process domain.
The Arbiter manages monitors which in turn manage a set of :class:`Actor`
performing similar tasks.

In other words, you may have a monitor managing actors for serving HTTP
requests on a given port, another monitor managing actors consuming tasks
from a task queue and so forth. You can think of :class:`Monitor` as
managers of pools of :class:`Actor`.

Monitors are created by invoking the :meth:`Arbiter.add_monitor`
functions and not by directly invoking the constructor. Therefore
adding a new monitor to the arbiter follows the pattern::

    import pulsar

    m = pulsar.arbiter().add_monitor(pulsar.Monitor,'mymonitor')

You can also create a monitor with a distributed queue as IO mechanism::

    from multiprocessing import Queue
    import pulsar

    m = pulsar.arbiter().add_monitor(pulsar.Monitor,
                                     'mymonitor',
                                     ioqueue = Queue())

Monitors with distributed queues manage :ref:`CPU-bound actors <cpubound>`.
'''
    socket = None

    @property
    def cpubound(self):
        return False

    def is_process(self):
        return False

    def is_monitor(self):
        return True

    def monitor_task(self):
        '''Monitor specific task called by the :meth:`Monitor.periodic_task`.
By default it does nothing. Override if you need to.'''
        pass

    def periodic_task(self):
        '''Overrides the :meth:`Actor.on_task`
:ref:`actor callback <actor-callbacks>` to perform
the monitor :class:`IOLoop` tasks, which are:

* To maintain a responsive set of actors ready to perform their duty.
* To perform its own tasks.

The implementation goes as following:

* It calls :meth:`PoolMixin.manage_actors` which removes from the live
  actors dictionary all actors which are not alive.
* Spawn new actors if required by calling :meth:`PoolMixin.spawn_actors`
  and :meth:`PoolMixin.stop_actors`.
* Call :meth:`Monitor.monitor_task` which performs the monitor specific
  task.

Users shouldn't need to override this method, but use
:meth:`Monitor.monitor_task` instead.'''
        if self.running():
            self.manage_actors()
            self.spawn_actors()
            self.stop_actors()
            self.monitor_task()
        self.ioloop.add_callback(self.periodic_task, False)

    # HOOKS
    def on_stop(self):
        return self.close_actors()

    @property
    def multithread(self):
        return self.cfg.concurrency == 'thread'

    @property
    def multiprocess(self):
        return self.cfg.concurrency == 'process'

    def actorparams(self):
        '''Spawn a new actor and add its :class:`ActorProxyMonitor`
 to the :attr:`PoolMixin.managed_actors` dictionary.'''
        p = super(Monitor, self).actorparams()
        p.update({'ioqueue': self.ioqueue,
                  'commands_set': self.impl.commands_set})
        return p

    def stop_actor(self, actor):
        if not actor.is_alive():
            self.managed_actors.pop(actor.aid)
        else:
            return actor.proxy.stop()

    def info(self):
        data = {'actor': {'actor_class':self.actor_class.__name__,
                          'concurrency':self.cfg.concurrency,
                          'name':self.name,
                          'age':self.impl.age,
                          'workers': len(self.managed_actors)}}
        if not self.started():
            return data
        data['workers'] = [a.info for a in itervalues(self.managed_actors)]
        tq = self.ioqueue
        if tq is not None:
            if isinstance(tq, Queue):
                tqs = 'multiprocessing.Queue'
            else:
                tqs = str(tq)
            data['queue'] = {'ioqueue': tqs,
                             'ioqueue_size': tq.qsize()}
        return self.on_info(data)

    def proxy_mailbox(address):
        return self.arbiter.proxy_mailboxes.get(address)

    def get_actor(self, aid):
        #Delegate get_actor to the arbiter
        a = super(Monitor, self).get_actor(aid)
        if a is None:
            a = self.arbiter.get_actor(aid)
        return a

    # OVERRIDES INTERNALS
    def _setup_ioloop(self):
        self.requestloop = self.arbiter.requestloop
        self.mailbox = mailbox(self)
        self.monitors = self.arbiter.monitors
        
    def _run(self):
        pass
