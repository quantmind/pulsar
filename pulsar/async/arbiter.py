import os
import sys
from multiprocessing import current_process

import pulsar
from pulsar.utils.tools import Pidfile
from pulsar.utils.security import gen_unique_id
from pulsar.utils.pep import itervalues
from pulsar import HaltServer

from .actor import Actor, ACTOR_STATES
from .monitor import PoolMixin, Monitor, _spawn_actor
from .futures import multi_async
from .access import get_actor, set_actor
from .proxy import actor_proxy_future


__all__ = ['arbiter', 'spawn', 'Arbiter']


def arbiter(**params):
    '''Obtain the :class:`.Arbiter`.

    It returns the arbiter instance only if we are on the arbiter
    context domain, otherwise it returns nothing.
    '''
    arbiter = get_actor()
    if arbiter is None:
        # Create the arbiter
        return set_actor(_spawn_actor(Arbiter, None, **params))
    elif isinstance(arbiter, Actor) and arbiter.is_arbiter():
        return arbiter


def spawn(**kwargs):
    '''Spawn a new :class:`.Actor` and return an :class:`.ActorProxyFuture`.

    This method can be used from any :class:`.Actor`.
    If not in the :class:`.Arbiter` domain, the method sends a request
    to the :class:`.Arbiter` to spawn a new actor.
    Once the arbiter creates the actor it returns the ``proxy`` to the
    original caller.

    **Parameter kwargs**

    These optional parameters are:

    * ``aid`` the actor id
    * ``name`` the actor name
    * :ref:`actor hooks <actor-hooks>` such as ``start``, ``stopping``
      and ``stop``
    * ``actor_class`` a custom :class:`.Actor` subclass (never used)

    :return: an :class:`.ActorProxyFuture`.

    A typical usage::

        >>> def do_something(actor):
                ...
        >>> a = spawn(start=do_something, ...)
        >>> a.aid
        'ba42b02b'
        >>> a.called
        True
        >>> p = a.result()
        >>> p.address
        ('127.0.0.1', 46691)
    '''
    aid = gen_unique_id()[:8]
    kwargs['aid'] = aid
    actor = get_actor()
    # The actor is not the Arbiter domain.
    # We send a message to the Arbiter to spawn a new Actor
    if not isinstance(actor, Arbiter):
        # send the request to the arbiter
        future = actor.send('arbiter', 'spawn', **kwargs)
        return actor_proxy_future(aid, future)
    else:
        return actor.spawn(**kwargs)


def stop_arbiter(self, exc=None):     # pragma    nocover
    p = self.pidfile
    if p is not None:
        self.logger.debug('Removing %s' % p.fname)
        p.unlink()
        self.pidfile = None
    if self.managed_actors:
        self.state = ACTOR_STATES.TERMINATE
    self.collect_coverage()
    exit_code = self.exit_code or 0
    self.stream.writeln("Bye (exit code = %s)" % exit_code)
    try:
        self.cfg.when_exit(self)
    except Exception:
        pass
    if exit_code:
        sys.exit(exit_code)
    return self


def start_arbiter(self, exc=None):
    if current_process().daemon:
        raise HaltServer('Cannot create the arbiter in a daemon process')
    os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
    pidfile = self.cfg.pidfile
    if pidfile is not None:
        try:
            p = Pidfile(pidfile)
            p.create(self.pid)
        except RuntimeError as e:
            raise HaltServer('ERROR. %s' % str(e), exit_code=3)
        self.pidfile = p


def info_arbiter(self, info=None):
    data = info
    monitors = {}
    for m in itervalues(self.monitors):
        info = m.info()
        if info:
            actor = info['actor']
            monitors[actor['name']] = info
    server = data.pop('actor')
    server.update({'version': pulsar.__version__,
                   'name': pulsar.SERVER_NAME,
                   'number_of_monitors': len(self.monitors),
                   'number_of_actors': len(self.managed_actors)})
    server.pop('is_process', None)
    server.pop('ppid', None)
    server.pop('actor_id', None)
    server.pop('age', None)
    data['server'] = server
    data['workers'] = [a.info for a in itervalues(self.managed_actors)]
    data['monitors'] = monitors
    return data


class Arbiter(PoolMixin):
    '''The Arbiter drives pulsar servers.

    It is the most important a :class:`.Actor` and :class:`.PoolMixin` in
    pulsar concurrent framework. It is used as singleton
    in the main process and it manages one or more :class:`.Monitor`.
    It runs the main :ref:`event loop <asyncio-event-loop>` of your
    concurrent application.

    Users access the arbiter (in the arbiter process domain) by the
    high level api::

        import pulsar

        arbiter = pulsar.arbiter()
    '''
    pidfile = None

    def __init__(self, impl):
        super(Arbiter, self).__init__(impl)
        self.monitors = {}
        self.registered = {'arbiter': self}
        self.bind_event('start', start_arbiter)
        self.bind_event('stop', stop_arbiter)
        self.bind_event('on_info', info_arbiter)

    ########################################################################
    # ARBITER HIGH LEVEL API
    ########################################################################
    def add_monitor(self, monitor_name, monitor_class=None, **params):
        '''Add a new :class:`.Monitor` to the :class:`Arbiter`.

        :param monitor_class: a :class:`.Monitor` class.
        :param monitor_name: a unique name for the monitor.
        :param kwargs: dictionary of key-valued parameters for the monitor.
        :return: the :class:`.Monitor` added.
        '''
        if monitor_name in self.registered:
            raise KeyError('Monitor "%s" already available' % monitor_name)
        monitor_class = monitor_class or Monitor
        params.update(self.actorparams())
        params['name'] = monitor_name
        m = self.spawn(monitor_class, **params)
        self.registered[m.name] = m
        self.monitors[m.aid] = m
        return m

    def close_monitors(self):
        '''Close all :class:`.Monitor` at once.
        '''
        return multi_async((m.stop() for m in list(itervalues(self.monitors))))

    def get_actor(self, aid):
        '''Given an actor unique id return the actor proxy.'''
        a = super(Arbiter, self).get_actor(aid)
        if a is None:
            if aid in self.monitors:  # Check in monitors aid
                return self.monitors[aid]
            elif aid in self.managed_actors:
                return self.managed_actors[aid]
            elif aid in self.registered:
                return self.registered[aid]
            else:  # Finally check in workers in monitors
                for m in itervalues(self.monitors):
                    if aid in m.managed_actors:
                        return m.managed_actors[aid]
        else:
            return a

    def identity(self):
        return self.name

    ########################################################################
    # INTERNALS
    ########################################################################
    def _remove_actor(self, actor, log=True):
        a = super(Arbiter, self)._remove_actor(actor, False)
        b = self.registered.pop(actor.name, None)
        c = self.monitors.pop(actor.aid, None)
        removed = a or b or c
        if removed and log:
            self.logger.warning('Removing %s', actor)
        return removed
