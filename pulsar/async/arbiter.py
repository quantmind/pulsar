from time import time
import os
import sys
import signal
from multiprocessing import current_process

import pulsar
from pulsar.utils import system
from pulsar.utils.tools import Pidfile
from pulsar.utils.security import gen_unique_id
from pulsar import HaltServer

from .defer import itervalues, iteritems, multi_async, async
from .actor import Actor, ACTOR_STATES
from .monitor import PoolMixin, _spawn_actor
from .access import get_actor, set_actor
from . import proxy


process_global = pulsar.process_global

__all__ = ['arbiter', 'spawn', 'Arbiter']


def arbiter(commands_set=None, **params):
    '''Obtain the arbiter instance.'''
    arbiter = get_actor()
    if arbiter is None:
        # Create the arbiter
        cset = set(proxy.actor_commands)
        cset.update(proxy.arbiter_commands)
        cset.update(commands_set or ())
        return set_actor(_spawn_actor(Arbiter, commands_set=cset, **params))
    elif isinstance(arbiter, Actor) and arbiter.is_arbiter():
        return arbiter


def spawn(cfg=None, **kwargs):
    '''Spawn a new :class:`Actor` and return an :class:`ActorProxyDeferred`.
This method can be used from any :class:`Actor`.
If not in the :class:`Arbiter` domain,
the method send a request to the :class:`Arbiter` to spawn a new actor, once
the arbiter creates the actor it returns the proxy to the original caller.

**Parameter kwargs**

These optional parameters are:
    * *actor_class* a custom :class:`Actor` subclass.
    * *aid* the actor id
    * *commands_set* the set of :ref:`remote commands <api-remote_commands>`
      the :class:`Actor` can respond to.
    
:rtype: an :class:`ActorProxyDeferred`.

A typical usage::

    >>> a = spawn()
    >>> a.aid
    'ba42b02b'
    >>> a.called
    True
    >>> p = a.result
    >>> p.address
    ('127.0.0.1', 46691)
    '''
    aid = gen_unique_id()[:8]
    kwargs['aid'] = aid
    actor = get_actor()
    # The actor is not the Arbiter domain.
    # We send a message to the Arbiter to spawn a new Actor
    if not isinstance(actor, Arbiter):
        msg = actor.send('arbiter', 'spawn', **kwargs)\
                            .add_callback(actor.link_actor)
        return proxy.ActorProxyDeferred(aid, msg)
    else:
        return actor.spawn(**kwargs)

    
class Arbiter(PoolMixin, Actor):
    '''The Arbiter is the most important a :class:`Actor`
and :class:`PoolMixin` in pulsar concurrent framework. It is used as singleton
in the main process and it manages one or more :class:`Monitor`.
It runs the main :class:`IOLoop` of your concurrent application.
It is the equivalent of the gunicorn_ arbiter, the twisted_ reactor
and the tornado_ eventloop.

Users access the arbiter (in the arbiter process domain) by the high level api::

    import pulsar

    arbiter = pulsar.arbiter()

.. _gunicorn: http://gunicorn.org/
.. _twisted: http://twistedmatrix.com/trac/
.. _tornado: http://www.tornadoweb.org/
'''
    pidfile = None
    restarted = False

    ############################################################################
    # ARBITER HIGH LEVEL API
    ############################################################################
    def is_arbiter(self):
        return True

    def add_monitor(self, monitor_class, monitor_name, **params):
        '''Add a new :class:`Monitor` to the :class:`Arbiter`.

:parameter monitor_class: a :class:`pulsar.Monitor` class.
:parameter monitor_name: a unique name for the monitor.
:parameter kwargs: dictionary of key-valued parameters for the monitor.
:rtype: an instance of a :class:`pulsar.Monitor`.'''
        if monitor_name in self.monitors:
            raise KeyError('Monitor "{0}" already available'\
                           .format(monitor_name))
        params['name'] = monitor_name
        m = self.spawn(monitor_class, **params)
        self.linked_actors[m.aid] = m
        self.monitors[m.name] = m
        return m

    def is_process(self):
        return True

    def get_all_monitors(self):
        '''A dictionary of all :class:`Monitor` in the arbiter'''
        return dict(((mon.name, mon.proxy) for mon in\
                      itervalues(self.monitors) if mon.mailbox))

    @multi_async
    def close_monitors(self):
        '''Close all :class:`Monitor` at once.'''
        for pool in list(itervalues(self.monitors)):
            yield pool.stop(True)

    def on_info(self, data):
        monitors = [p.info() for p in itervalues(self.monitors)]
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

    ############################################################################
    # OVERRIDE ACTOR HOOKS
    ############################################################################
    def on_start(self):
        if current_process().daemon:
            raise pulsar.PulsarException(
                    'Cannot create the arbiter in a daemon process')
        os.environ["SERVER_SOFTWARE"] = pulsar.SERVER_SOFTWARE
        pidfile = self.cfg.pidfile
        if pidfile is not None:
            p = Pidfile(pidfile)
            p.create(self.pid)
            self.pidfile = p
        PoolMixin.on_start(self)

    def periodic_task(self):
        # Arbiter periodic task
        if self.restarted:
            self.stop(exit_code=self.exit_code)
        if self.can_continue() and self.running():
            # managed actors job
            self.manage_actors()
            for m in list(itervalues(self.monitors)):
                if m.started():
                    if not m.running():
                        self.logger.info('Removing monitor %s', m)
                        self.monitors.pop(m.name)
                else:
                    m.start()
            try:
                self.cfg.arbiter_task(self)
            except:
                pass
        self.ioloop.add_callback(self.periodic_task, False)

    @async()
    def on_stop(self):
        '''Stop the pools the message queue and remaining actors.'''
        if not self.ioloop.running():
            for pool in list(itervalues(self.monitors)):
                pool.state = ACTOR_STATES.INACTIVE
            self.state = ACTOR_STATES.RUN
            self.restarted = True
            self.ioloop.start()
        else:
            self.state = ACTOR_STATES.STOPPING
            yield self.close_monitors()
            yield self.close_actors()
            yield self._close_message_queue()
    
    def on_exit(self):
        p = self.pidfile
        if p is not None:
            p.unlink()
        if self.managed_actors or self.linked_actors:
            self.state = ACTOR_STATES.TERMINATE
        self.logger.info("Bye.")
        if self.exit_code:
            sys.exit(self.exit_code)

    ############################################################################
    # INTERNALS
    ############################################################################
    def start(self):
        if self.state == ACTOR_STATES.INITIAL:
            if self.cfg.daemon: #pragma    nocover
                system.daemonize()
            return Actor.start(self)
            
    def _run(self):
        try:
            self.cfg.when_ready(self)
        except:
            pass
        try:
            self.requestloop.start()
        except HaltServer as e:
            self._halt(reason=str(e))
        except (KeyboardInterrupt, SystemExit) as e:
            self._halt(e.__class__.__name__)
        except:
            self._halt(code=1)

    def _halt(self, reason=None, code=None):
        if not self.closed():
            if not reason:
                self.logger.critical("Unhandled exception in main loop.",
                                     exc_info=True)
            else:
                self.logger.info(reason)
            self.stop(True, exit_code=code)

    def _close_message_queue(self):
        return

