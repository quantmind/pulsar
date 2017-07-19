import signal
from time import time

from pulsar.api import send, spawn, get_application, create_future, arbiter


def add(actor, a, b):
    return actor.name, a + b


def wait_for_stop(test, aid, terminating=False):
    '''Wait for an actor to stop'''
    actor = arbiter()
    waiter = create_future(loop=actor._loop)

    def remove():
        test.assertEqual(actor.event('periodic_task').unbind(check), 1)
        waiter.set_result(None)

    def check(caller, **kw):
        test.assertEqual(caller, actor)
        if not terminating:
            test.assertFalse(aid in actor.managed_actors)
        elif aid in actor.managed_actors:
            return
        actor._loop.call_soon(remove)

    actor.event('periodic_task').bind(check)
    return waiter


async def get_test(_):
    app = await get_application('test')
    return app.cfg


def check_environ(actor, name):
    import os
    return os.environ.get(name)


async def spawn_actor_from_actor(actor, name):
    actor2 = await spawn(name=name)
    pong = await send(actor2, 'ping')
    assert pong == 'pong', 'no pong from actor'
    t1 = time()
    # cover the notify from a fron actor
    t2 = await send(actor2, 'notify', {})
    assert t2 >= t1

    return actor2.aid


def cause_timeout(actor):
    actor.cfg.set('timeout', 10 * actor.cfg.timeout)


def cause_terminate(actor):
    actor.cfg.set('timeout', 100 * actor.cfg.timeout)
    actor.concurrency.kill = kill_hack(actor.concurrency.kill)
    actor.stop = lambda exc=None, exit_code=None: False


def kill_hack(kill):

    def _(sig):
        if sig == signal.SIGKILL:
            kill(sig)

    return _


def close_mailbox(actor, close=False):
    if not close:
        actor._loop.call_later(0.5, close_mailbox, actor, True)
        return True
    # just for coverage
    assert repr(actor.mailbox)
    # close mailbox
    actor.mailbox.close()
