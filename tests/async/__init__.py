import signal
from time import time

import pulsar
from pulsar import send, spawn, get_application


def add(actor, a, b):
    return actor.name, a+b


def wait_for_stop(test, aid, terminating=False):
    '''Wait for an actor to stop'''
    arbiter = pulsar.arbiter()
    waiter = pulsar.Future(loop=arbiter._loop)

    def remove():
        test.assertEqual(arbiter.remove_callback('periodic_task', check), 1)
        waiter.set_result(None)

    def check(caller, **kw):
        test.assertEqual(caller, arbiter)
        if not terminating:
            test.assertFalse(aid in arbiter.managed_actors)
        elif aid in arbiter.managed_actors:
            return
        arbiter._loop.call_soon(remove)

    arbiter.bind_event('periodic_task', check)
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
    if actor.next_periodic_task:
        actor.next_periodic_task.cancel()
    else:
        actor.event_loop.call_soon(cause_timeout, actor)


def cause_terminate(actor):
    if actor.next_periodic_task:
        actor.next_periodic_task.cancel()
        # hijack the SIGTERM
        actor.impl.kill = kill_hack(actor.impl.kill)
        actor.stop = lambda exc=None, exit_code=None: False
    else:
        actor._loop.call_soon(cause_timeout, actor)


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
