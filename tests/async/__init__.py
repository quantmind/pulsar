import asyncio
from time import time

from pulsar import send, spawn, get_application


def add(actor, a, b):
    return actor.name, a+b


@asyncio.coroutine
def get_test(_):
    app = yield from get_application('test')
    return app.cfg


@asyncio.coroutine
def spawn_actor_from_actor(actor, name):
    actor2 = yield from spawn(name=name)
    pong = yield from send(actor2, 'ping')
    assert pong == 'pong', 'no pong from actor'
    t1 = time()
    # cover the notify from a fron actor
    t2 = yield from send(actor2, 'notify', {})
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
        # hayjack the stop method
        actor.stop = lambda exc=None, exit_code=None: False
    else:
        actor._loop.call_soon(cause_timeout, actor)
