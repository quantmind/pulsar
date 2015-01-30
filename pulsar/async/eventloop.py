from threading import current_thread

from .access import asyncio, trollius, thread_data, LOGGER
from .futures import Future, Task, maybe_async
from .threads import run_in_executor, ThreadSafeLoop
from . import dns


__all__ = ['EventLoop', 'set_event_loop_policy',
           'loop_thread_id']


class EventLoop(trollius.SelectorEventLoop, ThreadSafeLoop):

    def __init__(self, selector=None, iothreadloop=False, logger=None,
                 cfg=None):
        super(EventLoop, self).__init__(selector)
        ThreadSafeLoop.__init__(self, iothreadloop)
        self.logger = logger or LOGGER
        self._dns = dns.resolver(self, cfg)

    def create_task(self, coro):
        return Task(coro, loop=self)

    def run_in_executor(self, executor, callback, *args):
        return run_in_executor(self, executor, callback, *args)

    def getaddrinfo(self, host, port, family=0, type=0, proto=0, flags=0):
        return self._dns.getaddrinfo(host, port, family, type, proto, flags)

    def getnameinfo(self, sockaddr, flags=0):
        return self._dns.getnameinfo(sockaddr, flags)

    def _assert_is_current_event_loop(self):
        if self._iothreadloop:
            super(EventLoop, self)._assert_is_current_event_loop()


class EventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    '''Pulsar event loop policy
    '''
    _loop_factory = EventLoop


def set_event_loop_policy(policy):
    '''Same event loop policy for asyncio and trollius
    '''
    asyncio.set_event_loop_policy(policy)
    trollius.set_event_loop_policy(policy)


set_event_loop_policy(EventLoopPolicy())


def loop_thread_id(loop):
    '''Thread ID of the running ``loop``.
    '''
    waiter = Future(loop=loop)
    loop.call_soon_threadsafe(
        lambda: waiter.set_result(current_thread().ident))
    return waiter
