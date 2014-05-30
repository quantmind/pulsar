import os
from threading import current_thread

from .access import asyncio, thread_data, LOGGER
from .futures import Future, maybe_async, async, Task
from .threads import run_in_executor, QueueEventLoop, set_as_loop
from . import dns


__all__ = ['EventLoop', 'call_repeatedly', 'loop_thread_id']


class EventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    '''Pulsar event loop policy'''
    def get_event_loop(self):
        return thread_data('_event_loop')

    def get_request_loop(self):
        return thread_data('_request_loop') or self.get_event_loop()

    def new_event_loop(self):
        return EventLoop()

    def set_event_loop(self, event_loop):
        """Set the event loop."""
        assert event_loop is None or isinstance(event_loop,
                                                asyncio.AbstractEventLoop)
        if isinstance(event_loop, QueueEventLoop):
            thread_data('_request_loop', event_loop)
        else:
            thread_data('_event_loop', event_loop)


asyncio.set_event_loop_policy(EventLoopPolicy())


class LoopingCall(object):

    def __init__(self, loop, callback, args, interval=None):
        self._loop = loop
        self.callback = callback
        self.args = args
        self._cancelled = False
        interval = interval or 0
        if interval > 0:
            self.interval = interval
            self.handler = self._loop.call_later(interval, self)
        else:
            self.interval = None
            self.handler = self._loop.call_soon(self)

    @property
    def cancelled(self):
        return self._cancelled

    def cancel(self):
        '''Attempt to cancel the callback.'''
        self._cancelled = True

    def __call__(self):
        try:
            result = maybe_async(self.callback(*self.args), self._loop)
        except Exception:
            self._loop.logger.exception('Exception in looping callback')
            self.cancel()
            return
        if isinstance(result, Future):
            result.add_done_callback(self._might_continue)
        else:
            self._continue()

    def _continue(self):
        if not self._cancelled:
            handler = self.handler
            loop = self._loop
            if self.interval:
                handler._cancelled = False
                handler._when = loop.time() + self.interval
                loop._add_callback(handler)
            else:
                loop._ready.append(self.handler)

    def _might_continue(self, fut):
        try:
            fut.result()
        except Exception:
            self._loop.logger.exception('Exception in looping callback')
            self.cancel()
        else:
            self._continue()


class EventLoop(asyncio.SelectorEventLoop):
    task_factory = Task

    def __init__(self, selector=None, iothreadloop=False, logger=None,
                 cfg=None):
        super(EventLoop, self).__init__(selector)
        self._iothreadloop = iothreadloop
        self.logger = logger or LOGGER
        self._dns = dns.resolver(self, cfg)
        self.call_soon(set_as_loop, self)

    def run_in_executor(self, executor, callback, *args):
        return run_in_executor(self, executor, callback, *args)

    def getaddrinfo(self, host, port, family=0, type=0, proto=0, flags=0):
        return self._dns.getaddrinfo(host, port, family, type, proto, flags)

    def getnameinfo(self, sockaddr, flags=0):
        return self._dns.getnameinfo(sockaddr, flags)


def call_repeatedly(loop, interval, callback, *args):
    """Call a ``callback`` every ``interval`` seconds.

    It handles asynchronous results. If an error occur in the ``callback``,
    the chain is broken and the ``callback`` won't be called anymore.
    """
    return LoopingCall(loop, callback, args, interval)


def loop_thread_id(loop):
    '''Thread ID of the running ``loop``.
    '''
    waiter = asyncio.Future(loop=loop)
    loop.call_soon_threadsafe(
        lambda: waiter.set_result(current_thread().ident))
    return waiter
