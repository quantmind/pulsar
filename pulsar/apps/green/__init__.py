import sys
import socket
from functools import wraps
from inspect import isgenerator

from greenlet import greenlet, getcurrent

from pulsar import Deferred, Protocol, async
from pulsar.async.defer import DeferredTask


class GreenLet(greenlet):

    def send(self, result):
        return self.switch(result)


class GreenTask(DeferredTask):
    _greenlet = None
    _main = None

    def _consume(self, result):
        step = self._step
        switch = False
        green = self._greenlet
        if green:
            # The task is in a greenlet, that means we have a result from an
            # asynchronous yield
            self._greenlet = None
            result, switch = step(result, self._gen)
        # The task is not currently in a suspended greenlet waiting for an
        # asynchronous yield
        while not self.done() and not switch:
            green = greenlet(step)
            result, switch = green.switch(result, self._gen)
        if switch:
            self._greenlet = green


def green_task(method):

    @wraps(method)
    def _(self, *args, **kwargs):
        result = method(self, *args, **kwargs)
        if isgenerator(result):
            return GreenTask(result, self._loop)
        else:
            return result

    return _


def green_loop_thread(sync_method):
    """Decorate `sync_method` so it is run on child greenlet in the
    event loop thread.
    """
    @wraps(sync_method)
    def method(self, *args, **kwargs):
        loop = self._loop
        future = Deferred(loop)

        def call_method():
            # Runs on child greenlet
            try:
                result = sync_method(self, *args, **kwargs)
                loop.call_soon(future.set_result, result)
            except Exception:
                loop.call_soon(future.set_exception, sys.exc_info())

        # Start running the operation on a greenlet.
        loop.call_soon_threadsafe(lambda: greenlet(call_method).switch())
        return future

    return method


def green_loop(method):
    '''Decorator for running the ``method`` in the event loop
    of an :ref:`async object <async-object>`.

    This decorator is somehow the ``greenlet`` equivalent of the
    :ref:`.in_loop` decorator.

    :param method: method of an :ref:`async object <async-object>` which
        returns a coroutine or a :class:`.Deferred`.
    '''
    @wraps(method)
    def _green_async(self, *args, **kw):
        # the loop controlling the thread
        child_gr = getcurrent()
        # the main greenlet is where pulsar carries out its async duties
        main = child_gr.parent
        assert main, "Should be on child greenlet"

        def callback(result):
            assert main == getcurrent(), "Should be on main"
            # switch back to child with the result
            result = child_gr.switch(result)
            return result

        def errback(failure):
            assert main == getcurrent(), "Should be on main"
            # switch back to child with the result
            result = child_gr.switch(failure)
            return result

        future = async(method(self, *args, **kw), self._loop)
        future.add_callback(callback, errback)
        # switch to the main greenlet
        return main.switch(future)

    return _green_async


class GreenProtocol(Protocol):
    '''A protocol to use with blocking clients.
    '''
    def __init__(self, *args, **kw):
        super(GreenProtocol, self).__init__(*args, **kw)
        self._reading = None
        self._buffer = None

    def fileno(self):
        return self._transport._sock_fd

    def settimeout(self, timeout):
        self.timeout = timeout

    def setsockopt(self, *args, **kwargs):
        self.transport._sock.setsockopt(*args, **kwargs)

    def sendall(self, data):
        if self._transport:
            try:
                self._transport.write(data)
            except IOError as e:
                raise socket.error(str(e))

            if self._transport.closed:
                raise socket.error("write error")

    @green_loop
    def recv(self, bufsize, **kw):
        assert not self._reading, "Already reading"
        self._reading = future = Deferred(self._transport._loop)
        self._transport._read_chunk_size = bufsize
        try:
            self._transport.resume_reading()
        except Exception:
            pass
        return future

    def data_received(self, chunk):
        if self._buffer:
            chunk = self._buffer + chunk
        size = self._transport._read_chunk_size - len(chunk)
        if size:
            self._buffer = chunk
            self._transport._read_chunk_size = size
        else:
            self._buffer = None
            self._reading, future = None, self._reading
            self._transport.pause_reading()
            future.set_result(chunk)

    def close(self):
        self.transport.close()

    def fileno(self):
        return self.transport._sock_fd
