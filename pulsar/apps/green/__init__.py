'''
Greenlet integration allows to use pulsar with synchronous socket
clients.
'''
import sys
import socket
from functools import wraps

from greenlet import greenlet, getcurrent

from pulsar import Deferred, Protocol, maybe_async


def _run(loop, method, *args, **kwargs):
    green = getcurrent()
    main = green.parent
    assert main, 'should have parent'
    r = maybe_async(method(*args, **kwargs), loop=loop)
    while isinstance(r, Deferred):
        r.add_callback(green.switch, lambda f: green.switch(f.error))
        main.switch()
        r = r.result()
    return r


def green_run(method):
    '''Decorator for a possibly asynchronous ``method``.

    If ``method`` return a coroutine or a :class:`.Deferred` not yet done,
    the current greenlet relase control to is parent.
    '''
    def _green_run(self, *args, **kwargs):
        loop = getattr(self, '_loop', None)
        return _run(loop, method, self, *args, **kwargs)

    return _green_run


class green_loop_thread:

    def __init__(self, delegate=None):
        self.delegate = delegate

    def __call__(self, sync_method):
        """Decorate `sync_method` so it is run on child greenlet in the
        event loop thread.
        """
        @wraps(sync_method)
        def method(caller, *args, **kwargs):
            loop = caller._loop
            future = Deferred(loop)
            if self.delegate:
                caller = getattr(caller, self.delegate)

            def call_method():
                # Runs on child greenlet
                try:
                    result = _run(loop, sync_method, caller, *args, **kwargs)
                    loop.call_soon(future.set_result, result)
                except Exception:
                    loop.call_soon(future.set_exception, sys.exc_info())

            # Start running the operation on a new greenlet.
            loop.call_soon_threadsafe(lambda: greenlet(call_method).switch())
            if not getattr(loop, '_iothreadloop', True) and not loop.is_running():
                return loop.run_until_complete(future)
            return future

        return method


class GreenProtocol(Protocol):
    '''A protocol to use with blocking clients.
    '''
    def __init__(self, *args, **kw):
        super(GreenProtocol, self).__init__(*args, **kw)
        self._reading = None

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
    send = sendall

    @green_run
    def recv(self, bufsize, **kw):
        assert not self._reading, "Already reading"
        self._reading = future = Deferred(self._loop)
        self._transport._read_chunk_size = bufsize
        try:
            self._transport.resume_reading()
        except Exception:
            pass
        return future

    def data_received(self, chunk):
        self._reading, future = None, self._reading
        self._transport.pause_reading()
        future.set_result(chunk)

    def close(self):
        self.transport.close()
