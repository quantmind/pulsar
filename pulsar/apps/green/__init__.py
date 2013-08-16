'''
The :mod:`pulsar.apps.green` adds support for greenlet_ micro-threads.
The code was inspired by the greentulip_ experimental library.


Implementation
=====================

Lets assume a generator ``gen``.
        

.. _greenlet: http://greenlet.readthedocs.org
.. _greentulip: https://github.com/1st1/greentulip
'''
from socket import socket

import pulsar
from pulsar import ImproperlyConfigured
from pulsar.async.defer import _PENDING
from pulsar.utils.pep import get_event_loop, ispy3k

try:
    import greenlet
    
    class TaskGreenLet(greenlet.greenlet):
        def send(self, result):
            return self.switch(result)
        
except ImportError:
    greenlet = None


class GreenTask(pulsar.Task):
    _greenlet = None
        
    def _consume(self, result):
        step = self._step
        switch = False
        green = self._greenlet
        if green:
            # The task is in a greenlet, that means we have a result from an
            # asynchronous yield
            self._greenlet = None
            result, switch = self._step(result, green)
        # The task is not currently in a suspended greenlet waiting for an
        # asynchronous yield
        while self._state == _PENDING and not switch:
            green = TaskGreenLet(step)
            result, switch = green.switch(result)
        if switch:
            self._greenlet = green
         

class GreenEventLoop(pulsar.EventLoop):
    task_factory = GreenTask
    
    def _green_run(self, method, args, kwargs):
        if not greenlet:
            raise ImproperlyConfigured('The green eventloop requires greenlet')
        return greenlet.greenlet(method).switch(*args, **kwargs)

    def run_until_complete(self, *args, **kwargs):
        method = super(GreenEventLoop, self).run_until_complete
        return self._green_run(method, args, kwargs)

    def run_forever(self, *args, **kwargs):
        method = super(GreenEventLoop, self).run_forever
        return self._green_run(method, args, kwargs)
    

class Socket(object):

    def __init__(self, *args, **kwargs):
        _from_sock = kwargs.pop('_from_sock', None) 
        if _from_sock:
            self._sock = _from_sock
        else:
            self._sock = socket(*args, **kwargs)
        self._sock.setblocking(False)
        self._event_loop = get_event_loop()
        assert isinstance(self._loop, GreenUnixSelectorLoop), \
            'GreenUnixSelectorLoop event loop is required'

    @classmethod
    def from_socket(cls, sock):
        return cls(_from_sock=sock)

    @property
    def family(self):
        return self._sock.family

    @property
    def type(self):
        return self._sock.type

    @property
    def proto(self):
        return self._sock.proto

    def _proxy(attr):
        def proxy(self, *args, **kwargs):
            meth = getattr(self._sock, attr)
            return meth(*args, **kwargs)

        proxy.__name__ = attr
        proxy.__qualname__ = attr
        proxy.__doc__ = getattr(getattr(socket, attr), '__doc__', None)
        return proxy

    def _copydoc(func):
        func.__doc__ = getattr(getattr(socket, func.__name__), '__doc__', None)
        return func

    @_copydoc
    def setblocking(self, flag):
        if flag:
            raise error('greentulip.socket does not support blocking mode')

    @_copydoc
    def recv(self, nbytes):
        fut = self._loop.sock_recv(self._sock, nbytes)
        yield_from(fut)
        return fut.result()

    @_copydoc
    def connect(self, addr):
        fut = self._loop.sock_connect(self._sock, addr)
        yield_from(fut)
        return fut.result()

    @_copydoc
    def sendall(self, data, flags=0):
        assert not flags
        fut = self._loop.sock_sendall(self._sock, data)
        yield_from(fut)
        return fut.result()

    @_copydoc
    def send(self, data, flags=0):
        self.sendall(data, flags)
        return len(data)

    @_copydoc
    def accept(self):
        fut = self._loop.sock_accept(self._sock)
        yield_from(fut)
        sock, addr = fut.result()
        return self.__class__.from_socket(sock), addr

    @_copydoc
    def makefile(self, mode, *args, **kwargs):
        if mode == 'rb':
            return ReadFile(self._loop, self._sock)
        elif mode == 'wb':
            return WriteFile(self._loop, self._sock)
        raise NotImplementedError

    bind = _proxy('bind')
    listen = _proxy('listen')
    getsockname = _proxy('getsockname')
    getpeername = _proxy('getpeername')
    gettimeout = _proxy('gettimeout')
    getsockopt = _proxy('getsockopt')
    setsockopt = _proxy('setsockopt')
    fileno = _proxy('fileno')
    close = _proxy('close')
    shutdown = _proxy('shutdown')
    if ispy3k:
        detach = _proxy('detach')

    del _copydoc, _proxy
