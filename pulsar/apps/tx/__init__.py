'''
Pulsar concurrent framework and applications can be used with twisted_, an
event driven network engine for python. Twisted has implementation
for several protocols which can be used in pulsar by importing the
:mod:`pulsar.apps.tx` module.

.. warning::

    This is an experimental implementation, by any means complete.
    Use at your own risk.

The implementation replaces the twisted reactor with a proxy for
an :ref:`asyncio event loop <asyncio-event-loop>`.
Twisted Deferred and Failures are made compatible with pulsar
by decorating a callable with the :func:`.tx` decorator.

Threads, signal handling, scheduling and so forth is handled by pulsar itself,
twisted implementation is switched off.


Implementation
====================

.. autofunction:: tx


.. _twisted: http://twistedmatrix.com/
'''
import types
from functools import wraps

try:    # pragma    nocover
    from twisted.internet.main import installReactor
    from twisted.internet.posixbase import PosixReactorBase
    from twisted.internet.defer import Deferred, _inlineCallbacks
except ImportError:     # pragma    nocover
    # This is for when we build documentation with sphinx in python 3
    import os
    if os.environ.get('BUILDING-PULSAR-DOCS') == 'yes':
        PosixReactorBase = object
        installReactor = None
    else:
        raise

from pulsar import get_event_loop, Future


def tx(callable):
    '''Decorator for callables returning a twisted deferred.
    '''
    @wraps(callable)
    def _(*args, **kwargs):
        res = callable(*args, **kwargs)
        if isinstance(res, types.GeneratorType):
            res = _inlineCallbacks(None, res, Deferred())
        if isinstance(res, Deferred):
            future = Future()
            res.addCallbacks(
                future.set_result,
                lambda failure: future.set_exception(failure.value))
            future._deferred = res
            return future
        else:
            raise TypeError(
                "Callable %r should return a generator or a twisted Deferred"
                % callable)

    return _


class PulsarReactor(PosixReactorBase):  # pragma    nocover
    '''A twisted reactor which acts as a proxy to pulsar eventloop.

    The event loop is obtained via the ``get_event_loop`` function.
    '''
    def callLater(self, _seconds, f, *args, **kw):
        return get_event_loop().call_later(_seconds, lambda: f(*args, **kw))

    def callFromThread(self, f, *args, **kw):
        return get_event_loop().call_soon_threadsafe(lambda: f(*args, **kw))

    def callInThread(self, f, *args, **kw):
        return get_event_loop().call_soon(lambda: f(*args, **kw))

    def addReader(self, reader):
        return get_event_loop().add_reader(reader.fileno(), reader.doRead)

    def addWriter(self, writer):
        return get_event_loop().add_writer(writer.fileno(), writer.doWrite)

    def removeReader(self, reader):
        return get_event_loop().remove_reader(reader.fileno())

    def removeWriter(self, writer):
        return get_event_loop().remove_writer(writer.fileno())

    def removeAll(self):
        return get_event_loop().remove_all()

    #######################################################################
    # Functions not needed by pulsar
    _registerAsIOThread = False

    def installWaker(self):
        pass

    def mainLoop(self):
        pass

    def doIteration(self, delay):
        pass

    def spawnProcess(self, *args, **kwargs):
        raise NotImplementedError('Cannot spawn process from Pulsar reactor')

    def _initThreads(self):
        pass

    def _handleSignals(self):
        pass


if installReactor:  # pragma    nocover
    _reactor = PulsarReactor()
    installReactor(_reactor)
    _reactor.run()
