'''
Pulsar concurrent framework and applications can be used with twisted_, an
event driven network engine for python. Twisted has implementation
for several protocols which can be used in pulsar by importing the
:mod:`pulsar.apps.tx` module.

.. warning::

    This is an experimental implementation, by any means complete.
    Use at your own risk

The implementation replaces the twisted reactor with a proxy for
an :ref:`asyncio event loop <asyncio-event-loop>`.
Twisted Deferred and Failures are made compatible with pulsar
by installing twisted asynchronous binding via the
:func:`.add_async_binding` function.

Threads, signal handling, scheduling and so forth is handled by pulsar itself,
twisted implementation is switched off.


Implementation
====================

.. autofunction:: check_twisted

.. autoclass:: PulsarReactor
   :members:
   :member-order: bysource


.. _twisted: http://twistedmatrix.com/
'''
try:    # pragma    nocover
    import twisted
    from twisted.internet.main import installReactor
    from twisted.internet.posixbase import PosixReactorBase
    from twisted.internet.defer import Deferred
except ImportError:     # pragma    nocover
    # This is for when we build documentation with sphinx in python 3
    import os
    if os.environ.get('BUILDING-PULSAR-DOCS') == 'yes':
        PosixReactorBase = object
        installReactor = None
    else:
        raise

import pulsar
from pulsar import get_event_loop, Future, add_async_binding


def check_twisted(deferred, loop):
    '''Binding for twisted.

    Added to pulsar asynchronous engine via the :func:`.add_async_binding`
    function.
    '''
    if isinstance(deferred, Deferred):
        future = Future(loop=loop)

        deferred.addCallbacks(
            future.set_result,
            lambda failure: future.set_exception(failure.value))

        return future


add_async_binding(check_twisted)


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
