'''
Pulsar concurrent framework and applications can be used with twisted_, an
event driven network engine for python. Twisted has implementation
for several protocols which can be used in pulsar by importing the
:mod:`pulsar.apps.tx` module::

    from pulsar.apps.tx import twisted

The implementation replaces the twisted reactor with a proxy for
:class:`pulsar.EventLoop`.
Twisted Deferred and Failures are made compatible with pulsar
by installing two different discovery functions via the :func:`pulsar.set_async`
function.

Threads, signal handling, scheduling and so forth is handled by pulsar itself,
twisted implementation is switched off.

The Coverage report is switched off because twisted is not available
in python 3.

Pulsar Reactor
====================

.. autoclass:: PulsarReactor
   :members:
   :member-order: bysource
   

.. _twisted: http://twistedmatrix.com/
'''
try:    #pragma    nocover
    import twisted
    from twisted.internet.main import installReactor
    from twisted.internet.posixbase import PosixReactorBase
    from twisted.internet.defer import Deferred
    from twisted.python.failure import Failure
except ImportError: #pragma    nocover
    # This is for when we build documentation with sphinx in python 3
    import os
    if os.environ.get('BUILDING-PULSAR-DOCS') == 'yes':
        PosixReactorBase = object
        installReactor = None
    else:
        raise

import pulsar
from pulsar.async.defer import default_maybe_async, default_maybe_failure, set_async
from pulsar.utils.pep import get_event_loop

    
def _maybe_async(obj, **params):    #pragma    nocover
    if isinstance(obj, Deferred):
        d = pulsar.Deferred()
        d._twisted_deferred = obj
        obj.addBoth(d.callback)
        obj = d
    return default_maybe_async(obj, **params)

def _maybe_failure(e):  #pragma    nocover
    if isinstance(e, Failure):
        return pulsar.Failure((e.type, e.value, e.tb))
    else:
        return default_maybe_failure(e)
        

# Set the new async discovery functions
set_async(_maybe_async, _maybe_failure)


class PulsarReactor(PosixReactorBase):  #pragma    nocover
    '''A twisted reactor which acts as a proxy to pulsar eventloop. The
event loop is obtained via the ``get_event_loop`` function.'''
    def callLater(self, _seconds, f, *args, **kw):
        return get_event_loop().call_later(_seconds, lambda : f(*args, **kw))
    
    def callFromThread(self, f, *args, **kw):
        return get_event_loop().call_soon_threadsafe(lambda : f(*args, **kw))
    
    def callInThread(self, f, *args, **kw):
        return get_event_loop().call_soon(lambda : f(*args, **kw))
        
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
    
    ############################################################################
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
        
if installReactor:  #pragma    nocover
    _reactor = PulsarReactor()
    installReactor(_reactor)
    _reactor.run()