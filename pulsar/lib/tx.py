'''
Pulsar concurrent framework and applications can be used with twisted_, an
event driven network engine for python. Twisted has implementation
for several protocols which can be used in pulsar by importing the
:mod:`pulsar.lib.tx` module.

.. _twisted: http://twistedmatrix.com/
'''
import twisted
from twisted.internet.main import installReactor
from twisted.internet.posixbase import PosixReactorBase
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from pulsar.async.defer import default_is_async, default_is_failure, set_async
from pulsar.utils.pep import get_event_loop


def result_or_self(self):
    return self.result if self.called and not self.callbacks else self

def get_traces(self):
    return [(self.type, self.value, self.tb)]
    
def is_async(obj):
    if not default_is_async(obj):
        if isinstance(obj, Deferred):
            if not hasattr(obj, 'add_both'):
                # twisted likes camels
                obj.add_both = obj.addBoth
                obj.add_callback = obj.addCallback
                obj.add_errback = obj.addErrback
                obj.result_or_self = lambda : result_or_self(obj)
            return True
    else:
        return True
    return False

def is_failure(e):
    if not default_is_failure(e):
        if isinstance(e, Failure):
            if not hasattr(e, 'get_traces'):
                e.get_traces = lambda : get_traces(e)
            return True
    else:
        return True
    return False
        

# Set the new async discovery functions
set_async(is_async, is_failure)


class PulsarReactor(PosixReactorBase):
    '''A proxy for the a twisted reactor.'''
        
    def installWaker(self):
        pass
    
    def callLater(self, _seconds, _f, *args, **kw):
        ioloop = get_event_loop()
        return ioloop.call_later(_seconds, lambda : _f(*args, **kw))
    
    def addReader(self, reader):
        get_event_loop().add_reader(reader.fileno(), reader.doRead)
        
    def addWriter(self, writer):
        get_event_loop().add_writer(writer.fileno(), writer.doWrite)
        
    def removeReader(self, reader):
        get_event_loop().remove_reader(reader.fileno())
        
    def removeWriter(self, writer):
        get_event_loop().remove_writer(writer.fileno())
        
    def removeAll(self):
        get_event_loop().remove_all()
        
    def run(self, installSignalHandlers=True):
        self._started = True
        self._stopped = False
        return self
    
    
installReactor(PulsarReactor().run())