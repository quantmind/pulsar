'''Pulsar & twisted utilities'''
import twisted
from twisted.internet.main import installReactor
from twisted.internet.posixbase import PosixReactorBase
from twisted.internet.defer import Deferred

from pulsar.utils.pep import get_event_loop


def wrap_deferred(d):
    if isinstance(d, Deferred) and not hasattr(d, 'add_both'):
        # twisted likes camels
        d.add_both = d.addBoth
        d.add_callback = d.addCallback
        d.add_errback = d.addErrback
    return d


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
        get_event_loop().call_soon_threadsafe(self.startRunning)
        
    def startRunning(self, *args):
        self._started = True
        self._stopped = False
    
    
installReactor(PulsarReactor())