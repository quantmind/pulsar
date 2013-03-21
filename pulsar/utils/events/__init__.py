"""Multi-consumer multi-producer dispatching mechanism,
originally based on pydispatch_ (BSD).

To register a new *event* use the :func:`bind` function::

    from pulsar.utils import events
    
    def handler(signal, sender, data=None, **params):
        ...
    
    events.bind('connection_lost', handler)
    
.. _pydispatch:  http://pypi.python.org/pypi/PyDispatcher/2.0.1
"""
import logging
from .dispatcher import Signal


LOGGER = logging.getLogger('pulsar.events')

_events = {}

def create(name, providing_args=None):
    '''Create a new event from a *name*. If the event is already available
it returns it.'''
    name = name.lower()
    if name not in _events:
        event = Signal()
        event.name = name
        _events[name] = event
    return _events[name]

def bind(name, callback, sender=None, **kwargs):
    '''Bind a *callback* to event *name*. The optional *sender* can be used
to bind only when the given sender fires the event.'''
    event = create(name)
    event.connect(callback, sender=sender, **kwargs)
    
def unbind(name, callback, sender=None, **kwargs):
    '''Bind a *callback* to event *name*. The optional *sender* can be used
to bind only when the given sender fires the event.'''
    event = create(name)
    event.disconnect(callback, sender=sender, **kwargs)
    
def fire(name, sender, **kwargs):
    '''Fire event *name* from *sender*.'''
    event = create(name)
    event.send(sender, **kwargs)
    
    
class Listener(object):
    '''A class for testing signals'''
    def __init__(self, *events):
        self._events = {}
        for event in events:
            self._events[event] = []
            bind(event, self)
            
    def __call__(self, signal=None, sender=None, data=None, **kwargs):
        self._events[signal.name].append((sender, data))
        
    def __getitem__(self, event):
        return self._events[event]