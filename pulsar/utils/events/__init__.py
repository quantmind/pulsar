"""Multi-consumer multi-producer dispatching mechanism

Originally based on pydispatch (BSD) http://pypi.python.org/pypi/PyDispatcher/2.0.1
See license.txt for original license.
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
        _events[name] = event
    return _events[name]

def bind(name, callback, sender=None, **kwargs):
    '''Bind a *callback* to event *name*. The optional *sender* can be used
to bind only when the given sender fires the event.'''
    event = create(name)
    event.connect(callback, sender=sender, **kwargs)
    
def fire(name, sender, **kwargs):
    '''Fire event *name* from *sender*.'''
    event = create(name)
    event.send(sender, **kwargs)
    
    
class EventHandler(object):
    EVENTS = ()
    
    def __new__(cls, *args, **kwargs):
        o = super(EventHandler, cls).__new__(cls)
        o.hooks = dict(((event, []) for event in cls.EVENTS))
        return o
        
    def bind_event(self, event, hook):
        '''Register an event hook'''
        self.hooks[event].append(hook)
        
    def fire(self, event, *event_data):
        """Dispatches an event dictionary on a given piece of data."""
        hooks = self.hooks
        if hooks and event in hooks:
            for hook in hooks[event]:
                try:
                    hook(*event_data)
                except Exception:
                    LOGGER.exception('%s unhandled error in %s hook',
                                     self, event)